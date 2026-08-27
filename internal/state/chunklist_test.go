package state

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestChunkListCanonicalRoundTrip(t *testing.T) {
	first := store.SumObject([]byte("first"))
	second := store.SumObject([]byte("second"))
	encoded, err := EncodeChunkList(ChunkList{
		Size: 7,
		Chunks: []ChunkRef{
			{ObjectID: first, Size: 3},
			{ObjectID: second, Size: 4},
		},
	})
	if err != nil {
		t.Fatalf("EncodeChunkList() error = %v", err)
	}
	decoded, err := DecodeChunkList(encoded)
	if err != nil {
		t.Fatalf("DecodeChunkList() error = %v", err)
	}
	if decoded.Size != 7 || len(decoded.Chunks) != 2 {
		t.Fatalf("decoded chunk list = %+v", decoded)
	}
	if decoded.Chunks[0].ObjectID != first || decoded.Chunks[0].Size != 3 || decoded.Chunks[1].ObjectID != second || decoded.Chunks[1].Size != 4 {
		t.Fatalf("decoded chunk refs = %+v", decoded.Chunks)
	}

	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] = 'z'
	if _, err := DecodeChunkList(corrupt); err == nil {
		t.Fatal("DecodeChunkList() accepted corrupt object id")
	}
}

func TestContentDefinedChunksAreDeterministicAndBounded(t *testing.T) {
	data := make([]byte, 20<<20)
	for i := range data {
		data[i] = byte((i*31 + i/4096) % 251)
	}

	first := ContentDefinedChunks(data)
	second := ContentDefinedChunks(data)
	if len(first) < 3 {
		t.Fatalf("chunk count = %d, want at least 3", len(first))
	}
	if len(first) != len(second) {
		t.Fatalf("deterministic chunk counts differ: %d != %d", len(first), len(second))
	}

	var rebuilt []byte
	for i, chunk := range first {
		if len(chunk) > ChunkMaxSize {
			t.Fatalf("chunk %d size = %d, max %d", i, len(chunk), ChunkMaxSize)
		}
		if i < len(first)-1 && len(chunk) < ChunkMinSize {
			t.Fatalf("non-final chunk %d size = %d, min %d", i, len(chunk), ChunkMinSize)
		}
		if !bytes.Equal(chunk, second[i]) {
			t.Fatalf("chunk %d differs across identical runs", i)
		}
		rebuilt = append(rebuilt, chunk...)
	}
	if !bytes.Equal(rebuilt, data) {
		t.Fatal("chunk concatenation did not reconstruct original data")
	}
}

func TestStreamContentDefinedChunksMatchesInMemoryAlgorithm(t *testing.T) {
	lengths := []int{
		ChunkMaxSize + 1,
		9<<20 + 17,
		12<<20 + 12345,
		16<<20 + 7,
		23 << 20,
	}
	for caseIndex, length := range lengths {
		data := make([]byte, length)
		for i := range data {
			// Vary the deterministic content between cases while retaining enough
			// local structure to exercise content-dependent boundaries.
			data[i] = byte((i*(31+caseIndex*7) + i/(1024+caseIndex*257) + caseIndex*13) % 251)
		}
		want := ContentDefinedChunks(data)

		for _, readSize := range []int{chunkStreamReadBuffer, 8191, 3073} {
			t.Run(chunkParityName(length, readSize), func(t *testing.T) {
				reader := &chunkedReader{data: data, maxRead: readSize, eofWithData: readSize == 3073}
				var got [][]byte
				total, err := StreamContentDefinedChunks(reader, func(chunk []byte) error {
					got = append(got, append([]byte(nil), chunk...))
					return nil
				})
				if err != nil {
					t.Fatalf("StreamContentDefinedChunks() error = %v", err)
				}
				if total != int64(len(data)) {
					t.Fatalf("stream total = %d, want %d", total, len(data))
				}
				assertChunkSetsEqual(t, got, want)
			})
		}
	}
}

func TestStreamContentDefinedChunksPreservesFinalRemainder(t *testing.T) {
	// v1 intentionally does not search for another boundary when the complete
	// remaining suffix is <= ChunkMaxSize. This was a subtle place where a naïve
	// streaming implementation can diverge from the canonical in-memory format.
	data := make([]byte, ChunkMaxSize+1)
	for i := range data {
		data[i] = byte((i*73 + i/997) % 251)
	}
	want := ContentDefinedChunks(data)
	if len(want) == 0 {
		t.Fatal("canonical chunker returned no chunks")
	}

	var got [][]byte
	_, err := StreamContentDefinedChunks(&chunkedReader{data: data, maxRead: 4093, eofWithData: true}, func(chunk []byte) error {
		got = append(got, append([]byte(nil), chunk...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChunkSetsEqual(t, got, want)
}

func chunkParityName(length, readSize int) string {
	return "bytes_" + itoa(length) + "_read_" + itoa(readSize)
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var buf [32]byte
	pos := len(buf)
	for value > 0 {
		pos--
		buf[pos] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[pos:])
}

func assertChunkSetsEqual(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("stream chunk count = %d, want %d; got sizes=%v want sizes=%v", len(got), len(want), chunkSizes(got), chunkSizes(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("stream chunk %d differs: got %d bytes, want %d", i, len(got[i]), len(want[i]))
		}
	}
}

func chunkSizes(chunks [][]byte) []int {
	sizes := make([]int, len(chunks))
	for i, chunk := range chunks {
		sizes[i] = len(chunk)
	}
	return sizes
}

type chunkedReader struct {
	data        []byte
	offset      int
	maxRead     int
	eofWithData bool
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	limit := r.maxRead
	if limit <= 0 || limit > len(p) {
		limit = len(p)
	}
	remaining := len(r.data) - r.offset
	if limit > remaining {
		limit = remaining
	}
	copy(p[:limit], r.data[r.offset:r.offset+limit])
	r.offset += limit
	if r.eofWithData && r.offset == len(r.data) {
		return limit, io.EOF
	}
	return limit, nil
}

func TestSnapshotLargeFilePublishesReachableChunks(t *testing.T) {
	workspace := t.TempDir()
	data := make([]byte, LargeFileThreshold+(3<<20))
	for i := range data {
		data[i] = byte((i*17 + i/1024) % 253)
	}
	if err := os.WriteFile(filepath.Join(workspace, "large.bin"), data, 0o600); err != nil {
		t.Fatalf("write large file: %v", err)
	}

	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x66}, 32))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	snapshot, err := (Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if snapshot.Files != 1 || snapshot.FileBytes != int64(len(data)) {
		t.Fatalf("snapshot stats = %+v", snapshot)
	}

	rootObj, err := cas.GetObject(snapshot.RootTreeID)
	if err != nil {
		t.Fatalf("load root tree: %v", err)
	}
	rootTree, err := ParseCanonicalTree(rootObj.Payload)
	if err != nil {
		t.Fatalf("parse root tree: %v", err)
	}
	if len(rootTree.Entries) != 1 {
		t.Fatalf("root entries = %d, want 1", len(rootTree.Entries))
	}
	fileObj, err := cas.GetObject(rootTree.Entries[0].ObjectID)
	if err != nil {
		t.Fatalf("load file object: %v", err)
	}
	if fileObj.Kind != store.ObjectChunkList {
		t.Fatalf("large file object kind = %q, want %q", fileObj.Kind, store.ObjectChunkList)
	}
	list, err := DecodeChunkList(fileObj.Payload)
	if err != nil {
		t.Fatalf("DecodeChunkList() error = %v", err)
	}
	if list.Size != int64(len(data)) || len(list.Chunks) < 2 {
		t.Fatalf("chunk list = size %d chunks %d", list.Size, len(list.Chunks))
	}

	verification, err := VerifySnapshot(cas, snapshot.RootTreeID)
	if err != nil {
		t.Fatalf("VerifySnapshot() error = %v", err)
	}
	if verification.Files != 1 || verification.FileBytes != int64(len(data)) {
		t.Fatalf("verification = %+v", verification)
	}

	inspection, err := InspectSnapshot(cas, snapshot.RootTreeID)
	if err != nil {
		t.Fatalf("InspectSnapshot() error = %v", err)
	}
	if len(inspection.Objects) < len(list.Chunks)+2 {
		t.Fatalf("reachable objects = %d, want at least %d", len(inspection.Objects), len(list.Chunks)+2)
	}
}
