package state

import (
	"bytes"
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
