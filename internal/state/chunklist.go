package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// LargeFileThreshold selects chunk-list storage for files larger than 8 MiB.
const LargeFileThreshold = 8 << 20
const ChunkMinSize = 1 << 20
const ChunkTargetSize = 4 << 20
const ChunkMaxSize = 8 << 20
const chunkObjectIDLength = 67 // "b3:" + 64 lowercase hexadecimal characters.
const chunkEntrySize = 4 + chunkObjectIDLength
const chunkStreamReadBuffer = 256 << 10

var chunkListMagic = []byte{'R', 'P', 'C', 'H', 'N', 'K', 0, 1}
var gearTable = buildGearTable()

// ChunkRef identifies one ordered content chunk and its plaintext byte length.
type ChunkRef struct {
	ObjectID store.ObjectID
	Size     uint32
}

// ChunkList is the canonical file payload used when a file is too large to be
// represented as one blob object.
type ChunkList struct {
	Size   int64
	Chunks []ChunkRef
}

// ContentDefinedChunks splits data using a deterministic Gear rolling hash.
// Boundaries are content-dependent, so insertions usually invalidate only the
// surrounding chunks rather than every subsequent fixed-size block.
func ContentDefinedChunks(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}

	chunks := make([][]byte, 0, len(data)/ChunkTargetSize+1)
	for start := 0; start < len(data); {
		end := nextChunkBoundary(data, start)
		chunks = append(chunks, data[start:end])
		start = end
	}
	return chunks
}

// StreamContentDefinedChunks applies the exact same boundary algorithm as
// ContentDefinedChunks without retaining the complete input in memory. At most
// one ChunkMaxSize chunk plus a small read buffer is live at a time. The emit
// callback receives an independent chunk buffer and may retain it.
func StreamContentDefinedChunks(r io.Reader, emit func([]byte) error) (int64, error) {
	if r == nil {
		return 0, fmt.Errorf("chunk reader is required")
	}
	if emit == nil {
		return 0, fmt.Errorf("chunk emitter is required")
	}

	readBuffer := make([]byte, chunkStreamReadBuffer)
	chunk := make([]byte, 0, ChunkMaxSize)
	var total int64
	var rolling uint64
	emptyReads := 0
	const boundaryMask = uint64(ChunkTargetSize - 1)

	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		out := append([]byte(nil), chunk...)
		if err := emit(out); err != nil {
			return err
		}
		chunk = chunk[:0]
		rolling = 0
		return nil
	}

	for {
		n, readErr := r.Read(readBuffer)
		if n > 0 {
			emptyReads = 0
			for _, value := range readBuffer[:n] {
				chunk = append(chunk, value)
				total++

				// The canonical v1 algorithm begins rolling only after the
				// first ChunkMinSize bytes of each chunk. This mirrors
				// nextChunkBoundary exactly.
				if len(chunk) > ChunkMinSize {
					rolling = (rolling << 1) + gearTable[value]
					if rolling&boundaryMask == 0 || len(chunk) >= ChunkMaxSize {
						if err := flush(); err != nil {
							return total, fmt.Errorf("emit content-defined chunk: %w", err)
						}
					}
				}
			}
		} else if readErr == nil {
			emptyReads++
			if emptyReads >= 100 {
				return total, io.ErrNoProgress
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return total, fmt.Errorf("read content-defined chunk stream: %w", readErr)
		}
	}

	if err := flush(); err != nil {
		return total, fmt.Errorf("emit final content-defined chunk: %w", err)
	}
	return total, nil
}

func nextChunkBoundary(data []byte, start int) int {
	remaining := len(data) - start
	if remaining <= ChunkMaxSize {
		return len(data)
	}

	limit := start + ChunkMaxSize
	position := start + ChunkMinSize
	var rolling uint64
	const mask = uint64(ChunkTargetSize - 1)
	for ; position < limit; position++ {
		rolling = (rolling << 1) + gearTable[data[position]]
		if rolling&mask == 0 {
			return position + 1
		}
	}
	return limit
}

func buildGearTable() [256]uint64 {
	// SplitMix64 with a fixed seed gives Replay a compact, reproducible table
	// without depending on generated source or runtime randomness.
	var table [256]uint64
	state := uint64(0x7261707069644149) // "rappidAI" as a stable seed.
	for i := range table {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		table[i] = z ^ (z >> 31)
	}
	return table
}

// EncodeChunkList returns a stable binary representation. Chunk order is file
// order and therefore part of the file's content identity.
func EncodeChunkList(list ChunkList) ([]byte, error) {
	if list.Size <= 0 {
		return nil, fmt.Errorf("chunk list size must be positive")
	}
	if len(list.Chunks) == 0 {
		return nil, fmt.Errorf("chunk list must contain at least one chunk")
	}
	if len(list.Chunks) > math.MaxUint32 {
		return nil, fmt.Errorf("too many chunks: %d", len(list.Chunks))
	}

	var total int64
	for index, chunk := range list.Chunks {
		if chunk.Size == 0 {
			return nil, fmt.Errorf("chunk %d has zero size", index)
		}
		if _, err := store.ParseObjectID(chunk.ObjectID.String()); err != nil {
			return nil, fmt.Errorf("chunk %d has invalid object id: %w", index, err)
		}
		if len(chunk.ObjectID.String()) != chunkObjectIDLength {
			return nil, fmt.Errorf("chunk %d object id has non-canonical length", index)
		}
		if total > math.MaxInt64-int64(chunk.Size) {
			return nil, fmt.Errorf("chunk sizes overflow int64")
		}
		total += int64(chunk.Size)
	}
	if total != list.Size {
		return nil, fmt.Errorf("chunk sizes total %d bytes, list declares %d", total, list.Size)
	}

	headerSize := len(chunkListMagic) + 8 + 4
	payload := make([]byte, headerSize+len(list.Chunks)*chunkEntrySize)
	copy(payload, chunkListMagic)
	binary.BigEndian.PutUint64(payload[len(chunkListMagic):], uint64(list.Size))
	binary.BigEndian.PutUint32(payload[len(chunkListMagic)+8:], uint32(len(list.Chunks)))

	offset := headerSize
	for _, chunk := range list.Chunks {
		binary.BigEndian.PutUint32(payload[offset:], chunk.Size)
		offset += 4
		copy(payload[offset:offset+chunkObjectIDLength], chunk.ObjectID.String())
		offset += chunkObjectIDLength
	}
	return payload, nil
}

// DecodeChunkList validates and parses the canonical chunk-list representation.
func DecodeChunkList(payload []byte) (ChunkList, error) {
	headerSize := len(chunkListMagic) + 8 + 4
	if len(payload) < headerSize {
		return ChunkList{}, fmt.Errorf("chunk list is truncated")
	}
	if !bytes.Equal(payload[:len(chunkListMagic)], chunkListMagic) {
		return ChunkList{}, fmt.Errorf("chunk list magic/version mismatch")
	}

	totalRaw := binary.BigEndian.Uint64(payload[len(chunkListMagic):])
	if totalRaw == 0 || totalRaw > math.MaxInt64 {
		return ChunkList{}, fmt.Errorf("invalid chunk list total size %d", totalRaw)
	}
	count := binary.BigEndian.Uint32(payload[len(chunkListMagic)+8:])
	if count == 0 {
		return ChunkList{}, fmt.Errorf("chunk list contains no chunks")
	}
	if uint64(count) > uint64((len(payload)-headerSize)/chunkEntrySize)+1 {
		return ChunkList{}, fmt.Errorf("chunk list count exceeds payload bounds")
	}
	expected := headerSize + int(count)*chunkEntrySize
	if expected != len(payload) {
		return ChunkList{}, fmt.Errorf("chunk list length = %d, want %d", len(payload), expected)
	}

	chunks := make([]ChunkRef, 0, count)
	var total int64
	offset := headerSize
	for i := uint32(0); i < count; i++ {
		size := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		if size == 0 {
			return ChunkList{}, fmt.Errorf("chunk %d has zero size", i)
		}
		idText := string(payload[offset : offset+chunkObjectIDLength])
		offset += chunkObjectIDLength
		id, err := store.ParseObjectID(idText)
		if err != nil {
			return ChunkList{}, fmt.Errorf("chunk %d has invalid object id: %w", i, err)
		}
		if total > math.MaxInt64-int64(size) {
			return ChunkList{}, fmt.Errorf("chunk sizes overflow int64")
		}
		chunks = append(chunks, ChunkRef{ObjectID: id, Size: size})
		total += int64(size)
	}
	if total != int64(totalRaw) {
		return ChunkList{}, fmt.Errorf("chunk sizes total %d bytes, list declares %d", total, totalRaw)
	}
	return ChunkList{Size: int64(totalRaw), Chunks: chunks}, nil
}
