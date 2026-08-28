package state

import (
	"bytes"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func FuzzParseCanonicalTree(f *testing.F) {
	fileID := store.SumObject([]byte("seed"))
	seed, err := CanonicalBytes(NewTree([]Entry{{Name: []byte("seed.txt"), Kind: EntryFile, Size: 4, ObjectID: fileID}}))
	if err != nil {
		f.Fatalf("create tree seed: %v", err)
	}
	f.Add(seed)
	f.Add([]byte(`{"schema":"rappid.replay.tree-object/1","entries":[]}`))
	f.Add([]byte(`{}`))

	f.Fuzz(func(t *testing.T, input []byte) {
		tree, err := ParseCanonicalTree(input)
		if err != nil {
			return
		}
		canonical, err := CanonicalBytes(tree)
		if err != nil {
			t.Fatalf("accepted tree failed canonical re-encode: %v", err)
		}
		if !bytes.Equal(canonical, input) {
			t.Fatalf("accepted tree was not byte-canonical")
		}
	})
}

func FuzzDecodeChunkList(f *testing.F) {
	id := store.SumObject([]byte("chunk"))
	seed, err := EncodeChunkList(ChunkList{Size: 5, Chunks: []ChunkRef{{ObjectID: id, Size: 5}}})
	if err != nil {
		f.Fatalf("create chunk-list seed: %v", err)
	}
	f.Add(seed)
	f.Add([]byte("RPCHNK"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, input []byte) {
		list, err := DecodeChunkList(input)
		if err != nil {
			return
		}
		canonical, err := EncodeChunkList(list)
		if err != nil {
			t.Fatalf("accepted chunk list failed canonical re-encode: %v", err)
		}
		if !bytes.Equal(canonical, input) {
			t.Fatalf("accepted chunk list was not byte-canonical")
		}
	})
}
