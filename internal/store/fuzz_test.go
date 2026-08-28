package store

import (
	"bytes"
	"testing"
)

func FuzzDecodeObject(f *testing.F) {
	seed, err := EncodeObject(ObjectBlob, []byte("seed"))
	if err != nil {
		f.Fatalf("create object seed: %v", err)
	}
	f.Add(seed)
	f.Add([]byte("RPOBJ"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, input []byte) {
		obj, err := DecodeObject(input)
		if err != nil {
			return
		}
		canonical, err := EncodeObject(obj.Kind, obj.Payload)
		if err != nil {
			t.Fatalf("accepted object failed canonical re-encode: %v", err)
		}
		if !bytes.Equal(canonical, input) {
			t.Fatalf("accepted object was not byte-canonical")
		}
	})
}
