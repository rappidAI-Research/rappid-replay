package store

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

func testCodec(t *testing.T) *Codec {
	t.Helper()
	key := bytes.Repeat([]byte{0x42}, chacha20poly1305.KeySize)
	codec, err := NewCodec(key)
	if err != nil {
		t.Fatalf("NewCodec() error = %v", err)
	}
	t.Cleanup(func() {
		if err := codec.Close(); err != nil {
			t.Errorf("Codec.Close() error = %v", err)
		}
	})
	return codec
}

func TestCodecRoundTripAndStableIdentity(t *testing.T) {
	codec := testCodec(t)
	plaintext := []byte("canonical replay object\nwith repeated repeated repeated content")

	idA, payloadA, err := codec.Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal(A) error = %v", err)
	}
	idB, payloadB, err := codec.Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal(B) error = %v", err)
	}
	if idA != idB {
		t.Fatalf("same plaintext produced different IDs: %s != %s", idA, idB)
	}
	if bytes.Equal(payloadA, payloadB) {
		t.Fatal("encrypted payloads unexpectedly identical; fresh nonces are required")
	}

	got, err := codec.Open(idA, payloadA)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("Open() = %q, want %q", got, plaintext)
	}
}

func TestCodecSupportsEmptyObjects(t *testing.T) {
	codec := testCodec(t)
	id, payload, err := codec.Seal(nil)
	if err != nil {
		t.Fatalf("Seal(nil) error = %v", err)
	}
	got, err := codec.Open(id, payload)
	if err != nil {
		t.Fatalf("Open(empty) error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Open(empty) returned %d bytes", len(got))
	}
}

func TestCodecRejectsTamperingAndWrongIdentity(t *testing.T) {
	codec := testCodec(t)
	id, payload, err := codec.Seal([]byte("sensitive object"))
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}

	tampered := append([]byte(nil), payload...)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := codec.Open(id, tampered); err == nil {
		t.Fatal("Open() accepted tampered ciphertext")
	}

	wrongID := SumObject([]byte("different object"))
	if _, err := codec.Open(wrongID, payload); err == nil {
		t.Fatal("Open() accepted payload under the wrong object ID")
	}
}

func TestCodecConcurrentSealAndOpen(t *testing.T) {
	codec := testCodec(t)

	const workers = 16
	const iterations = 20
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				plaintext := []byte(fmt.Sprintf("worker=%d iteration=%d payload payload payload", worker, iteration))
				id, payload, err := codec.Seal(plaintext)
				if err != nil {
					errs <- fmt.Errorf("Seal: %w", err)
					return
				}
				got, err := codec.Open(id, payload)
				if err != nil {
					errs <- fmt.Errorf("Open: %w", err)
					return
				}
				if !bytes.Equal(got, plaintext) {
					errs <- fmt.Errorf("roundtrip mismatch")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestNewCodecRejectsInvalidKeyLength(t *testing.T) {
	if _, err := NewCodec([]byte("short")); err == nil {
		t.Fatal("NewCodec() accepted invalid key length")
	}
}

func TestParseObjectID(t *testing.T) {
	id := SumObject([]byte("object"))
	parsed, err := ParseObjectID(id.String())
	if err != nil {
		t.Fatalf("ParseObjectID() error = %v", err)
	}
	if parsed != id {
		t.Fatalf("ParseObjectID() = %s, want %s", parsed, id)
	}

	if _, err := ParseObjectID("sha256:deadbeef"); err == nil {
		t.Fatal("ParseObjectID() accepted wrong hash namespace")
	}
}
