package store

import (
	"bytes"
	"os"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

func TestLocalStorePutGetAndDeduplicate(t *testing.T) {
	key := bytes.Repeat([]byte{0x24}, chacha20poly1305.KeySize)
	store, err := NewLocalStore(t.TempDir(), key)
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("LocalStore.Close() error = %v", err)
		}
	})

	plaintext := []byte("immutable replay content")
	id, err := store.Put(plaintext)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	path, err := store.objectPath(id)
	if err != nil {
		t.Fatalf("objectPath() error = %v", err)
	}
	firstPayload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read stored payload: %v", err)
	}

	idAgain, err := store.Put(plaintext)
	if err != nil {
		t.Fatalf("second Put() error = %v", err)
	}
	if idAgain != id {
		t.Fatalf("second Put() id = %s, want %s", idAgain, id)
	}
	secondPayload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read deduplicated payload: %v", err)
	}
	if !bytes.Equal(firstPayload, secondPayload) {
		t.Fatal("deduplicated Put() rewrote existing encrypted object")
	}

	got, err := store.Get(id)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("Get() = %q, want %q", got, plaintext)
	}
}

func TestLocalStoreDoesNotSilentlyReplaceCorruption(t *testing.T) {
	key := bytes.Repeat([]byte{0x31}, chacha20poly1305.KeySize)
	store, err := NewLocalStore(t.TempDir(), key)
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	defer store.Close()

	plaintext := []byte("object that will be corrupted")
	id, err := store.Put(plaintext)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	path, err := store.objectPath(id)
	if err != nil {
		t.Fatalf("objectPath() error = %v", err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	payload[len(payload)-1] ^= 0x80
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("corrupt payload: %v", err)
	}

	if err := store.Verify(id); err == nil {
		t.Fatal("Verify() accepted corrupted object")
	}
	if _, err := store.Put(plaintext); err == nil {
		t.Fatal("Put() silently replaced corrupted existing object")
	}
}
