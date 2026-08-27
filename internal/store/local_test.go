package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

func TestLocalStorePutGetAndDeduplicate(t *testing.T) {
	key := bytes.Repeat([]byte{0x24}, chacha20poly1305.KeySize)
	root := t.TempDir()
	store, err := NewLocalStore(root, key)
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

	if runtime.GOOS != "windows" {
		for name, want := range map[string]os.FileMode{
			root:                 0o700,
			filepath.Join(root, "b3"): 0o700,
			path:                 0o600,
		} {
			info, err := os.Stat(name)
			if err != nil {
				t.Fatalf("stat %s: %v", name, err)
			}
			if info.Mode().Perm() != want {
				t.Fatalf("permissions for %s = %o, want %o", name, info.Mode().Perm(), want)
			}
		}
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

	if err := store.Verify(id); !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("Verify() error = %v, want ErrCorruptObject", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt object path stat error = %v, want object moved out of active CAS", err)
	}
	blocked, err := store.isQuarantined(id)
	if err != nil {
		t.Fatalf("isQuarantined() error = %v", err)
	}
	if !blocked {
		t.Fatal("corrupt object was not persistently blocked after quarantine")
	}
	if _, err := store.Put(plaintext); !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("Put() after quarantine error = %v, want ErrCorruptObject", err)
	}
}

func TestLocalStoreQuarantinesOversizedStoredObjectBeforeReadAll(t *testing.T) {
	store, err := NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x32}, chacha20poly1305.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	id, err := store.Put([]byte("bounded"))
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.objectPath(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, maxStoredObjectBytes+1); err != nil {
		t.Fatalf("expand object file: %v", err)
	}
	if err := store.Verify(id); !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("Verify(oversized) error = %v, want ErrCorruptObject", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversized object remained active: %v", err)
	}
}

func TestLocalStoreRejectsSymlinkObjectWithoutFollowing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated privileges on Windows")
	}
	store, err := NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x33}, chacha20poly1305.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	id, err := store.Put([]byte("object"))
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.objectPath(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	outsideContents := []byte("must not be treated as CAS data")
	if err := os.WriteFile(outside, outsideContents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}

	if err := store.Verify(id); !errors.Is(err, ErrCorruptObject) {
		t.Fatalf("Verify(symlink) error = %v, want ErrCorruptObject", err)
	}
	got, err := os.ReadFile(outside)
	if err != nil {
		t.Fatalf("read outside file: %v", err)
	}
	if !bytes.Equal(got, outsideContents) {
		t.Fatalf("outside file was modified: %q", got)
	}
}

func TestLocalStoreRejectsWrongKeyBeforeObjectAccess(t *testing.T) {
	root := t.TempDir()
	firstKey := bytes.Repeat([]byte{0x41}, chacha20poly1305.KeySize)
	first, err := NewLocalStore(root, firstKey)
	if err != nil {
		t.Fatalf("NewLocalStore(first) error = %v", err)
	}
	if _, err := first.Put([]byte("protected object")); err != nil {
		_ = first.Close()
		t.Fatalf("Put() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	wrongKey := bytes.Repeat([]byte{0x42}, chacha20poly1305.KeySize)
	second, err := NewLocalStore(root, wrongKey)
	if err == nil {
		_ = second.Close()
		t.Fatal("NewLocalStore() accepted a master key that does not match the store")
	}
	if !errors.Is(err, ErrStoreKeyCheck) {
		t.Fatalf("NewLocalStore(wrong key) error = %v, want ErrStoreKeyCheck", err)
	}
}
