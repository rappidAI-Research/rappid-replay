package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/platform"
)

type memoryCredentialStore struct {
	mu     sync.Mutex
	values map[string]string
	sets   int
	getErr error
	setErr error
}

func newMemoryCredentialStore() *memoryCredentialStore {
	return &memoryCredentialStore{values: make(map[string]string)}
}

func credentialMapKey(service, account string) string { return service + "\x00" + account }

func (m *memoryCredentialStore) Get(service, account string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return "", m.getErr
	}
	value, ok := m.values[credentialMapKey(service, account)]
	if !ok {
		return "", platform.ErrCredentialNotFound
	}
	return value, nil
}

func (m *memoryCredentialStore) Set(service, account, secret string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.setErr != nil {
		return m.setErr
	}
	m.values[credentialMapKey(service, account)] = secret
	m.sets++
	return nil
}

func (m *memoryCredentialStore) Delete(service, account string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := credentialMapKey(service, account)
	if _, ok := m.values[key]; !ok {
		return platform.ErrCredentialNotFound
	}
	delete(m.values, key)
	return nil
}

func TestMasterKeyManagerCreatesAndReusesKey(t *testing.T) {
	credentials := newMemoryCredentialStore()
	manager, err := NewMasterKeyManager(credentials, filepath.Join(t.TempDir(), "master-key.lock"))
	if err != nil {
		t.Fatal(err)
	}
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x42}, masterKeyBytes))

	first, err := manager.LoadOrCreate(context.Background())
	if err != nil {
		t.Fatalf("first LoadOrCreate() error = %v", err)
	}
	defer zeroBytes(first)
	second, err := manager.LoadOrCreate(context.Background())
	if err != nil {
		t.Fatalf("second LoadOrCreate() error = %v", err)
	}
	defer zeroBytes(second)

	if !bytes.Equal(first, second) {
		t.Fatal("reloaded master key differs from created key")
	}
	if credentials.sets != 1 {
		t.Fatalf("credential Set calls = %d, want 1", credentials.sets)
	}
	if _, err := os.Stat(manager.markerPath); err != nil {
		t.Fatalf("initialization marker was not created: %v", err)
	}
}

func TestMasterKeyManagerRejectsMalformedStoredKey(t *testing.T) {
	credentials := newMemoryCredentialStore()
	credentials.values[credentialMapKey(masterKeyService, masterKeyAccount)] = "not-base64!"
	manager, err := NewMasterKeyManager(credentials, filepath.Join(t.TempDir(), "master-key.lock"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := manager.LoadOrCreate(context.Background()); err == nil {
		t.Fatal("LoadOrCreate() succeeded with malformed stored key")
	}
	if credentials.sets != 0 {
		t.Fatalf("credential Set calls = %d, want 0", credentials.sets)
	}
}

func TestMasterKeyManagerFailsClosedOnCredentialBackendError(t *testing.T) {
	credentials := newMemoryCredentialStore()
	credentials.getErr = errors.New("credential backend unavailable")
	manager, err := NewMasterKeyManager(credentials, filepath.Join(t.TempDir(), "master-key.lock"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := manager.LoadOrCreate(context.Background()); err == nil {
		t.Fatal("LoadOrCreate() succeeded while credential backend was unavailable")
	}
	if credentials.sets != 0 {
		t.Fatalf("credential Set calls = %d, want 0", credentials.sets)
	}
}

func TestMasterKeyManagerRefusesRegenerationAfterCredentialDeletion(t *testing.T) {
	credentials := newMemoryCredentialStore()
	manager, err := NewMasterKeyManager(credentials, filepath.Join(t.TempDir(), "master-key.lock"))
	if err != nil {
		t.Fatal(err)
	}
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x17}, masterKeyBytes))

	key, err := manager.LoadOrCreate(context.Background())
	if err != nil {
		t.Fatalf("initial LoadOrCreate() error = %v", err)
	}
	zeroBytes(key)
	if err := credentials.Delete(masterKeyService, masterKeyAccount); err != nil {
		t.Fatalf("delete credential: %v", err)
	}

	_, err = manager.LoadOrCreate(context.Background())
	if !errors.Is(err, ErrMasterKeyMissing) {
		t.Fatalf("LoadOrCreate() after credential deletion error = %v, want ErrMasterKeyMissing", err)
	}
	if credentials.sets != 1 {
		t.Fatalf("credential Set calls = %d, want no regeneration after initial set", credentials.sets)
	}
}

func TestMasterKeyManagerSerializesConcurrentInitialization(t *testing.T) {
	credentials := newMemoryCredentialStore()
	lockPath := filepath.Join(t.TempDir(), "master-key.lock")
	firstManager, err := NewMasterKeyManager(credentials, lockPath)
	if err != nil {
		t.Fatal(err)
	}
	secondManager, err := NewMasterKeyManager(credentials, lockPath)
	if err != nil {
		t.Fatal(err)
	}
	firstManager.random = bytes.NewReader(bytes.Repeat([]byte{0x11}, masterKeyBytes))
	secondManager.random = bytes.NewReader(bytes.Repeat([]byte{0x22}, masterKeyBytes))
	firstManager.lockTimeout = 5 * time.Second
	secondManager.lockTimeout = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := make(chan []byte, 2)
	errorsCh := make(chan error, 2)
	var wg sync.WaitGroup
	for _, manager := range []*MasterKeyManager{firstManager, secondManager} {
		wg.Add(1)
		go func(manager *MasterKeyManager) {
			defer wg.Done()
			key, err := manager.LoadOrCreate(ctx)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- key
		}(manager)
	}
	wg.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent LoadOrCreate() error = %v", err)
	}

	var keys [][]byte
	for key := range results {
		keys = append(keys, key)
		defer zeroBytes(key)
	}
	if len(keys) != 2 {
		t.Fatalf("key result count = %d, want 2", len(keys))
	}
	if !bytes.Equal(keys[0], keys[1]) {
		t.Fatal("concurrent initialization returned different master keys")
	}
	if credentials.sets != 1 {
		t.Fatalf("credential Set calls = %d, want 1", credentials.sets)
	}
}

func TestOpenLocalStoreUsesManagedMasterKey(t *testing.T) {
	credentials := newMemoryCredentialStore()
	manager, err := NewMasterKeyManager(credentials, filepath.Join(t.TempDir(), "master-key.lock"))
	if err != nil {
		t.Fatal(err)
	}
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x5a}, masterKeyBytes))
	objects := filepath.Join(t.TempDir(), "objects")

	first, err := OpenLocalStore(context.Background(), objects, manager)
	if err != nil {
		t.Fatalf("OpenLocalStore(first) error = %v", err)
	}
	id, err := first.Put([]byte("managed-key-object"))
	if err != nil {
		_ = first.Close()
		t.Fatalf("Put() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	second, err := OpenLocalStore(context.Background(), objects, manager)
	if err != nil {
		t.Fatalf("OpenLocalStore(second) error = %v", err)
	}
	defer second.Close()
	plaintext, err := second.Get(id)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(plaintext) != "managed-key-object" {
		t.Fatalf("Get() = %q", plaintext)
	}
}
