package store

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	"github.com/rappidAI-Research/rappid-replay/internal/platform"
)

const (
	masterKeyBytes       = 32
	masterKeyService     = "rappidAI Replay"
	masterKeyAccount     = "local-cas-master-key-v1"
	masterKeyLockName    = "master-key.lock"
	masterKeyMarkerName  = "master-key.initialized-v1"
	masterKeyLockTimeout = 15 * time.Second
	masterKeyLockRetry   = 50 * time.Millisecond
)

var ErrMasterKeyMissing = errors.New("Replay master key is missing after prior initialization")

// MasterKeyManager loads or initializes Replay's per-user local CAS master key.
// The key is stored only in the operating-system credential store. The lock and
// initialization marker contain no secret material.
type MasterKeyManager struct {
	credentials platform.CredentialStore
	lockPath    string
	markerPath  string
	random      io.Reader
	lockTimeout time.Duration
}

// NewMasterKeyManager constructs a key manager with an explicit credential
// backend and lock path. This is primarily useful for dependency injection and
// tests; production callers should use NewSystemMasterKeyManager.
func NewMasterKeyManager(credentials platform.CredentialStore, lockPath string) (*MasterKeyManager, error) {
	if credentials == nil {
		return nil, fmt.Errorf("credential store is required")
	}
	if lockPath == "" {
		return nil, fmt.Errorf("master-key lock path is required")
	}
	absLockPath, err := filepath.Abs(lockPath)
	if err != nil {
		return nil, fmt.Errorf("resolve master-key lock path: %w", err)
	}
	return &MasterKeyManager{
		credentials: credentials,
		lockPath:    absLockPath,
		markerPath:  filepath.Join(filepath.Dir(absLockPath), masterKeyMarkerName),
		random:      cryptorand.Reader,
		lockTimeout: masterKeyLockTimeout,
	}, nil
}

// NewSystemMasterKeyManager binds Replay to the native OS credential store and
// uses the architecture-defined user configuration directory for its lock file.
func NewSystemMasterKeyManager() (*MasterKeyManager, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve user home directory for master key: %w", err)
	}
	if home == "" {
		return nil, fmt.Errorf("user home directory for master key is empty")
	}
	lockPath := filepath.Join(home, ".config", "rappidAI", "replay", masterKeyLockName)
	return NewMasterKeyManager(platform.SystemCredentialStore{}, lockPath)
}

// LoadOrCreate returns the existing 256-bit master key or creates it exactly
// once. Credential-backend failures and malformed stored values fail closed.
// A non-secret initialization marker prevents a deleted credential from being
// silently replaced with a new key that would orphan existing encrypted data.
func (m *MasterKeyManager) LoadOrCreate(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if err := os.MkdirAll(filepath.Dir(m.lockPath), 0o700); err != nil {
		return nil, fmt.Errorf("create master-key lock directory: %w", err)
	}

	fileLock := flock.New(m.lockPath, flock.SetPermissions(0o600))
	lockCtx, cancel := context.WithTimeout(ctx, m.lockTimeout)
	defer cancel()
	locked, err := fileLock.TryLockContext(lockCtx, masterKeyLockRetry)
	if err != nil {
		return nil, fmt.Errorf("acquire master-key initialization lock: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("master-key initialization lock was not acquired")
	}

	key, operationErr := m.loadOrCreateLocked()
	unlockErr := fileLock.Unlock()
	if operationErr != nil {
		return nil, operationErr
	}
	if unlockErr != nil {
		zeroBytes(key)
		return nil, fmt.Errorf("release master-key initialization lock: %w", unlockErr)
	}
	return key, nil
}

func (m *MasterKeyManager) loadOrCreateLocked() ([]byte, error) {
	encoded, err := m.credentials.Get(masterKeyService, masterKeyAccount)
	if err == nil {
		key, decodeErr := decodeMasterKey(encoded)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if err := ensureInitializationMarker(m.markerPath); err != nil {
			zeroBytes(key)
			return nil, fmt.Errorf("persist master-key initialization marker: %w", err)
		}
		return key, nil
	}
	if !errors.Is(err, platform.ErrCredentialNotFound) {
		return nil, fmt.Errorf("load Replay master key: %w", err)
	}

	initialized, err := initializationMarkerExists(m.markerPath)
	if err != nil {
		return nil, err
	}
	if initialized {
		return nil, fmt.Errorf("%w; refusing to generate a replacement key", ErrMasterKeyMissing)
	}

	candidate := make([]byte, masterKeyBytes)
	if _, err := io.ReadFull(m.random, candidate); err != nil {
		return nil, fmt.Errorf("generate Replay master key: %w", err)
	}
	encodedCandidate := base64.RawStdEncoding.EncodeToString(candidate)
	if err := m.credentials.Set(masterKeyService, masterKeyAccount, encodedCandidate); err != nil {
		zeroBytes(candidate)
		return nil, fmt.Errorf("persist Replay master key in system credential store: %w", err)
	}

	storedValue, err := m.credentials.Get(masterKeyService, masterKeyAccount)
	if err != nil {
		zeroBytes(candidate)
		return nil, fmt.Errorf("verify persisted Replay master key: %w", err)
	}
	storedKey, err := decodeMasterKey(storedValue)
	if err != nil {
		zeroBytes(candidate)
		return nil, fmt.Errorf("verify persisted Replay master key: %w", err)
	}
	if subtle.ConstantTimeCompare(candidate, storedKey) != 1 {
		zeroBytes(candidate)
		zeroBytes(storedKey)
		return nil, fmt.Errorf("persisted Replay master key does not match generated key")
	}
	zeroBytes(storedKey)

	if err := ensureInitializationMarker(m.markerPath); err != nil {
		zeroBytes(candidate)
		return nil, fmt.Errorf("persist master-key initialization marker: %w", err)
	}
	return candidate, nil
}

func initializationMarkerExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		if !info.Mode().IsRegular() {
			return false, fmt.Errorf("master-key initialization marker is not a regular file")
		}
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("stat master-key initialization marker: %w", err)
}

func ensureInitializationMarker(path string) error {
	exists, err := initializationMarkerExists(path)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			_, verifyErr := initializationMarkerExists(path)
			return verifyErr
		}
		return err
	}
	if _, err := io.WriteString(file, "initialized-v1\n"); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func decodeMasterKey(encoded string) ([]byte, error) {
	decoded, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode Replay master key: %w", err)
	}
	if len(decoded) != masterKeyBytes {
		zeroBytes(decoded)
		return nil, fmt.Errorf("Replay master key has %d bytes, want %d", len(decoded), masterKeyBytes)
	}
	return decoded, nil
}

func zeroBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

// OpenLocalStore loads the managed master key, constructs the encrypted object
// store, and clears the temporary key buffer after the codec has consumed it.
func OpenLocalStore(ctx context.Context, root string, keys *MasterKeyManager) (*LocalStore, error) {
	if keys == nil {
		return nil, fmt.Errorf("master-key manager is required")
	}
	key, err := keys.LoadOrCreate(ctx)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(key)
	return NewLocalStore(root, key)
}

// OpenSystemLocalStore is the production convenience path for a local CAS.
func OpenSystemLocalStore(ctx context.Context, root string) (*LocalStore, error) {
	keys, err := NewSystemMasterKeyManager()
	if err != nil {
		return nil, err
	}
	return OpenLocalStore(ctx, root, keys)
}
