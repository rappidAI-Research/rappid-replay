package platform

import (
	"errors"
	"fmt"

	keyring "github.com/zalando/go-keyring"
)

// SystemCredentialStore uses the user's native credential store: Keychain on
// macOS, Credential Manager on Windows, and Secret Service over D-Bus on Linux.
type SystemCredentialStore struct{}

func (SystemCredentialStore) Get(service, account string) (string, error) {
	secret, err := keyring.Get(service, account)
	if err == nil {
		return secret, nil
	}
	if errors.Is(err, keyring.ErrNotFound) {
		return "", ErrCredentialNotFound
	}
	return "", fmt.Errorf("get system credential %q/%q: %w", service, account, err)
}

func (SystemCredentialStore) Set(service, account, secret string) error {
	if err := keyring.Set(service, account, secret); err != nil {
		return fmt.Errorf("set system credential %q/%q: %w", service, account, err)
	}
	return nil
}

func (SystemCredentialStore) Delete(service, account string) error {
	if err := keyring.Delete(service, account); err != nil {
		if errors.Is(err, keyring.ErrNotFound) {
			return ErrCredentialNotFound
		}
		return fmt.Errorf("delete system credential %q/%q: %w", service, account, err)
	}
	return nil
}
