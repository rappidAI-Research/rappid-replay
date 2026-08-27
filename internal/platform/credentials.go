package platform

import "errors"

// ErrCredentialNotFound indicates that a requested credential does not exist in
// the operating-system credential store. Callers must distinguish this from a
// backend failure so they never replace an unreadable existing key by mistake.
var ErrCredentialNotFound = errors.New("credential not found")

// CredentialStore is Replay's narrow boundary to the operating-system secret
// store. Secret values must never be persisted by callers outside this API.
type CredentialStore interface {
	Get(service, account string) (string, error)
	Set(service, account, secret string) error
	Delete(service, account string) error
}
