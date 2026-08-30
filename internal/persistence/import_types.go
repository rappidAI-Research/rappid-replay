package persistence

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// ImportedState is a portable state together with every authenticated CAS
// object reachable from its root. Object rows are catalogued before reachability
// links are inserted.
type ImportedState struct {
	Record  StateRecord
	Objects []store.ObjectID
}

// ImportedSession is sealed immutable evidence ready for transactional import.
type ImportedSession struct {
	Record      SessionRecord
	Events      []event.Event
	States      []ImportedState
	Environment json.RawMessage
	Artifacts   []ArtifactRecord
}

// ImportEvidence atomically imports session/event/state/environment/artifact
// metadata after callers have authenticated and persisted every required CAS
// object. Existing session IDs are rejected rather than merged or rewritten.
func (db *DB) ImportEvidence(ctx context.Context, objects []store.ObjectMetadata, sessions []ImportedSession) error {
	if db == nil || db.sql == nil {
		return fmt.Errorf("Replay database is required")
	}
	if len(sessions) == 0 {
		return fmt.Errorf("portable import contains no sessions")
	}
	if err := validateImportedEvidence(objects, sessions); err != nil {
		return err
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin portable import: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	for _, metadata := range objects {
		if err := importObjectCatalogRow(ctx, tx, metadata); err != nil {
			return err
		}
	}
	ordered, err := orderImportedSessions(ctx, tx, sessions)
	if err != nil {
		return err
	}
	for _, imported := range ordered {
		if err := validateImportedLineageTx(ctx, tx, imported); err != nil {
			return err
		}
		if err := importSessionTx(ctx, tx, imported); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit portable import: %w", err)
	}
	rollback = false
	return nil
}
