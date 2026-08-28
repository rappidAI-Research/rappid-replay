package persistence

import "fmt"

// validateLegacyMigrationBaseline protects the one-time checksum backfill for
// migrations that shipped before schema_migrations gained a checksum column.
// It remains a separate invariant from ordinary embedded-file checksum checks:
// an already-applied legacy migration may only be blessed if its currently
// embedded bytes match the release-pinned baseline.
func validateLegacyMigrationBaseline(item migration) error {
	want, ok := legacyMigrationChecksums[item.version]
	if !ok {
		return fmt.Errorf("migration %s is recorded without a checksum and has no pinned legacy baseline", item.name)
	}
	if item.checksum != want {
		return fmt.Errorf("legacy migration %s checksum mismatch: pinned=%s embedded=%s", item.name, want, item.checksum)
	}
	return nil
}
