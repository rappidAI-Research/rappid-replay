package persistence

import (
	"strings"
	"testing"
)

func TestLegacyMigrationBaselinesMatchOriginallyShippedSQL(t *testing.T) {
	items, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) < len(legacyMigrationChecksums) {
		t.Fatalf("loaded %d migrations, need at least %d legacy migrations", len(items), len(legacyMigrationChecksums))
	}
	for version := 1; version <= len(legacyMigrationChecksums); version++ {
		item := items[version-1]
		if item.version != version {
			t.Fatalf("migration at index %d has version %d", version-1, item.version)
		}
		if err := validateLegacyMigrationBaseline(item); err != nil {
			t.Fatalf("legacy migration %d baseline mismatch: %v", version, err)
		}
	}
}

func TestLegacyMigrationBaselineRejectsEmbeddedDrift(t *testing.T) {
	items, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	item := items[0]
	item.checksum = "sha256:tampered"
	if err := validateLegacyMigrationBaseline(item); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("validateLegacyMigrationBaseline() error = %v, want checksum mismatch", err)
	}
}
