package persistence

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func TestOpenAppliesMigrationsAndSafetyPragmas(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("DB.Close() error = %v", err)
		}
	})

	var journalMode string
	if err := db.sql.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		t.Fatalf("journal_mode = %q, want WAL", journalMode)
	}

	var foreignKeys int
	if err := db.sql.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatalf("read foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("foreign_keys = %d, want 1", foreignKeys)
	}

	for _, table := range []string{
		"schema_migrations", "sessions", "objects", "states", "state_objects", "events", "environments",
	} {
		var count int
		if err := db.sql.QueryRowContext(ctx,
			"SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name=?", table,
		).Scan(&count); err != nil {
			t.Fatalf("find table %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("table %s count = %d, want 1", table, count)
		}
	}

	wantMigrations := embeddedMigrationCount(t)
	var migrations int
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM schema_migrations").Scan(&migrations); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if migrations != wantMigrations {
		t.Fatalf("migration count = %d, want %d", migrations, wantMigrations)
	}

	var missingChecksums int
	if err := db.sql.QueryRowContext(ctx,
		"SELECT COUNT(1) FROM schema_migrations WHERE checksum IS NULL OR checksum = ''",
	).Scan(&missingChecksums); err != nil {
		t.Fatalf("count missing migration checksums: %v", err)
	}
	if missingChecksums != 0 {
		t.Fatalf("migrations without checksums = %d, want 0", missingChecksums)
	}
}

func TestOpenHardensDatabaseFilesystemPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not authoritative on Windows")
	}
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "replay-data")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "replay.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("database directory mode = %o, want 700", got)
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("database file mode = %o, want 600", got)
	}
}

func TestOpenIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replay.db")

	first, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer second.Close()

	wantMigrations := embeddedMigrationCount(t)
	var migrations int
	if err := second.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM schema_migrations").Scan(&migrations); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if migrations != wantMigrations {
		t.Fatalf("migration count after reopen = %d, want %d", migrations, wantMigrations)
	}
}

func TestConcurrentOpenSerializesMigrations(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replay.db")

	const workers = 8
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := Open(ctx, path)
			if err != nil {
				errs <- err
				return
			}
			if err := db.Close(); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Open() error = %v", err)
	}

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() after concurrent initialization error = %v", err)
	}
	defer db.Close()

	wantMigrations := embeddedMigrationCount(t)
	var migrations int
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM schema_migrations").Scan(&migrations); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if migrations != wantMigrations {
		t.Fatalf("migration count after concurrent opens = %d, want %d", migrations, wantMigrations)
	}
}

func TestOpenRejectsMigrationChecksumDrift(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replay.db")

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if _, err := db.sql.ExecContext(ctx,
		"UPDATE schema_migrations SET checksum = 'sha256:tampered' WHERE version = 1",
	); err != nil {
		_ = db.Close()
		t.Fatalf("tamper migration checksum: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := Open(ctx, path); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("Open() error = %v, want checksum mismatch", err)
	}
}

func embeddedMigrationCount(t *testing.T) int {
	t.Helper()
	items, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations() error = %v", err)
	}
	return len(items)
}
