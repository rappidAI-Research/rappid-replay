package persistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

const (
	migrationLockTimeout = 30 * time.Second
	migrationLockRetry   = 50 * time.Millisecond
)

// Versions 1 and 2 shipped before schema_migrations had a checksum column.
// Their accepted SHA-256 values are pinned in code so an old database cannot
// silently bless modified embedded SQL during the one-time checksum backfill.
var legacyMigrationChecksums = map[int]string{
	1: "sha256:03a2da4877c3c87acedddefc9a64cf1e2125689870a6fb785b180014fdbae1f5",
	2: "sha256:42507fe04a7f84d49cb1dc1a7025a99a8891438e03a0309fb0e686ef7ab2298a",
}

type migration struct {
	version  int
	name     string
	sql      []byte
	checksum string
}

type queryContext interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (db *DB) migrateWithLock(ctx context.Context, databasePath string) error {
	lockPath := databasePath + ".migrate.lock"
	lock := flock.New(lockPath, flock.SetPermissions(0o600))
	lockCtx, cancel := context.WithTimeout(ctx, migrationLockTimeout)
	defer cancel()
	locked, err := lock.TryLockContext(lockCtx, migrationLockRetry)
	if err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if !locked {
		return fmt.Errorf("migration lock was not acquired")
	}

	migrationErr := db.migrate(ctx)
	unlockErr := lock.Unlock()
	if migrationErr != nil {
		return migrationErr
	}
	if unlockErr != nil {
		return fmt.Errorf("release migration lock: %w", unlockErr)
	}
	return nil
}

func (db *DB) migrate(ctx context.Context) error {
	if _, err := db.sql.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);`); err != nil {
		return fmt.Errorf("initialize schema migrations: %w", err)
	}

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	checksumSupported, err := migrationChecksumColumnExists(ctx, db.sql)
	if err != nil {
		return err
	}

	for _, item := range migrations {
		applied, appliedName, appliedChecksum, err := readMigrationRecord(ctx, db.sql, item.version, checksumSupported)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", item.name, err)
		}
		if applied {
			if appliedName != item.name {
				return fmt.Errorf("migration version %d recorded as %q, embedded file is %q", item.version, appliedName, item.name)
			}
			if checksumSupported && appliedChecksum.Valid {
				if appliedChecksum.String != item.checksum {
					return fmt.Errorf("migration %s checksum mismatch: database=%s embedded=%s", item.name, appliedChecksum.String, item.checksum)
				}
			} else if err := validateLegacyMigrationBaseline(item); err != nil {
				return err
			}
			continue
		}

		tx, err := db.sql.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", item.name, err)
		}
		if _, err := tx.ExecContext(ctx, string(item.sql)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", item.name, err)
		}

		txChecksumSupported, err := migrationChecksumColumnExists(ctx, tx)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("inspect schema after migration %s: %w", item.name, err)
		}
		if txChecksumSupported {
			_, err = tx.ExecContext(ctx,
				"INSERT INTO schema_migrations(version, name, checksum) VALUES(?, ?, ?)",
				item.version, item.name, item.checksum,
			)
		} else {
			_, err = tx.ExecContext(ctx,
				"INSERT INTO schema_migrations(version, name) VALUES(?, ?)",
				item.version, item.name,
			)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", item.name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", item.name, err)
		}
		checksumSupported = txChecksumSupported
	}

	if checksumSupported {
		if err := db.backfillAndValidateMigrationChecksums(ctx, migrations); err != nil {
			return err
		}
	}
	return nil
}

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

func loadMigrations() ([]migration, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	items := make([]migration, 0, len(entries))
	seenVersions := make(map[int]string)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		version, err := migrationVersion(entry.Name())
		if err != nil {
			return nil, err
		}
		if previous, exists := seenVersions[version]; exists {
			return nil, fmt.Errorf("migration version %d is duplicated by %q and %q", version, previous, entry.Name())
		}
		seenVersions[version] = entry.Name()

		sqlBytes, err := migrationFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		sum := sha256.Sum256(sqlBytes)
		items = append(items, migration{
			version:  version,
			name:     entry.Name(),
			sql:      sqlBytes,
			checksum: "sha256:" + hex.EncodeToString(sum[:]),
		})
	}

	sort.Slice(items, func(i, j int) bool { return items[i].version < items[j].version })
	for i, item := range items {
		want := i + 1
		if item.version != want {
			return nil, fmt.Errorf("migration sequence has version %d at position %d, want %d", item.version, i, want)
		}
	}
	return items, nil
}

func readMigrationRecord(ctx context.Context, db *sql.DB, version int, checksumSupported bool) (bool, string, sql.NullString, error) {
	var name string
	var checksum sql.NullString
	if checksumSupported {
		err := db.QueryRowContext(ctx,
			"SELECT name, checksum FROM schema_migrations WHERE version = ?", version,
		).Scan(&name, &checksum)
		if err == nil {
			return true, name, checksum, nil
		}
		if err == sql.ErrNoRows {
			return false, "", sql.NullString{}, nil
		}
		return false, "", sql.NullString{}, err
	}

	err := db.QueryRowContext(ctx,
		"SELECT name FROM schema_migrations WHERE version = ?", version,
	).Scan(&name)
	if err == nil {
		return true, name, sql.NullString{}, nil
	}
	if err == sql.ErrNoRows {
		return false, "", sql.NullString{}, nil
	}
	return false, "", sql.NullString{}, err
}

func migrationChecksumColumnExists(ctx context.Context, q queryContext) (bool, error) {
	rows, err := q.QueryContext(ctx, "PRAGMA table_info(schema_migrations)")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull int
		var defaultValue any
		var primaryKey int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == "checksum" {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (db *DB) backfillAndValidateMigrationChecksums(ctx context.Context, migrations []migration) error {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration checksum validation: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	for _, item := range migrations {
		var name string
		var checksum sql.NullString
		if err := tx.QueryRowContext(ctx,
			"SELECT name, checksum FROM schema_migrations WHERE version = ?", item.version,
		).Scan(&name, &checksum); err != nil {
			return fmt.Errorf("read migration %s checksum: %w", item.name, err)
		}
		if name != item.name {
			return fmt.Errorf("migration version %d recorded as %q, embedded file is %q", item.version, name, item.name)
		}
		if checksum.Valid {
			if checksum.String != item.checksum {
				return fmt.Errorf("migration %s checksum mismatch: database=%s embedded=%s", item.name, checksum.String, item.checksum)
			}
			continue
		}
		if err := validateLegacyMigrationBaseline(item); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx,
			"UPDATE schema_migrations SET checksum = ? WHERE version = ? AND name = ? AND checksum IS NULL",
			item.checksum, item.version, item.name,
		)
		if err != nil {
			return fmt.Errorf("backfill migration %s checksum: %w", item.name, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read migration %s checksum update result: %w", item.name, err)
		}
		if rows != 1 {
			return fmt.Errorf("migration %s checksum backfill affected %d rows, want 1", item.name, rows)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration checksum validation: %w", err)
	}
	rollback = false
	return nil
}

func migrationVersion(name string) (int, error) {
	base := strings.TrimSuffix(name, filepath.Ext(name))
	prefix, _, ok := strings.Cut(base, "_")
	if !ok {
		return 0, fmt.Errorf("migration %q must use NNNN_name.sql format", name)
	}
	version, err := strconv.Atoi(prefix)
	if err != nil || version <= 0 {
		return 0, fmt.Errorf("migration %q has invalid positive numeric version", name)
	}
	return version, nil
}
