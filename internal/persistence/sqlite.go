// Package persistence owns Replay's local transactional metadata database.
package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"

	_ "modernc.org/sqlite"
)

// DB wraps Replay's SQLite metadata/index database.
type DB struct {
	sql *sql.DB
}

// Open opens or creates a Replay metadata database and applies all embedded
// schema migrations before returning it to the caller.
func Open(ctx context.Context, path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}

	sqlDB, err := sql.Open("sqlite", sqliteDSN(abs))
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	// WAL permits concurrent readers. SQLite still has one writer, so keep the
	// pool bounded and rely on busy_timeout rather than creating a large pool of
	// competing write connections.
	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(4)

	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping sqlite database: %w", err)
	}

	db := &DB{sql: sqlDB}
	if err := db.migrate(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return db, nil
}

func sqliteDSN(path string) string {
	slashPath := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && len(slashPath) >= 2 && slashPath[1] == ':' {
		slashPath = "/" + slashPath
	}

	u := url.URL{Scheme: "file", Path: slashPath}
	q := u.Query()
	q.Set("_busy_timeout", "5000")
	q.Set("_foreign_keys", "on")
	q.Set("_journal_mode", "WAL")
	q.Set("_synchronous", "FULL")
	q.Set("_defensive", "1")
	q.Set("_dqs", "0")
	u.RawQuery = q.Encode()
	return u.String()
}

// Close closes the underlying database handle.
func (db *DB) Close() error { return db.sql.Close() }
