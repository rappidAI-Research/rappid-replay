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
// schema migrations before returning it to the caller. The metadata database is
// not the encrypted CAS, so Replay enforces private filesystem permissions at
// this boundary rather than relying on the caller's umask.
func Open(ctx context.Context, path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	dir := filepath.Dir(abs)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("set database directory permissions: %w", err)
	}

	// Create the database ourselves with a restrictive mode before SQLite gets
	// a chance to create it using process umask defaults. Chmod also hardens a
	// pre-existing Replay database that was created by an older version.
	file, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("prepare sqlite database file: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("set sqlite database permissions: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close prepared sqlite database file: %w", err)
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

	db := &DB{sql: sqlDB}
	// Connection establishment itself can execute DSN PRAGMAs such as
	// journal_mode=WAL. On Windows two first-openers can otherwise contend before
	// the migration lock is reached and one Ping returns SQLITE_BUSY. Keep the
	// initial Ping and migrations under the same cross-process lock.
	if err := db.initializeWithLock(ctx, abs); err != nil {
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
