// Package certmagicsqlite provides a SQLite-backed storage implementation
// for CertMagic. It implements the certmagic.Storage and certmagic.Locker
// interfaces using a pure Go SQLite driver (no CGO required).
package certmagicsqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

var (
	_ certmagic.Storage = (*SQLiteStorage)(nil)
	_ certmagic.Locker  = (*SQLiteStorage)(nil)
)

// SQLiteStorage implements certmagic.Storage and certmagic.Locker
// using a SQLite database for persistence.
type SQLiteStorage struct {
	db           *sql.DB
	lockTTL      time.Duration
	queryTimeout time.Duration
	ownerID      string
	locks        map[string]struct{}
	locksMu      sync.Mutex
	managedDB    bool // true if we opened the DB and should close it
}

// Option configures a SQLiteStorage instance.
type Option func(*SQLiteStorage)

// WithLockTTL sets the duration after which locks expire.
// Default is 2 minutes.
func WithLockTTL(d time.Duration) Option {
	return func(s *SQLiteStorage) { s.lockTTL = d }
}

// WithQueryTimeout sets the timeout for database queries.
// Default is 3 seconds.
func WithQueryTimeout(d time.Duration) Option {
	return func(s *SQLiteStorage) { s.queryTimeout = d }
}

// WithOwnerID sets a custom owner identifier for distributed locking.
// By default, a random UUID is generated on each instantiation.
//
// Providing a stable ownerID (e.g., hostname, instance ID) allows the
// application to clean up its own stale locks after a restart, rather
// than waiting for them to expire.
//
// For single-instance deployments, use a stable ID like hostname.
// For multi-instance deployments, ensure each instance has a unique ID.
func WithOwnerID(id string) Option {
	return func(s *SQLiteStorage) { s.ownerID = id }
}

// New creates a new SQLiteStorage instance. The dsn parameter should be
// a path to the SQLite database file, or ":memory:" for an in-memory database.
// The storage will manage the database connection and close it when Close() is called.
// Recommended PRAGMAs (WAL, synchronous=NORMAL, busy_timeout) are applied automatically.
func New(dsn string, opts ...Option) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := applyPragmas(db, dsn); err != nil {
		db.Close()
		return nil, err
	}

	s, err := NewWithDB(db, opts...)
	if err != nil {
		db.Close()
		return nil, err
	}
	s.managedDB = true

	return s, nil
}

func applyPragmas(db *sql.DB, dsn string) error {
	isMemory := dsn == ":memory:" || strings.Contains(dsn, "mode=memory")

	pragmas := []string{
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	}
	if !isMemory {
		pragmas = append([]string{"PRAGMA journal_mode=WAL"}, pragmas...)
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	return nil
}

// NewWithDB creates a new SQLiteStorage instance using an existing database connection.
// This allows sharing a SQLite database with other parts of your application.
// The caller is responsible for closing the database connection.
// Schema migrations will be run automatically.
func NewWithDB(db *sql.DB, opts ...Option) (*SQLiteStorage, error) {
	s := &SQLiteStorage{
		db:           db,
		lockTTL:      2 * time.Minute,
		queryTimeout: 3 * time.Second,
		ownerID:      uuid.New().String(),
		locks:        make(map[string]struct{}),
		managedDB:    false,
	}
	for _, opt := range opts {
		opt(s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	if err := s.initSchema(db); err != nil {
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return s, nil
}

// Close closes the database connection if it was opened by New().
// If the storage was created with NewWithDB(), this is a no-op.
func (s *SQLiteStorage) Close() error {
	if s.managedDB {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStorage) initSchema(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS certmagic_data (
		key TEXT PRIMARY KEY,
		value BLOB NOT NULL,
		modified INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS certmagic_locks (
		name TEXT PRIMARY KEY,
		expires_at INTEGER NOT NULL,
		owner_id TEXT NOT NULL
	);`

	_, err := db.Exec(schema)
	return err
}

func normalizeKey(key string) string {
	key = strings.ReplaceAll(key, "\\", "/")
	key = strings.TrimPrefix(key, "/")
	for strings.Contains(key, "//") {
		key = strings.ReplaceAll(key, "//", "/")
	}
	return key
}

func prefixRange(prefix string) (string, string) {
	if prefix == "" {
		return "", "\uffff"
	}
	return prefix, prefix + "\uffff"
}

// Store saves the value at the given key.
func (s *SQLiteStorage) Store(ctx context.Context, key string, value []byte) error {
	key = normalizeKey(key)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO certmagic_data(key, value, modified)
		VALUES(?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value=excluded.value,
			modified=excluded.modified`,
		key, value, time.Now().UnixMilli())
	return err
}

// Load retrieves the value at the given key.
// Returns fs.ErrNotExist if the key does not exist.
func (s *SQLiteStorage) Load(ctx context.Context, key string) ([]byte, error) {
	key = normalizeKey(key)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	var value []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM certmagic_data WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fs.ErrNotExist
	}
	return value, err
}

// Delete removes the key and all keys with the same prefix.
func (s *SQLiteStorage) Delete(ctx context.Context, key string) error {
	key = normalizeKey(key)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	if key == "" {
		_, err := s.db.ExecContext(ctx, `DELETE FROM certmagic_data`)
		return err
	}

	start, end := prefixRange(key + "/")
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM certmagic_data WHERE key = ? OR (key >= ? AND key < ?)`,
		key, start, end)
	return err
}

// Exists returns true if the key exists (either as a file or prefix).
func (s *SQLiteStorage) Exists(ctx context.Context, key string) bool {
	key = normalizeKey(key)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	start, end := prefixRange(key + "/")
	var exists int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM certmagic_data WHERE key = ? OR (key >= ? AND key < ?) LIMIT 1`,
		key, start, end).Scan(&exists)
	return err == nil
}

// List returns all keys with the given prefix.
// If recursive is false, only direct children are returned.
// Returns fs.ErrNotExist if no keys match.
func (s *SQLiteStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	prefix = normalizeKey(prefix)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	prefixDir := prefix
	if prefixDir != "" && !strings.HasSuffix(prefixDir, "/") {
		prefixDir += "/"
	}

	start, end := prefixRange(prefixDir)
	rows, err := s.db.QueryContext(ctx,
		`SELECT key FROM certmagic_data WHERE key >= ? AND key < ? ORDER BY key`,
		start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	var results []string

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}

		if recursive {
			results = append(results, key)
		} else {
			rest := strings.TrimPrefix(key, prefixDir)
			if idx := strings.Index(rest, "/"); idx != -1 {
				rest = rest[:idx]
			}
			if _, ok := seen[rest]; !ok {
				seen[rest] = struct{}{}
				results = append(results, prefixDir+rest)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fs.ErrNotExist
	}
	return results, nil
}

// Stat returns information about the key.
// Returns fs.ErrNotExist if the key does not exist.
func (s *SQLiteStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	key = normalizeKey(key)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	var size int64
	var modified int64
	err := s.db.QueryRowContext(ctx,
		`SELECT LENGTH(value), modified FROM certmagic_data WHERE key = ?`, key).
		Scan(&size, &modified)
	if err == nil {
		return certmagic.KeyInfo{
			Key:        key,
			Modified:   time.UnixMilli(modified),
			Size:       size,
			IsTerminal: true,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return certmagic.KeyInfo{}, err
	}

	start, end := prefixRange(key + "/")
	err = s.db.QueryRowContext(ctx,
		`SELECT MAX(modified) FROM certmagic_data WHERE key >= ? AND key < ?`,
		start, end).Scan(&modified)
	if err != nil || modified == 0 {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}

	return certmagic.KeyInfo{
		Key:        key,
		Modified:   time.UnixMilli(modified),
		Size:       0,
		IsTerminal: false,
	}, nil
}

func isConstraintError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "UNIQUE constraint failed") ||
		strings.Contains(errStr, "constraint failed") ||
		strings.Contains(errStr, "SQLITE_CONSTRAINT")
}

func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "SQLITE_BUSY") ||
		strings.Contains(errStr, "SQLITE_LOCKED")
}

// Lock acquires a named lock, blocking until the lock is acquired or
// the context is cancelled.
func (s *SQLiteStorage) Lock(ctx context.Context, name string) error {
	const pollInterval = 100 * time.Millisecond

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		acquired, err := s.tryLock(ctx, name)
		if err != nil {
			return err
		}
		if acquired {
			s.locksMu.Lock()
			s.locks[name] = struct{}{}
			s.locksMu.Unlock()
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval + time.Duration(rand.Int63n(50))*time.Millisecond):
		}
	}
}

func (s *SQLiteStorage) tryLock(ctx context.Context, name string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	now := time.Now().UnixMilli()
	expiresAt := time.Now().Add(s.lockTTL).UnixMilli()

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO certmagic_locks(name, expires_at, owner_id) VALUES(?, ?, ?)`,
		name, expiresAt, s.ownerID)
	if err == nil {
		return true, nil
	}

	if !isConstraintError(err) {
		if isBusyError(err) {
			return false, nil
		}
		return false, err
	}

	result, err := s.db.ExecContext(ctx,
		`UPDATE certmagic_locks SET expires_at = ?, owner_id = ?
		 WHERE name = ? AND expires_at <= ?`,
		expiresAt, s.ownerID, name, now)
	if err != nil {
		if isBusyError(err) {
			return false, nil
		}
		return false, err
	}
	rows, _ := result.RowsAffected()
	return rows > 0, nil
}

// Unlock releases a named lock.
// Returns fs.ErrNotExist if the lock was not held by this owner.
func (s *SQLiteStorage) Unlock(ctx context.Context, name string) error {
	s.locksMu.Lock()
	delete(s.locks, name)
	s.locksMu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	result, err := s.db.ExecContext(ctx,
		`DELETE FROM certmagic_locks WHERE name = ? AND owner_id = ?`,
		name, s.ownerID)
	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fs.ErrNotExist
	}
	return nil
}
