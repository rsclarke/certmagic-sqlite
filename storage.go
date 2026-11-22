// Package certmagicsqlite provides a SQLite-backed storage implementation
// for CertMagic. It implements the certmagic.Storage and certmagic.Locker
// interfaces using a pure Go SQLite driver (no CGO required).
package certmagicsqlite

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/google/uuid"
	_ "modernc.org/sqlite" // register pure-Go SQLite driver
)

var (
	_ certmagic.Storage = (*SQLiteStorage)(nil)
	_ certmagic.Locker  = (*SQLiteStorage)(nil)
)

const defaultQueryTimeout = 3 * time.Second

// ErrLocksActive means Close was called while a name was reserved for
// acquisition, holding, or release.
// Finish or cancel that work before closing storage.
var ErrLocksActive = errors.New("storage has active locks")

// SQLiteStorage implements certmagic.Storage and certmagic.Locker
// using a SQLite database for persistence.
type SQLiteStorage struct {
	db              *sql.DB
	lockTTL         time.Duration
	refreshInterval *time.Duration
	queryTimeout    time.Duration
	ownerID         string
	locks           map[string]*lockState
	locksMu         sync.Mutex
	managedDB       bool // true if we opened the DB and should close it
	closed          bool
	closeOnce       sync.Once
	closeErr        error
}

// A record reserves its name until acquisition fails or Unlock finishes.
// cancel and releasing are protected by locksMu. Closing done signals that
// refresh has stopped; it does not report whether the lease was lost.
type lockState struct {
	ticket    string
	released  chan struct{}
	cancel    context.CancelFunc
	done      chan struct{}
	releasing bool
}

// Option configures a SQLiteStorage instance.
type Option func(*SQLiteStorage)

// WithLockTTL sets the duration after the last refresh at which a lock expires.
// Default is 2 minutes; the minimum is 1 millisecond, but production values need
// scheduling headroom. WithRefreshInterval controls how often held locks refresh.
func WithLockTTL(d time.Duration) Option {
	return func(s *SQLiteStorage) { s.lockTTL = d }
}

// WithRefreshInterval sets the interval between lock refresh attempts.
// When omitted, the interval defaults to one third of the configured lock TTL.
// An explicit interval must be positive and less than the TTL rounded down to
// milliseconds. Choose an interval well below the TTL to allow for delays.
func WithRefreshInterval(d time.Duration) Option {
	return func(s *SQLiteStorage) { s.refreshInterval = &d }
}

// WithQueryTimeout sets the timeout for database queries.
// Default is 3 seconds.
func WithQueryTimeout(d time.Duration) Option {
	return func(s *SQLiteStorage) { s.queryTimeout = d }
}

// WithOwnerID sets a label used in unique per-acquisition lock tickets.
// By default, a random UUID is generated on each instantiation.
//
// Reusing a label does not authorize unlocking another acquisition, including
// after a restart. Abandoned locks must expire before they can be acquired.
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
		return nil, closeWithJoin(err, db)
	}

	s, err := NewWithDB(db, opts...)
	if err != nil {
		return nil, closeWithJoin(err, db)
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

	ctx, cancel := context.WithTimeout(context.Background(), defaultQueryTimeout)
	defer cancel()

	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	return nil
}

func closeWithJoin(baseErr error, db *sql.DB) error {
	if closeErr := db.Close(); closeErr != nil {
		return errors.Join(baseErr, fmt.Errorf("close database: %w", closeErr))
	}

	return baseErr
}

// NewWithDB creates a new SQLiteStorage instance using an existing database connection.
// This allows sharing a SQLite database with other parts of your application.
// The caller is responsible for closing the database connection.
// Schema migrations will be run automatically.
func NewWithDB(db *sql.DB, opts ...Option) (*SQLiteStorage, error) {
	s := &SQLiteStorage{
		db:           db,
		lockTTL:      2 * time.Minute,
		queryTimeout: defaultQueryTimeout,
		ownerID:      uuid.New().String(),
		locks:        make(map[string]*lockState),
		managedDB:    false,
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.lockTTL < time.Millisecond {
		return nil, errors.New("lock TTL must be at least 1 millisecond")
	}
	if s.refreshInterval == nil {
		interval := s.lockTTL / 3
		s.refreshInterval = &interval
	}
	if *s.refreshInterval <= 0 || *s.refreshInterval >= s.lockTTL.Truncate(time.Millisecond) {
		return nil, errors.New("refresh interval must be positive and less than the lock TTL rounded down to milliseconds")
	}
	if s.queryTimeout <= 0 {
		return nil, errors.New("query timeout must be positive")
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

// Close prevents new locks and closes the database if it was opened by New.
// It never closes a database supplied to NewWithDB. A name reserved for
// acquisition, holding, or release makes Close return ErrLocksActive and leaves
// storage usable. A caller waiting for a local reservation can lose the handoff
// race to Close and receive fs.ErrClosed. Cancel pending requests and finish
// all successful acquisitions with Unlock first.
// After a successful Close, do not reuse the storage object.
func (s *SQLiteStorage) Close() error {
	s.locksMu.Lock()
	if len(s.locks) != 0 {
		s.locksMu.Unlock()
		return ErrLocksActive
	}
	s.closed = true
	s.locksMu.Unlock()
	s.closeOnce.Do(func() {
		if s.managedDB {
			if err := s.db.Close(); err != nil {
				s.closeErr = fmt.Errorf("close database: %w", err)
			}
		}
	})
	return s.closeErr
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

	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()

	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("exec schema: %w", err)
	}
	return nil
}

func normalizeKey(key string) string {
	key = strings.ReplaceAll(key, "\\", "/")
	key = strings.TrimPrefix(key, "/")
	for strings.Contains(key, "//") {
		key = strings.ReplaceAll(key, "//", "/")
	}
	return key
}

// descendantRange bounds the literal component prefix under SQLite's default
// BINARY collation: '/' is immediately before '0', regardless of the suffix.
// An empty start denotes the virtual root and requires an unbounded query.
func descendantRange(key string) (string, string) {
	if key == "" {
		return "", ""
	}
	return key + "/", key + "0"
}

// Store saves the value at the exact key. An empty normalized key is reserved
// for the virtual root and returns an error wrapping fs.ErrInvalid.
func (s *SQLiteStorage) Store(ctx context.Context, key string, value []byte) error {
	key = normalizeKey(key)
	if key == "" {
		return fmt.Errorf("store %q: empty key: %w", key, fs.ErrInvalid)
	}
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO certmagic_data(key, value, modified)
		VALUES(?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value=excluded.value,
			modified=excluded.modified`,
		key, value, time.Now().UnixMilli()); err != nil {
		return fmt.Errorf("store %q: %w", key, err)
	}
	return nil
}

// Load retrieves the value at the exact key. An empty normalized key returns
// an error wrapping fs.ErrInvalid because the virtual root is not a file.
// Returns fs.ErrNotExist if the key does not exist.
func (s *SQLiteStorage) Load(ctx context.Context, key string) ([]byte, error) {
	key = normalizeKey(key)
	if key == "" {
		return nil, fmt.Errorf("load %q: empty key: %w", key, fs.ErrInvalid)
	}
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	var value []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM certmagic_data WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fs.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("load %q: %w", key, err)
	}
	return value, nil
}

// Delete atomically removes the exact key and its component descendants.
// The empty key clears data, preserving the schema, locks, and virtual root.
func (s *SQLiteStorage) Delete(ctx context.Context, key string) error {
	key = normalizeKey(key)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	if key == "" {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM certmagic_data`); err != nil {
			return fmt.Errorf("delete all: %w", err)
		}
		return nil
	}

	start, end := descendantRange(key)
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM certmagic_data WHERE key = ? OR (key >= ? AND key < ?)`,
		key, start, end); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}

// Exists returns true if the key exists as a file, implicit directory, or the
// virtual root (empty key). Any database or context error returns false.
func (s *SQLiteStorage) Exists(ctx context.Context, key string) bool {
	key = normalizeKey(key)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	start, end := descendantRange(key)
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM certmagic_data WHERE key = ? OR (key >= ? AND key < ?)) OR ? = ''`,
		key, start, end, key).Scan(&exists)
	return err == nil && ctx.Err() == nil && exists
}

// List returns children of the given path, excluding the path itself.
// Recursive listings include implicit directories as well as terminal keys;
// otherwise only direct children are returned. Existing files and an empty
// virtual root list successfully with no children. Missing paths return
// fs.ErrNotExist.
func (s *SQLiteStorage) List(ctx context.Context, prefix string, recursive bool) (_ []string, retErr error) {
	prefix = normalizeKey(prefix)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	// Preserve List's existing acceptance of a trailing slash.
	start, end := descendantRange(strings.TrimSuffix(prefix, "/"))
	if prefix == "/" {
		// Repeated leading separators normalize to a literal slash prefix,
		// not the virtual root. Bound that prefix without trimming it away.
		start, end = "/", "0"
	}
	var rows *sql.Rows
	var err error
	if prefix == "" {
		rows, err = s.db.QueryContext(ctx, `SELECT key FROM certmagic_data ORDER BY key`)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT key FROM certmagic_data WHERE key = ? OR (key >= ? AND key < ?) ORDER BY key`,
			prefix, start, end)
	}
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", prefix, err)
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()

	seen := make(map[string]struct{})
	var results []string
	exists := prefix == ""

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("list scan: %w", err)
		}

		exists = true
		if key == prefix {
			continue
		}
		dir, rest := start, strings.TrimPrefix(key, start)
		for {
			component, remaining, hasMore := strings.Cut(rest, "/")
			child := dir + component
			if _, ok := seen[child]; !ok && child != prefix {
				seen[child] = struct{}{}
				results = append(results, child)
			}
			if !recursive || !hasMore {
				break
			}
			dir, rest = child+"/", remaining
		}
	}

	if err := errors.Join(rows.Err(), ctx.Err()); err != nil {
		return nil, fmt.Errorf("list rows: %w", err)
	}
	if !exists {
		return nil, fs.ErrNotExist
	}
	return results, nil
}

// Stat returns information about the exact key or an implicit directory.
// The empty key is always a directory in initialized, accessible storage.
// Returns fs.ErrNotExist if the key does not exist.
func (s *SQLiteStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	key = normalizeKey(key)
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	var size int64
	var modified int64
	err := s.db.QueryRowContext(ctx,
		`SELECT LENGTH(value), modified FROM certmagic_data WHERE key = ? AND key <> ''`, key).
		Scan(&size, &modified)
	err = errors.Join(err, ctx.Err())
	if err == nil {
		return certmagic.KeyInfo{
			Key:        key,
			Modified:   time.UnixMilli(modified),
			Size:       size,
			IsTerminal: true,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return certmagic.KeyInfo{}, fmt.Errorf("stat %q: %w", key, err)
	}

	start, end := descendantRange(key)
	var directoryModified sql.NullInt64
	if start == "" {
		err = s.db.QueryRowContext(ctx, `SELECT MAX(modified) FROM certmagic_data WHERE key <> ''`).Scan(&directoryModified)
	} else {
		err = s.db.QueryRowContext(ctx,
			`SELECT MAX(modified) FROM certmagic_data WHERE key >= ? AND key < ?`,
			start, end).Scan(&directoryModified)
	}
	err = errors.Join(err, ctx.Err())
	if err != nil {
		return certmagic.KeyInfo{}, fmt.Errorf("stat directory %q: %w", key, err)
	}
	if !directoryModified.Valid && key != "" {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}

	info := certmagic.KeyInfo{
		Key:        key,
		Size:       0,
		IsTerminal: false,
	}
	if directoryModified.Valid {
		info.Modified = time.UnixMilli(directoryModified.Int64)
	}
	return info, nil
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

func lockPollJitter(maxMillis int64) time.Duration {
	if maxMillis <= 0 {
		return 0
	}

	n, err := crand.Int(crand.Reader, big.NewInt(maxMillis))
	if err != nil {
		return 0
	}

	return time.Duration(n.Int64()) * time.Millisecond
}

// Lock acquires a named lock, blocking until it is available or ctx is cancelled.
// Successful acquisitions refresh until Unlock, independently of ctx. A long
// process pause or database outage can still cause lease loss and overlapping
// work; this interface cannot fence certificate writes by a former holder.
func (s *SQLiteStorage) Lock(ctx context.Context, name string) error {
	const pollInterval = 100 * time.Millisecond

	state, err := s.reserveLock(ctx, name)
	if err != nil {
		return fmt.Errorf("lock %q: %w", name, err)
	}
	held := false
	defer func() {
		if !held {
			s.forgetLock(name, state)
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("lock %q: %w", name, err)
		}

		expiry, err := s.tryLock(ctx, name, state.ticket)
		if err != nil {
			// A cancelled statement may have committed before reporting failure.
			cleanupErr := s.deleteLock(context.WithoutCancel(ctx), name, state.ticket)
			return errors.Join(err, cleanupErr)
		}
		if expiry != 0 {
			if ctx.Err() != nil || expiry <= time.Now().UnixMilli() {
				err := s.deleteLock(context.WithoutCancel(ctx), name, state.ticket)
				if err != nil {
					return errors.Join(ctx.Err(), err)
				}
				continue
			}
			refreshCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			s.locksMu.Lock()
			state.cancel = cancel
			state.done = make(chan struct{})
			s.locksMu.Unlock()
			held = true
			go s.refreshLock(refreshCtx, name, state, expiry)
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("lock %q: %w", name, ctx.Err())
		case <-time.After(pollInterval + lockPollJitter(50)):
		}
	}
}

func (s *SQLiteStorage) reserveLock(ctx context.Context, name string) (*lockState, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("reserve lock: %w", err)
		}
		s.locksMu.Lock()
		if s.closed {
			s.locksMu.Unlock()
			return nil, fs.ErrClosed
		}
		previous := s.locks[name]
		if previous == nil {
			state := &lockState{
				ticket:   s.ownerID + "/" + uuid.NewString(),
				released: make(chan struct{}),
			}
			s.locks[name] = state
			s.locksMu.Unlock()
			return state, nil
		}
		s.locksMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for local lock: %w", ctx.Err())
		case <-previous.released:
		}
	}
}

func (s *SQLiteStorage) forgetLock(name string, state *lockState) {
	s.locksMu.Lock()
	delete(s.locks, name)
	close(state.released)
	s.locksMu.Unlock()
}

// SQLite evaluates 'now' when the statement runs, not before connection-pool waits.
const sqliteNowMillis = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

func (s *SQLiteStorage) tryLock(ctx context.Context, name, ticket string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	var expiry int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO certmagic_locks(name, expires_at, owner_id)
		VALUES(?, `+sqliteNowMillis+` + ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			expires_at = excluded.expires_at, owner_id = excluded.owner_id
		WHERE certmagic_locks.expires_at <= `+sqliteNowMillis+`
		RETURNING expires_at`, name, s.lockTTL.Milliseconds(), ticket).Scan(&expiry)
	// Cancellation can hide a RETURNING finalization error in database/sql.
	// A scanned expiry alone does not establish that the write committed.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, fmt.Errorf("acquire lock %q: %w", name, errors.Join(err, ctxErr))
	}
	if errors.Is(err, sql.ErrNoRows) || isBusyError(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("acquire lock %q: %w", name, err)
	}
	return expiry, nil
}

func (s *SQLiteStorage) refreshLock(ctx context.Context, name string, state *lockState, expiry int64) {
	defer close(state.done)
	interval := *s.refreshInterval
	// Acquisition may have spent part of the lease waiting for commit. Start
	// from the stored expiry, refreshing immediately if the first tick is due.
	delay := max(0, time.Until(time.UnixMilli(expiry))-s.lockTTL+interval)
	for {
		remaining := time.Until(time.UnixMilli(expiry))
		if remaining <= 0 {
			return
		}
		timer := time.NewTimer(min(delay, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = interval // Subsequent attempts keep the normal retry cadence.
		remaining = time.Until(time.UnixMilli(expiry))
		if remaining <= 0 {
			return
		}
		queryCtx, cancel := context.WithTimeout(ctx, min(s.queryTimeout, interval, remaining))
		var nextExpiry int64
		err := s.db.QueryRowContext(queryCtx, `
			UPDATE certmagic_locks SET expires_at = `+sqliteNowMillis+` + ?
			WHERE name = ? AND owner_id = ? AND expires_at > `+sqliteNowMillis+`
			RETURNING expires_at`, s.lockTTL.Milliseconds(), name, state.ticket).Scan(&nextExpiry)
		err = errors.Join(err, queryCtx.Err()) // Check before our own cancellation.
		cancel()
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			return
		}
		if err == nil {
			expiry = nextExpiry
		}
	}
}

// Unlock ends one successful acquisition, even if database cleanup fails.
// Call it once, after the critical section ends. It stops refresh, removes only
// that acquisition's row, and wakes local waiters. A failed deletion leaves a
// row to expire; do not retry an old Unlock after it returns. A missing, expired,
// or replaced row is not an error. fs.ErrNotExist means no local acquisition is
// available to release. Cancellation does not skip local cleanup.
func (s *SQLiteStorage) Unlock(ctx context.Context, name string) error {
	s.locksMu.Lock()
	state := s.locks[name]
	if state == nil || state.cancel == nil || state.releasing {
		s.locksMu.Unlock()
		return fs.ErrNotExist
	}
	state.releasing = true
	s.locksMu.Unlock()
	defer s.forgetLock(name, state)
	state.cancel()
	<-state.done
	if err := s.deleteLock(ctx, name, state.ticket); err != nil {
		return fmt.Errorf("unlock %q: %w", name, err)
	}
	return nil
}

func (s *SQLiteStorage) deleteLock(ctx context.Context, name, ticket string) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	_, err := s.db.ExecContext(ctx,
		`DELETE FROM certmagic_locks WHERE name = ? AND owner_id = ?`, name, ticket)
	err = errors.Join(err, ctx.Err())
	if err != nil {
		return fmt.Errorf("delete lock %q: %w", name, err)
	}
	return nil
}
