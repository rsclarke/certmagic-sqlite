package certmagicsqlite

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"
	"modernc.org/sqlite"
)

func newTestStorage(t *testing.T, opts ...Option) *SQLiteStorage {
	t.Helper()
	return newTestStorageAt(t, ":memory:", opts...)
}

func newTestStorageAt(t *testing.T, dsn string, opts ...Option) *SQLiteStorage {
	t.Helper()
	s, err := New(dsn, opts...)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close storage: %v", err)
		}
	})
	return s
}

func TestStoreAndLoad(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	key := "test/key"
	value := []byte("hello world")

	if err := s.Store(ctx, key, value); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	loaded, err := s.Load(ctx, key)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(loaded) != string(value) {
		t.Errorf("Load returned %q, want %q", loaded, value)
	}
}

func TestLoadNotExist(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	_, err := s.Load(ctx, "nonexistent")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Load returned %v, want fs.ErrNotExist", err)
	}
}

func TestStoreOverwrite(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	key := "test/overwrite"
	if err := s.Store(ctx, key, []byte("first")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	if err := s.Store(ctx, key, []byte("second")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	loaded, err := s.Load(ctx, key)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(loaded) != "second" {
		t.Errorf("Load returned %q, want %q", loaded, "second")
	}
}

func TestExists(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	if s.Exists(ctx, "nonexistent") {
		t.Error("Exists returned true for nonexistent key")
	}

	if err := s.Store(ctx, "a/b/c", []byte("data")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if !s.Exists(ctx, "a/b/c") {
		t.Error("Exists returned false for existing key")
	}
	if !s.Exists(ctx, "a/b") {
		t.Error("Exists returned false for existing prefix")
	}
	if !s.Exists(ctx, "a") {
		t.Error("Exists returned false for existing prefix")
	}
}

func TestStat(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	key := "test/stat"
	value := []byte("test data")
	before := time.Now().Unix()

	if err := s.Store(ctx, key, value); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	info, err := s.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Key != key {
		t.Errorf("Stat Key = %q, want %q", info.Key, key)
	}
	if info.Size != int64(len(value)) {
		t.Errorf("Stat Size = %d, want %d", info.Size, len(value))
	}
	if !info.IsTerminal {
		t.Error("Stat IsTerminal = false, want true")
	}
	if info.Modified.Unix() < before {
		t.Errorf("Stat Modified = %v, want >= %v", info.Modified.Unix(), before)
	}
}

func TestStatDirectory(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	if err := s.Store(ctx, "a/b/c", []byte("data")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	info, err := s.Stat(ctx, "a/b")
	if err != nil {
		t.Fatalf("Stat directory failed: %v", err)
	}
	if info.Key != "a/b" {
		t.Errorf("Stat Key = %q, want %q", info.Key, "a/b")
	}
	if info.IsTerminal {
		t.Error("Stat IsTerminal = true, want false for directory")
	}
	if info.Size != 0 {
		t.Errorf("Stat Size = %d, want 0 for directory", info.Size)
	}

	info, err = s.Stat(ctx, "a")
	if err != nil {
		t.Fatalf("Stat parent directory failed: %v", err)
	}
	if info.IsTerminal {
		t.Error("Stat IsTerminal = true, want false for directory")
	}
}

func TestStatNotExist(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	_, err := s.Stat(ctx, "nonexistent")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Stat returned %v, want fs.ErrNotExist", err)
	}
}

func TestDelete(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	if err := s.Store(ctx, "a/b/c", []byte("1")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	if err := s.Store(ctx, "a/b/d", []byte("2")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	if err := s.Store(ctx, "a/x", []byte("3")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if err := s.Delete(ctx, "a/b"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if s.Exists(ctx, "a/b/c") {
		t.Error("a/b/c still exists after delete")
	}
	if s.Exists(ctx, "a/b/d") {
		t.Error("a/b/d still exists after delete")
	}
	if !s.Exists(ctx, "a/x") {
		t.Error("a/x should not have been deleted")
	}
}

func TestDeleteExact(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	if err := s.Store(ctx, "foo", []byte("1")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	if err := s.Store(ctx, "foobar", []byte("2")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if err := s.Delete(ctx, "foo"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if s.Exists(ctx, "foo") {
		t.Error("foo still exists after delete")
	}
	if !s.Exists(ctx, "foobar") {
		t.Error("foobar should not have been deleted")
	}
}

func TestListRecursive(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	keys := []string{"a/b/c", "a/b/d", "a/x"}
	for _, k := range keys {
		if err := s.Store(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Store failed: %v", err)
		}
	}

	list, err := s.List(ctx, "a", true)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("List returned %d items, want 3: %v", len(list), list)
	}

	expected := map[string]bool{"a/b/c": true, "a/b/d": true, "a/x": true}
	for _, k := range list {
		if !expected[k] {
			t.Errorf("unexpected key in list: %s", k)
		}
	}
}

func TestListNonRecursive(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	keys := []string{"a/b/c", "a/b/d", "a/x"}
	for _, k := range keys {
		if err := s.Store(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Store failed: %v", err)
		}
	}

	list, err := s.List(ctx, "a", false)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("List returned %d items, want 2: %v", len(list), list)
	}

	expected := map[string]bool{"a/b": true, "a/x": true}
	for _, k := range list {
		if !expected[k] {
			t.Errorf("unexpected key in list: %s", k)
		}
	}
}

func TestListNotExist(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	_, err := s.List(ctx, "nonexistent", true)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("List returned %v, want fs.ErrNotExist", err)
	}
}

func TestUnlockRequiresLocalAcquisition(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	err := s.Unlock(ctx, "never-locked")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Unlock without Lock returned %v, want fs.ErrNotExist", err)
	}
}

func TestLockWaitCancellationAllowsReuse(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	lockName := "contention-lock"

	if err := s.Lock(ctx, lockName); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	ctx2, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err := s.Lock(ctx2, lockName)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	if err := s.Unlock(ctx, lockName); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	if err := s.Lock(ctx, lockName); err != nil {
		t.Fatalf("Lock after unlock failed: %v", err)
	}
	if err := s.Unlock(ctx, lockName); err != nil {
		t.Fatalf("final unlock failed: %v", err)
	}
}

func TestConcurrentStoreLoad(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	const goroutines = 10
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				key := "concurrent/key"
				value := []byte("data")
				if err := s.Store(ctx, key, value); err != nil {
					t.Errorf("Store failed: %v", err)
					return
				}
				if _, err := s.Load(ctx, key); err != nil {
					t.Errorf("Load failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestKeyNormalization(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	if err := s.Store(ctx, "\\path\\to\\key", []byte("data")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if _, err := s.Load(ctx, "path/to/key"); err != nil {
		t.Errorf("Load with normalized key failed: %v", err)
	}

	if err := s.Store(ctx, "/leading/slash", []byte("data")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if _, err := s.Load(ctx, "leading/slash"); err != nil {
		t.Errorf("Load without leading slash failed: %v", err)
	}

	if err := s.Store(ctx, "double//slash", []byte("data")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if _, err := s.Load(ctx, "double/slash"); err != nil {
		t.Errorf("Load with collapsed slash failed: %v", err)
	}
}

func TestDeleteAll(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	keys := []string{"a/b/c", "x/y/z", "root"}
	for _, k := range keys {
		if err := s.Store(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Store failed: %v", err)
		}
	}

	if err := s.Delete(ctx, ""); err != nil {
		t.Fatalf("Delete all failed: %v", err)
	}

	for _, k := range keys {
		if s.Exists(ctx, k) {
			t.Errorf("key %s still exists after delete all", k)
		}
	}
}

func TestMemoryDB(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create in-memory storage: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close storage: %v", err)
		}
	}()

	ctx := context.Background()
	if err := s.Store(ctx, "test", []byte("data")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	val, err := s.Load(ctx, "test")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(val) != "data" {
		t.Errorf("Load returned %q, want %q", val, "data")
	}
}

func TestNewWithDB(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	// A private in-memory database belongs to one connection; keep it alive.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close database: %v", err)
		}
	}()
	ctx := context.Background()

	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS app_data (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create app table: %v", err)
	}

	s, err := NewWithDB(db)
	if err != nil {
		t.Fatalf("NewWithDB failed: %v", err)
	}

	if err := s.Store(ctx, "cert/example.com", []byte("certificate data")); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	val, err := s.Load(ctx, "cert/example.com")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if string(val) != "certificate data" {
		t.Errorf("Load returned %q, want %q", val, "certificate data")
	}

	_, err = db.ExecContext(ctx, "INSERT INTO app_data (value) VALUES (?)", "app value")
	if err != nil {
		t.Fatalf("failed to insert app data: %v", err)
	}

	if err := s.Lock(ctx, "lock-a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); !errors.Is(err, ErrLocksActive) {
		t.Fatalf("shared storage Close with active lock = %v", err)
	}
	if err := s.Unlock(ctx, "lock-a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := s.Lock(ctx, "lock-a"); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed shared storage Lock = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("repeated Close = %v", err)
	}

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM app_data").Scan(&count)
	if err != nil {
		t.Fatalf("DB should still be usable after storage.Close(): %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row in app_data, got %d", count)
	}
}

func TestMillisecondTTL(t *testing.T) {
	for _, ttl := range []time.Duration{-time.Second, 0, time.Microsecond, time.Millisecond} {
		t.Run(ttl.String(), func(t *testing.T) {
			s, err := New(":memory:", WithLockTTL(ttl))
			if ttl < time.Millisecond {
				if err == nil {
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
					t.Fatal("expected invalid TTL to fail")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueryTimeoutValidation(t *testing.T) {
	for _, timeout := range []time.Duration{-time.Second, 0} {
		t.Run(timeout.String(), func(t *testing.T) {
			s, err := New(":memory:", WithQueryTimeout(timeout))
			if err == nil {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
				t.Fatal("expected invalid query timeout to fail")
			}
		})
	}
}

func TestRefreshInterval(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ttl      time.Duration
		interval time.Duration
		wantErr  bool
	}{
		{"negative", time.Millisecond, -time.Nanosecond, true},
		{"zero", time.Millisecond, 0, true},
		{"equal to TTL", time.Millisecond, time.Millisecond, true},
		{"above TTL", time.Millisecond, 2 * time.Millisecond, true},
		{"just below TTL", time.Millisecond, time.Millisecond - time.Nanosecond, false},
		{"above stored TTL", 1500 * time.Microsecond, 1200 * time.Microsecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, opts := range [][]Option{
				{WithLockTTL(tc.ttl), WithRefreshInterval(tc.interval)},
				{WithRefreshInterval(tc.interval), WithLockTTL(tc.ttl)},
			} {
				s, err := New(":memory:", opts...)
				if err == nil {
					if err := s.Close(); err != nil {
						t.Error(err)
					}
				}
				if (err != nil) != tc.wantErr {
					t.Fatalf("New = %v, want error: %t", err, tc.wantErr)
				}
			}
		})
	}
	for _, tc := range []struct {
		name string
		opts []Option
		min  time.Duration
		max  time.Duration
	}{
		{"default", []Option{WithLockTTL(3 * time.Second)}, 900 * time.Millisecond, 1400 * time.Millisecond},
		{"interval first", []Option{WithRefreshInterval(1500 * time.Millisecond), WithLockTTL(3 * time.Second)}, 1400 * time.Millisecond, 2300 * time.Millisecond},
		{"interval last", []Option{WithLockTTL(3 * time.Second), WithRefreshInterval(500 * time.Millisecond)}, 250 * time.Millisecond, 900 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStorage(t, tc.opts...)
			if err := s.Lock(t.Context(), "lock-a"); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Unlock(t.Context(), "lock-a"); err != nil {
					t.Error(err)
				}
			}()
			previous := lockExpiry(t, s, "lock-a")
			// Inspect persisted expiry changes for both the first and next refresh.
			// The windows allow scheduling delay but exclude the wrong cadence.
			for range 2 {
				var next int64
				waitFor(t, func() bool {
					next = lockExpiry(t, s, "lock-a")
					return next > previous
				})
				if elapsed := time.Duration(next-previous) * time.Millisecond; elapsed < tc.min || elapsed > tc.max {
					t.Fatalf("refresh advanced expiry by %v, want %v–%v", elapsed, tc.min, tc.max)
				}
				previous = next
			}
		})
	}
}

func TestOwnerLabelDoesNotAuthorizeRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "certs.db")
	a := newTestStorageAt(t, path, WithOwnerID("same-label"))
	b := newTestStorageAt(t, path, WithOwnerID("same-label"))
	if err := a.Lock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
	if err := b.Unlock(t.Context(), "lock-a"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("matching label must not authorize Unlock, got %v", err)
	}
	expectLockBlocked(t, b, "lock-a", 100*time.Millisecond)
	if err := a.Unlock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
	if err := b.Lock(t.Context(), "lock-a"); err != nil {
		t.Fatalf("Lock after unlock should succeed: %v", err)
	}
	if err := b.Unlock(t.Context(), "lock-a"); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func expectLockBlocked(t *testing.T, s *SQLiteStorage, name string, duration time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()
	if err := s.Lock(ctx, name); !errors.Is(err, context.DeadlineExceeded) {
		if err == nil {
			if err := s.Unlock(t.Context(), name); err != nil {
				t.Error(err)
			}
		}
		t.Fatalf("Lock(%q) = %v, want deadline exceeded", name, err)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func lockExpiry(t *testing.T, s *SQLiteStorage, name string) int64 {
	t.Helper()
	var expiry int64
	if err := s.db.QueryRowContext(t.Context(), "SELECT expires_at FROM certmagic_locks WHERE name = ?", name).Scan(&expiry); err != nil {
		t.Fatal(err)
	}
	return expiry
}

func TestHealthyLocksSurviveCancellationAndStayIndependent(t *testing.T) {
	const ttl = 600 * time.Millisecond
	path := filepath.Join(t.TempDir(), "certs.db")
	a := newTestStorageAt(t, path, WithLockTTL(ttl))
	b := newTestStorageAt(t, path, WithLockTTL(ttl))
	ctx, cancel := context.WithCancel(t.Context())
	if err := a.Lock(ctx, "lock-a"); err != nil {
		t.Fatal(err)
	}
	cancel() // Acquisition cancellation must not end protection of active work.
	if err := a.Close(); !errors.Is(err, ErrLocksActive) {
		t.Fatalf("Close while holding lock-a = %v", err)
	}
	if err := a.Lock(t.Context(), "lock-b"); err != nil {
		t.Fatal(err)
	}
	if err := a.Store(t.Context(), "lock-a/certificate", []byte("saved")); err != nil {
		t.Fatal(err)
	}
	value, err := b.Load(t.Context(), "lock-a/certificate")
	if err != nil || string(value) != "saved" {
		t.Fatalf("Load = %q, %v", value, err)
	}
	expectLockBlocked(t, b, "lock-a", 3*ttl)
	if err := a.Unlock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
	if err := b.Lock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
	if lockExpiry(t, b, "lock-b") <= time.Now().UnixMilli() {
		t.Fatal("releasing lock-a must not stop lock-b's protection")
	}
	expectLockBlocked(t, b, "lock-b", 100*time.Millisecond)
	if err := b.Unlock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
	if err := a.Unlock(t.Context(), "lock-b"); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseLossPreservesAcquisitionOwnership(t *testing.T) {
	for _, row := range []string{"missing", "expired", "replaced"} {
		t.Run(row, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "certs.db")
			a := newTestStorageAt(t, path, WithOwnerID("same-label"), WithLockTTL(600*time.Millisecond))
			b := newTestStorageAt(t, path, WithOwnerID("same-label"))
			if err := a.Lock(t.Context(), "lock-a"); err != nil {
				t.Fatal(err)
			}
			loss := "DELETE FROM certmagic_locks WHERE name = 'lock-a'"
			if row == "expired" {
				loss = "UPDATE certmagic_locks SET expires_at = 0 WHERE name = 'lock-a'"
			}
			if _, err := b.db.ExecContext(t.Context(), loss); err != nil {
				t.Fatal(err)
			}
			// The row is available: SQL contention must not hide a broken local gate.
			expectLockBlocked(t, a, "lock-a", 800*time.Millisecond)
			if row == "replaced" {
				if err := b.Lock(t.Context(), "lock-a"); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.Unlock(t.Context(), "lock-a"); err != nil {
				t.Fatalf("lost holder Unlock = %v", err)
			}
			if err := a.Unlock(t.Context(), "lock-a"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("second Unlock = %v, want no local acquisition", err)
			}
			if row == "replaced" {
				expectLockBlocked(t, a, "lock-a", 100*time.Millisecond)
				if err := b.Unlock(t.Context(), "lock-a"); err != nil {
					t.Fatal(err)
				}
			}
			if err := a.Lock(t.Context(), "lock-a"); err != nil {
				t.Fatal(err)
			}
			if err := a.Unlock(t.Context(), "lock-a"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAcquisitionLeaseStartsAfterPoolWait(t *testing.T) {
	const ttl = 600 * time.Millisecond
	path := filepath.Join(t.TempDir(), "certs.db")
	a := newTestStorageAt(t, path, WithLockTTL(ttl))
	b := newTestStorageAt(t, path, WithLockTTL(ttl))
	conn, err := a.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			t.Error(err)
		}
	}()
	result := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() { result <- a.Lock(ctx, "lock-a") }()
	waitFor(t, func() bool { return a.db.Stats().WaitCount > 0 })
	time.Sleep(2 * ttl)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	expectLockBlocked(t, b, "lock-a", 100*time.Millisecond)
	if err := a.Unlock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
}

func TestAcquisitionCommitDelayDoesNotLeaveProtectionGap(t *testing.T) {
	const ttl = 1200 * time.Millisecond
	for _, delay := range []time.Duration{time.Second, 1400 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rollback.db")
			openDB := func() *sql.DB {
				db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(3000)")
				if err != nil {
					t.Fatal(err)
				}
				db.SetMaxOpenConns(1)
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				return db
			}
			db, peerDB, reader := openDB(), openDB(), openDB()
			a, err := NewWithDB(db, WithLockTTL(ttl))
			if err != nil {
				t.Fatal(err)
			}
			b, err := NewWithDB(peerDB, WithLockTTL(ttl))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := errors.Join(a.Close(), b.Close()); err != nil {
					t.Error(err)
				}
			})
			var journal string
			if err := reader.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "delete" {
				t.Fatalf("journal = %q, %v; want rollback-journal mode", journal, err)
			}
			tx, err := reader.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
					t.Error(err)
				}
			}()
			var count int
			if err := tx.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM certmagic_locks").Scan(&count); err != nil {
				t.Fatal(err)
			}
			// This reader lets SQLite calculate the expiry and write the row,
			// but blocks commit. Test grants near expiry and already past expiry.
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- a.Lock(ctx, "lock-a") }()
			select {
			case err := <-result:
				t.Fatalf("Lock completed before releasing the reader: %v", err)
			case <-time.After(delay):
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := a.Unlock(t.Context(), "lock-a"); err != nil {
					t.Error(err)
				}
			}()
			// Once acquisition succeeds, healthy work must stay protected, not
			// wait out the remaining lease before attempting its first refresh.
			expectLockBlocked(t, b, "lock-a", 2*ttl)
		})
	}
}

func TestRefreshRetriesOnlyWhileLeaseValid(t *testing.T) {
	const ttl = 900 * time.Millisecond
	t.Run("transient write failure", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "certs.db")
		a := newTestStorageAt(t, path, WithLockTTL(ttl))
		b := newTestStorageAt(t, path, WithLockTTL(ttl))
		if err := a.Lock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
		initial := lockExpiry(t, a, "lock-a")
		if _, err := b.db.ExecContext(t.Context(), `CREATE TRIGGER fail_refresh BEFORE UPDATE ON certmagic_locks
			BEGIN SELECT RAISE(FAIL, 'temporary write failure'); END`); err != nil {
			t.Fatal(err)
		}
		time.Sleep(ttl / 2)
		if got := lockExpiry(t, b, "lock-a"); got != initial {
			t.Fatalf("fault did not prevent refresh: %d != %d", got, initial)
		}
		if _, err := b.db.ExecContext(t.Context(), "DROP TRIGGER fail_refresh"); err != nil {
			t.Fatal(err)
		}
		expectLockBlocked(t, b, "lock-a", 2*ttl)
		if err := a.Unlock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("expired row is not revived", func(t *testing.T) {
		s := newTestStorage(t, WithLockTTL(ttl))
		if err := s.Lock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(t.Context(), "UPDATE certmagic_locks SET expires_at = 0"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(ttl / 2)
		if got := lockExpiry(t, s, "lock-a"); got != 0 {
			t.Fatalf("expired row was revived: %d", got)
		}
		if err := s.Unlock(t.Context(), "lock-a"); err != nil {
			t.Fatalf("expired Unlock = %v", err)
		}
	})
	t.Run("pool stall loses lease", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "certs.db")
		a := newTestStorageAt(t, path, WithLockTTL(ttl))
		b := newTestStorageAt(t, path, WithLockTTL(ttl))
		if err := a.Lock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
		conn, err := a.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
				t.Error(err)
			}
		}()
		time.Sleep(2 * ttl)
		if err := b.Lock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if err := a.Unlock(t.Context(), "lock-a"); err != nil {
			t.Fatalf("stale Unlock = %v", err)
		}
		expectLockBlocked(t, a, "lock-a", 100*time.Millisecond)
		if err := b.Unlock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCancelledUnlockStillReleasesLocalOwnership(t *testing.T) {
	const ttl = 900 * time.Millisecond
	s := newTestStorage(t, WithLockTTL(ttl))
	if err := s.Lock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Unlock(ctx, "lock-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled deletion = %v", err)
	}
	expectLockBlocked(t, s, "lock-a", 100*time.Millisecond)
	ctx, cancel = context.WithTimeout(t.Context(), 3*ttl)
	defer cancel()
	if err := s.Lock(ctx, "lock-a"); err != nil {
		t.Fatalf("abandoned row did not expire: %v", err)
	}
	if err := s.Unlock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
}

func TestFailedDeleteCommitStillReleasesLocalOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollback.db")
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(100)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	s, err := NewWithDB(db, WithLockTTL(600*time.Millisecond), WithQueryTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := s.Lock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	var journal string
	if err := reader.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "delete" {
		t.Fatalf("journal = %q, %v; want rollback-journal mode", journal, err)
	}
	tx, err := reader.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	var before int64
	if err := tx.QueryRowContext(t.Context(), "SELECT expires_at FROM certmagic_locks WHERE name = 'lock-a'").Scan(&before); err != nil {
		t.Fatal(err)
	}
	// The DELETE can execute, but this reader prevents its commit. No driver
	// wrapper is involved: ExecContext must expose the real cleanup failure.
	if err := s.Unlock(t.Context(), "lock-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked DELETE commit = %v, want query deadline", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if after := lockExpiry(t, s, "lock-a"); after != before {
		t.Fatalf("failed cleanup changed lease expiry: %d != %d", after, before)
	}
	if err := s.Unlock(t.Context(), "lock-a"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("failed cleanup retained local acquisition: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := s.Lock(ctx, "lock-a"); err != nil {
		t.Fatalf("abandoned row did not expire: %v", err)
	}
	if err := s.Unlock(t.Context(), "lock-a"); err != nil {
		t.Fatal(err)
	}
}

// SQLite still executes and rolls back the real RETURNING write. This models
// database/sql discarding the first Rows.Close error when cancellation wins
// the close race (observed on Go 1.26), without changing the integer scanner.
type completionFaultConnector struct {
	dsn               string
	operation         string
	enabled           atomic.Bool
	closed            chan error
	lastAttemptMillis atomic.Int64
}

func (*completionFaultConnector) Driver() driver.Driver { return &sqlite.Driver{} }

func (c *completionFaultConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, fmt.Errorf("open fault connection: %w", err)
	}
	return &completionFaultConn{Conn: conn, fault: c}, nil
}

type completionFaultConn struct {
	driver.Conn
	fault *completionFaultConnector
}

func (c *completionFaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	faulting := c.fault.enabled.Load() && strings.HasPrefix(strings.TrimSpace(query), c.fault.operation)
	if faulting {
		// Record attempts even when SQLite rejects the write or finds no row.
		c.fault.lastAttemptMillis.Store(time.Now().UnixMilli())
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, fmt.Errorf("query fault connection: %w", err)
	}
	if faulting {
		return &completionFaultRows{Rows: rows, ctx: ctx, fault: c.fault}, nil
	}
	return rows, nil
}

type completionFaultRows struct {
	driver.Rows
	ctx   context.Context
	fault *completionFaultConnector
}

func (r *completionFaultRows) Close() error {
	<-r.ctx.Done()
	err := r.Rows.Close()
	r.fault.closed <- err
	return nil // Lose the finalization error, as database/sql can on cancellation.
}

func TestCancelledReturningWriteDoesNotConfirmLease(t *testing.T) {
	for _, operation := range []string{"INSERT", "UPDATE"} {
		t.Run(operation, func(t *testing.T) {
			const ttl = 600 * time.Millisecond
			path := filepath.Join(t.TempDir(), "rollback.db")
			fault := &completionFaultConnector{
				dsn: path, operation: operation,
				closed: make(chan error, 10),
			}
			db := sql.OpenDB(fault)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			queryTimeout := 50 * time.Millisecond
			if operation == "UPDATE" {
				// Use the worker's TTL/3 budget so the first failed refresh
				// consumes the last opportunity to retry before confirmed expiry.
				queryTimeout = ttl
			}
			s, err := NewWithDB(db, WithLockTTL(ttl), WithQueryTimeout(queryTimeout))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if operation != "INSERT" {
				if err := s.Lock(t.Context(), "lock-a"); err != nil {
					t.Fatal(err)
				}
			}
			reader, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			}()
			var journal string
			if err := reader.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "delete" {
				t.Fatalf("journal = %q, %v; want rollback-journal mode", journal, err)
			}
			tx, err := reader.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
					t.Error(err)
				}
			}()
			var before int
			var beforeExpiry int64
			if err := tx.QueryRowContext(t.Context(), "SELECT COUNT(*), COALESCE(MAX(expires_at), 0) FROM certmagic_locks").Scan(&before, &beforeExpiry); err != nil {
				t.Fatal(err)
			}
			fault.enabled.Store(true)
			switch operation {
			case "INSERT":
				err = s.Lock(t.Context(), "lock-a")
				if err == nil {
					// Finalize the false acquisition so failure does not leak its worker.
					_ = s.Unlock(t.Context(), "lock-a")
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("acquisition must report its query deadline, got %v", err)
				}
			case "UPDATE":
				// Keep the reader until the confirmed lease is lost. A returned
				// expiry from a rolled-back refresh must not count as protection.
				time.Sleep(2 * ttl)
			}
			select {
			case closeErr := <-fault.closed:
				if closeErr == nil {
					t.Fatal("fixture did not cause SQLite finalization to fail")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("fixture did not reach RETURNING finalization")
			}
			fault.enabled.Store(false)
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			var after int
			var afterExpiry int64
			if err := reader.QueryRowContext(t.Context(), "SELECT COUNT(*), COALESCE(MAX(expires_at), 0) FROM certmagic_locks").Scan(&after, &afterExpiry); err != nil {
				t.Fatal(err)
			}
			if after != before || afterExpiry != beforeExpiry {
				t.Fatalf("cancelled write changed rows/expiry: before=%d/%d after=%d/%d", before, beforeExpiry, after, afterExpiry)
			}
			if operation == "UPDATE" {
				if err := s.Unlock(t.Context(), "lock-a"); err != nil {
					t.Fatalf("cleanup must not report historical refresh failure: %v", err)
				}
				// Rollback alone is insufficient: accepting the uncommitted expiry
				// would allow another attempt beyond the last confirmed deadline.
				if attempted := fault.lastAttemptMillis.Load(); attempted >= beforeExpiry {
					t.Errorf("refresh attempted at %d after confirmed expiry %d", attempted, beforeExpiry)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := s.Lock(ctx, "lock-a"); err != nil {
				t.Fatalf("recovery after cancellation: %v", err)
			}
			if err := s.Unlock(t.Context(), "lock-a"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Signal that a caller has started waiting on cancellation, without inspecting
// storage's local lock records or depending on a goroutine scheduling delay.
type waitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *waitingContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestCloseDoesNotAbandonLocks(t *testing.T) {
	t.Run("pending acquisition", func(t *testing.T) {
		s := newTestStorage(t)
		conn, err := s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
				t.Error(err)
			}
		}()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- s.Lock(ctx, "lock-a") }()
		waitFor(t, func() bool { return s.db.Stats().WaitCount > 0 })
		if err := s.Close(); !errors.Is(err, ErrLocksActive) {
			t.Fatalf("pending Close = %v", err)
		}
		cancel()
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("pending Lock = %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Lock(t.Context(), "lock-a"); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("Lock after Close = %v", err)
		}
	})
	t.Run("blocked release", func(t *testing.T) {
		s := newTestStorage(t)
		if err := s.Lock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
		conn, err := s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
				t.Error(err)
			}
		}()
		released := make(chan error, 1)
		go func() { released <- s.Unlock(t.Context(), "lock-a") }()
		waitFor(t, func() bool { return s.db.Stats().WaitCount > 0 })
		if err := s.Unlock(t.Context(), "lock-a"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("concurrent release must not claim the same acquisition: %v", err)
		}
		if err := s.Close(); !errors.Is(err, ErrLocksActive) {
			t.Fatalf("Close while deletion is blocked = %v", err)
		}
		expectLockBlocked(t, s, "lock-a", 100*time.Millisecond)
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-released; err != nil {
			t.Fatal(err)
		}
		if err := s.Lock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
		if err := s.Unlock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("same-name handoff races close", func(t *testing.T) {
		for range 30 {
			s := newTestStorage(t)
			if err := s.Lock(t.Context(), "lock-a"); err != nil {
				t.Fatal(err)
			}
			ctx := &waitingContext{Context: t.Context(), waiting: make(chan struct{})}
			locked := make(chan error, 1)
			go func() { locked <- s.Lock(ctx, "lock-a") }()
			<-ctx.waiting
			if err := s.Unlock(t.Context(), "lock-a"); err != nil {
				t.Fatal(err)
			}
			closeErr := s.Close()
			lockErr := <-locked
			if closeErr == nil {
				if !errors.Is(lockErr, fs.ErrClosed) {
					t.Fatalf("waiter after successful Close = %v", lockErr)
				}
			} else {
				if !errors.Is(closeErr, ErrLocksActive) || lockErr != nil {
					t.Fatalf("handoff Lock = %v, Close = %v", lockErr, closeErr)
				}
				if err := s.Unlock(t.Context(), "lock-a"); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("acquisition races close", func(t *testing.T) {
		for range 30 {
			s := newTestStorage(t)
			start := make(chan struct{})
			locked, closed := make(chan error, 1), make(chan error, 1)
			go func() { <-start; locked <- s.Lock(t.Context(), "lock-a") }()
			go func() { <-start; closed <- s.Close() }()
			close(start)
			lockErr, closeErr := <-locked, <-closed
			if lockErr == nil {
				if !errors.Is(closeErr, ErrLocksActive) {
					t.Fatalf("Close abandoned a successful Lock: %v", closeErr)
				}
				if err := s.Unlock(t.Context(), "lock-a"); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(lockErr, fs.ErrClosed) || closeErr != nil {
				t.Fatalf("Lock = %v, Close = %v", lockErr, closeErr)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestLockCrashRecovery(t *testing.T) {
	const ttl = 900 * time.Millisecond
	const ownerID = "same-label"
	if path := os.Getenv("CERTMAGIC_SQLITE_CHILD_DB"); path != "" {
		s := newTestStorageAt(t, path, WithLockTTL(ttl), WithOwnerID(ownerID))
		if err := s.Lock(t.Context(), "lock-a"); err != nil {
			t.Fatal(err)
		}
		initial := lockExpiry(t, s, "lock-a")
		waitFor(t, func() bool { return lockExpiry(t, s, "lock-a") > initial })
		fmt.Println("refreshed")
		<-t.Context().Done() // Parent kills us without Unlock or Close.
		return
	}
	path := filepath.Join(t.TempDir(), "certs.db")
	s := newTestStorageAt(t, path, WithLockTTL(ttl))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Re-execute only this test binary, with a fixed test selector.
	//nolint:gosec // The executable comes from os.Executable, not external input.
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestLockCrashRecovery$")
	cmd.Env = append(os.Environ(), "CERTMAGIC_SQLITE_CHILD_DB="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "refreshed\n" {
		t.Fatalf("child readiness = %q, %v", line, err)
	}
	expectLockBlocked(t, s, "lock-a", 100*time.Millisecond)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("expected killed child to exit unsuccessfully")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopening with the crashed holder's label grants no ownership of its lease.
	s = newTestStorageAt(t, path, WithLockTTL(ttl), WithOwnerID(ownerID))
	expiry := lockExpiry(t, s, "lock-a")
	if expiry <= time.Now().UnixMilli() {
		t.Fatal("child lease expired before crash recovery check")
	}
	if err := s.Unlock(ctx, "lock-a"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("restarted owner Unlock = %v, want no local acquisition", err)
	}
	expectLockBlocked(t, s, "lock-a", 100*time.Millisecond)
	if err := s.Lock(ctx, "lock-a"); err != nil {
		t.Fatal(err)
	}
	if time.Now().UnixMilli() < expiry {
		t.Fatal("recovered before the crashed holder's last expiry")
	}
	if err := s.Store(ctx, "lock-a/certificate", []byte("recovered")); err != nil {
		t.Fatal(err)
	}
	value, err := s.Load(ctx, "lock-a/certificate")
	if err != nil || string(value) != "recovered" {
		t.Fatalf("recovered storage = %q, %v", value, err)
	}
	if err := s.Unlock(ctx, "lock-a"); err != nil {
		t.Fatal(err)
	}
}

// This issuer signs CSR public keys locally and can pause one domain without blocking another.
type testIssuer struct {
	key     *ecdsa.PrivateKey
	issue   func(context.Context, string) error
	serial  atomic.Int64
	nearEnd bool // Set only before use: seed a still-valid certificate due for renewal.
}

func (*testIssuer) IssuerKey() string { return "local-test-issuer" }

func (iss *testIssuer) Issue(ctx context.Context, csr *x509.CertificateRequest) (*certmagic.IssuedCertificate, error) {
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("invalid CSR: %w", err)
	}
	if iss.issue != nil {
		if err := iss.issue(ctx, csr.DNSNames[0]); err != nil {
			return nil, err
		}
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(iss.serial.Add(1)), DNSNames: csr.DNSNames,
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if iss.nearEnd {
		template.NotAfter = now.Add(time.Minute)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, csr.PublicKey, iss.key)
	if err != nil {
		return nil, fmt.Errorf("sign test certificate: %w", err)
	}
	return &certmagic.IssuedCertificate{Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

func testCertMagic(t *testing.T, s *SQLiteStorage, issuer certmagic.Issuer) *certmagic.Config {
	t.Helper()
	var cfg *certmagic.Config
	cache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) { return cfg, nil },
		Logger:           zap.NewNop(),
	})
	t.Cleanup(cache.Stop)
	cfg = certmagic.New(cache, certmagic.Config{Storage: s, Issuers: []certmagic.Issuer{issuer}, Logger: zap.NewNop()})
	return cfg
}

func TestCertMagicReleaseFailureRecovery(t *testing.T) {
	s := newTestStorage(t, WithLockTTL(300*time.Millisecond))
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issuer := &testIssuer{key: key}
	cfg := testCertMagic(t, s, issuer)
	if _, err := s.db.ExecContext(t.Context(), `CREATE TRIGGER fail_release BEFORE DELETE ON certmagic_locks
		BEGIN SELECT RAISE(FAIL, 'temporary cleanup failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := cfg.ObtainCertSync(t.Context(), "example.com"); err != nil {
		t.Fatal(err)
	}
	// CertMagic logs the failed deferred release, but does not retry it.
	var count int
	if err := s.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM certmagic_locks").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("failed release left %d rows, want 1", count)
	}
	if _, err := s.db.ExecContext(t.Context(), "DROP TRIGGER fail_release"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := cfg.RenewCertSync(ctx, "example.com", true); err != nil {
		t.Fatalf("certificate cannot renew after cleanup recovers: %v", err)
	}
	if calls := issuer.serial.Load(); calls != 2 {
		t.Fatalf("issuer calls = %d, want obtain and forced renewal", calls)
	}
}

func TestCertMagicObtainAndRenew(t *testing.T) {
	for _, renewal := range []bool{false, true} {
		t.Run(fmt.Sprintf("renewal=%t", renewal), func(t *testing.T) {
			const ttl = 600 * time.Millisecond
			const domain, otherDomain = "example.com", "other.example.com"
			path := filepath.Join(t.TempDir(), "certs.db")
			a := newTestStorageAt(t, path, WithLockTTL(ttl))
			b := newTestStorageAt(t, path, WithLockTTL(ttl))
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			issuer := &testIssuer{key: key}
			cfgA, cfgB := testCertMagic(t, a, issuer), testCertMagic(t, b, issuer)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if renewal {
				seed := testCertMagic(t, a, &testIssuer{key: key, nearEnd: true})
				if err := seed.ObtainCertSync(ctx, domain); err != nil {
					t.Fatal(err)
				}
			}
			entered, release := make(chan struct{}, 2), make(chan struct{})
			var domainCalls atomic.Int64
			issuer.issue = func(ctx context.Context, name string) error {
				if name == domain {
					domainCalls.Add(1)
					entered <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}
			issue := func(cfg *certmagic.Config) error {
				if renewal {
					return cfg.RenewCertSync(ctx, domain, false)
				}
				return cfg.ObtainCertSync(ctx, domain)
			}
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { first <- issue(cfgA) }()
			select {
			case <-entered:
			case err := <-first:
				t.Fatalf("first task returned before entering issuer: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			go func() { second <- issue(cfgB) }()
			if err := cfgB.ObtainCertSync(ctx, otherDomain); err != nil {
				t.Fatalf("independent domain blocked behind active domain: %v", err)
			}
			select {
			case err := <-second:
				t.Fatalf("second task completed while first was active: %v", err)
			case <-entered:
				t.Fatal("second task entered issuer while first was active")
			case <-time.After(3 * ttl):
			}
			close(release)
			for _, result := range []chan error{first, second} {
				if err := <-result; err != nil {
					t.Fatal(err)
				}
			}
			if calls := domainCalls.Load(); calls != 1 {
				t.Fatalf("issuer calls for shared domain = %d, want 1", calls)
			}
			for _, domain := range []string{domain, otherDomain} {
				certPEM, err := b.Load(ctx, certmagic.StorageKeys.SiteCert(issuer.IssuerKey(), domain))
				if err != nil {
					t.Fatal(err)
				}
				keyPEM, err := b.Load(ctx, certmagic.StorageKeys.SitePrivateKey(issuer.IssuerKey(), domain))
				if err != nil {
					t.Fatal(err)
				}
				pair, err := tls.X509KeyPair(certPEM, keyPEM)
				if err != nil {
					t.Fatalf("%s certificate/key mismatch: %v", domain, err)
				}
				if err := pair.Leaf.VerifyHostname(domain); err != nil {
					t.Fatal(err)
				}
				if time.Until(pair.Leaf.NotAfter) < 23*time.Hour {
					t.Fatalf("%s did not get a fresh certificate", domain)
				}
			}
		})
	}
}
