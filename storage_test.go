package certmagicsqlite

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newTestStorage(t *testing.T) *SQLiteStorage {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
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

func TestLockUnlock(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	if err := s.Lock(ctx, "testlock"); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if err := s.Unlock(ctx, "testlock"); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestUnlockWithoutLock(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	err := s.Unlock(ctx, "never-locked")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Unlock without Lock returned %v, want fs.ErrNotExist", err)
	}
}

func TestLockContention(t *testing.T) {
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
	s.Unlock(ctx, lockName)
}

func TestLockExpiration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := New(dbPath, WithLockTTL(100*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	lockName := "expiring-lock"

	if err := s.Lock(ctx, lockName); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := s.Lock(ctx2, lockName); err != nil {
		t.Fatalf("Lock after expiration failed: %v", err)
	}
	s.Unlock(ctx, lockName)
}

func TestConcurrentStoreLoad(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	const goroutines = 10
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
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
		}(i)
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

func TestLockOwnership(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	s1, err := New(dbPath, WithLockTTL(100*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to create storage 1: %v", err)
	}
	defer s1.Close()

	s2, err := New(dbPath, WithLockTTL(100*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to create storage 2: %v", err)
	}
	defer s2.Close()

	ctx := context.Background()
	lockName := "ownership-lock"

	if err := s1.Lock(ctx, lockName); err != nil {
		t.Fatalf("s1 Lock failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	if err := s2.Lock(ctx, lockName); err != nil {
		t.Fatalf("s2 Lock after expiration failed: %v", err)
	}

	// s1's unlock should fail because s2 now owns the lock
	err = s1.Unlock(ctx, lockName)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("s1 Unlock should return fs.ErrNotExist (lock stolen), got: %v", err)
	}

	// s1 should not be able to acquire lock held by s2
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err = s1.Lock(ctx2, lockName)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("s1 should not be able to acquire lock held by s2, got: %v", err)
	}

	// s2 can unlock its own lock
	if err := s2.Unlock(ctx, lockName); err != nil {
		t.Fatalf("s2 Unlock failed: %v", err)
	}
}

func TestMemoryDB(t *testing.T) {
	s, err := New(":memory:")
	if err != nil {
		t.Fatalf("failed to create in-memory storage: %v", err)
	}
	defer s.Close()

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
	dbPath := filepath.Join(t.TempDir(), "shared.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS app_data (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create app table: %v", err)
	}

	s, err := NewWithDB(db)
	if err != nil {
		t.Fatalf("NewWithDB failed: %v", err)
	}

	ctx := context.Background()
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

	_, err = db.Exec("INSERT INTO app_data (value) VALUES (?)", "app value")
	if err != nil {
		t.Fatalf("failed to insert app data: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM app_data").Scan(&count)
	if err != nil {
		t.Fatalf("DB should still be usable after storage.Close(): %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row in app_data, got %d", count)
	}
}

func TestMillisecondTTL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := New(dbPath, WithLockTTL(50*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	lockName := "ms-ttl-lock"

	if err := s.Lock(ctx, lockName); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	ctx2, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := s.Lock(ctx2, lockName); err != nil {
		t.Fatalf("Lock after 50ms TTL expiration failed: %v", err)
	}
	s.Unlock(ctx, lockName)
}

func TestWithOwnerID(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()
	lockName := "owner-test-lock"

	s1, err := New(dbPath, WithOwnerID("instance-1"))
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	if err := s1.Lock(ctx, lockName); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	s1.Close()

	s2, err := New(dbPath, WithOwnerID("instance-1"))
	if err != nil {
		t.Fatalf("failed to recreate storage: %v", err)
	}
	defer s2.Close()

	if err := s2.Unlock(ctx, lockName); err != nil {
		t.Fatalf("Unlock with same ownerID should succeed: %v", err)
	}

	if err := s2.Lock(ctx, lockName); err != nil {
		t.Fatalf("Lock after unlock should succeed: %v", err)
	}
	s2.Unlock(ctx, lockName)
}

func TestWithOwnerIDDifferentOwners(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()
	lockName := "different-owner-lock"

	s1, err := New(dbPath, WithOwnerID("instance-1"), WithLockTTL(time.Minute))
	if err != nil {
		t.Fatalf("failed to create storage 1: %v", err)
	}
	defer s1.Close()

	s2, err := New(dbPath, WithOwnerID("instance-2"), WithLockTTL(time.Minute))
	if err != nil {
		t.Fatalf("failed to create storage 2: %v", err)
	}
	defer s2.Close()

	if err := s1.Lock(ctx, lockName); err != nil {
		t.Fatalf("s1 Lock failed: %v", err)
	}

	err = s2.Unlock(ctx, lockName)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("s2 Unlock should return fs.ErrNotExist, got: %v", err)
	}

	ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err = s2.Lock(ctx2, lockName)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("s2 Lock should timeout, got: %v", err)
	}

	s1.Unlock(ctx, lockName)
}

func TestStableOwnerIDAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()
	ownerID := "my-stable-app-id"

	s1, err := New(dbPath, WithOwnerID(ownerID))
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	if err := s1.Lock(ctx, "lock1"); err != nil {
		t.Fatalf("Lock 1 failed: %v", err)
	}
	if err := s1.Lock(ctx, "lock2"); err != nil {
		t.Fatalf("Lock 2 failed: %v", err)
	}
	s1.Close()

	s2, err := New(dbPath, WithOwnerID(ownerID))
	if err != nil {
		t.Fatalf("failed to recreate storage: %v", err)
	}
	defer s2.Close()

	if err := s2.Unlock(ctx, "lock1"); err != nil {
		t.Fatalf("Unlock lock1 after restart failed: %v", err)
	}
	if err := s2.Unlock(ctx, "lock2"); err != nil {
		t.Fatalf("Unlock lock2 after restart failed: %v", err)
	}

	if err := s2.Lock(ctx, "lock1"); err != nil {
		t.Fatalf("Reacquire lock1 failed: %v", err)
	}
	if err := s2.Lock(ctx, "lock2"); err != nil {
		t.Fatalf("Reacquire lock2 failed: %v", err)
	}
}
