# certmagic-sqlite

A SQLite storage backend for [CertMagic](https://github.com/caddyserver/certmagic). Pure Go, no CGO required.

## Features

- Implements `certmagic.Storage` and `certmagic.Locker` interfaces
- Pure Go SQLite driver ([modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)) — no CGO
- Supports shared database connections for applications already using SQLite
- Automatic schema migrations
- Automatically refreshed locks with expiration-based crash recovery

## Installation

```bash
go get github.com/rsclarke/certmagic-sqlite
```

## Usage

### Standalone Database

Use `New()` when CertMagic should manage its own database file:

```go
package main

import (
    "github.com/caddyserver/certmagic"
    "github.com/rsclarke/certmagic-sqlite"
)

func main() {
    storage, err := certmagicsqlite.New("/path/to/certs.db")
    if err != nil {
        log.Fatal(err)
    }
    defer storage.Close()

    certmagic.Default.Storage = storage

    // Use certmagic...
}
```

`New()` automatically configures:
- WAL journal mode (for better concurrency)
- `synchronous=NORMAL` (safe balance of durability and performance)
- `busy_timeout=5000` (5 second wait on lock contention)

### Shared Database

Use `NewWithDB()` when you want to store certificates in an existing application database:

```go
package main

import (
    "database/sql"

    "github.com/caddyserver/certmagic"
    "github.com/rsclarke/certmagic-sqlite"
    _ "modernc.org/sqlite"
)

func main() {
    // Open your application database
    db, err := sql.Open("sqlite", "/path/to/app.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Configure recommended PRAGMAs (see below)
    db.Exec("PRAGMA journal_mode=WAL")
    db.Exec("PRAGMA busy_timeout=5000")

    // Create your application tables
    db.Exec("CREATE TABLE IF NOT EXISTS users (...)")

    // Create CertMagic storage (runs schema migrations automatically)
    storage, err := certmagicsqlite.NewWithDB(db)
    if err != nil {
        log.Fatal(err)
    }
    defer storage.Close() // Ends the storage lifecycle; doesn't close the shared db

    certmagic.Default.Storage = storage
}
```

### File Permissions

The database and WAL can contain certificate and account private keys. On Unix, use a service-owned containing directory with mode `0700` and a database with mode `0600`. SQLite creates the `-wal` and `-shm` files itself, normally using the database's permissions. Secure the directory before opening the database, and keep parent directories protected from untrusted writes; use equivalent ACL restrictions on other platforms.

Neither constructor enforces these permissions. Set umask `077` before starting the application to restrict newly created files. This does not tighten existing files: stop all database users before correcting their ownership and permissions. These requirements also apply to the entire shared database when using `NewWithDB()`.

## Configuration Options

Both constructors accept functional options:

```go
storage, err := certmagicsqlite.New("/path/to/certs.db",
    certmagicsqlite.WithLockTTL(5*time.Minute),       // Time allowed since last refresh (default: 2 minutes)
    certmagicsqlite.WithRefreshInterval(time.Minute), // Refresh interval (default: TTL / 3)
    certmagicsqlite.WithQueryTimeout(10*time.Second), // Query timeout (default: 3 seconds)
)
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithLockTTL(d)` | 2 minutes | Lease duration after each refresh; must be at least 1 ms |
| `WithRefreshInterval(d)` | TTL / 3 | Time between lock refresh attempts; must be positive and below TTL |
| `WithQueryTimeout(d)` | 3 seconds | Timeout for individual database queries; must be positive |
| `WithOwnerID(id)` | Random UUID | Instance label for identifying lock records |

### Locking

CertMagic handles locking automatically to coordinate certificate work between processes sharing the same SQLite database. The driver refreshes active locks and lets abandoned locks expire after a crash. `WithLockTTL` controls how long a lock remains valid without a successful refresh; allow enough time for scheduling and database delays. A pause or outage longer than this can allow overlapping certificate work.

Lock owners get random UUID labels by default. Set `WithOwnerID` to a hostname or application instance name for easier identification in database records. This is only a label, not a way to reclaim locks after a restart.

When upgrading from a version without automatic lock refresh, stop all processes sharing the database before starting the new version.

## Recommended SQLite Settings for `NewWithDB`

When using `NewWithDB()`, you are responsible for configuring the database connection. Here are the recommended settings:

### Journal Mode: WAL

```go
db.Exec("PRAGMA journal_mode=WAL")
```

WAL (Write-Ahead Logging) provides:
- Better concurrency (readers don't block writers)
- Improved performance for mixed read/write workloads
- Crash resilience

**Note:** WAL is not supported for `:memory:` databases.

### Busy Timeout

```go
db.Exec("PRAGMA busy_timeout=5000")  // 5000ms = 5 seconds
```

Configures how long SQLite waits when the database is locked by another connection. Without this, you may see "database is locked" errors under contention.

### Synchronous Mode

```go
db.Exec("PRAGMA synchronous=NORMAL")
```

Options:
- `FULL` (default): Safest, slowest — syncs after every transaction
- `NORMAL`: Safe with WAL — syncs less frequently, good performance
- `OFF`: Fastest, but risks corruption on power loss

`NORMAL` is recommended when using WAL mode.

### Connection Pool Settings

```go
db.SetMaxOpenConns(1)  // Serialize all writes
db.SetMaxIdleConns(1)  // Keep connection warm
```

SQLite only supports one writer at a time. Setting `MaxOpenConns(1)` prevents "database is locked" errors by serializing access at the Go level.

### Complete Example

```go
db, _ := sql.Open("sqlite", "/path/to/app.db")

// Recommended settings
db.SetMaxOpenConns(1)
db.SetMaxIdleConns(1)
db.Exec("PRAGMA journal_mode=WAL")
db.Exec("PRAGMA synchronous=NORMAL")
db.Exec("PRAGMA busy_timeout=5000")

storage, _ := certmagicsqlite.NewWithDB(db)
```

## Schema

The following tables are created automatically:

```sql
CREATE TABLE IF NOT EXISTS certmagic_data (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL,
    modified INTEGER NOT NULL  -- Unix timestamp (milliseconds)
);

CREATE TABLE IF NOT EXISTS certmagic_locks (
    name TEXT PRIMARY KEY,
    expires_at INTEGER NOT NULL,  -- Unix timestamp (milliseconds)
    owner_id TEXT NOT NULL
);
```

These tables coexist safely with your application tables.

## When to Use SQLite vs Other Backends

**SQLite is ideal for:**
- Single-node deployments
- Embedded applications or appliances
- Simplicity (no external database server)
- Applications already using SQLite

**Consider PostgreSQL/Redis/Consul for:**
- Multi-node clusters sharing certificates
- High-availability deployments
- Distributed locking across multiple servers

SQLite storage provides the same single-node guarantees as the default filesystem storage, with added benefits of atomic transactions and single-file backup.

## Development

Tool versions are pinned in `mise.toml`. Install [mise](https://mise.jdx.dev/),
configure its [shell activation](https://mise.jdx.dev/shell-setup.html), then run
`mise install --locked` from the repository root. When updating tool versions,
regenerate `mise.lock` with `mise lock` and commit both files.

- `task ci` — lint and test
- `task test` — run tests with the Go race detector (requires a supported CGO-enabled toolchain; the SQLite driver itself remains pure Go)
- `task zizmor` — audit GitHub Actions
