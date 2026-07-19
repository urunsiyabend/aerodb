<p align="center">
  <img src="images/logo.png" width="200" height="200" alt="AeroDB logo">
</p>

<h1 align="center">AeroDB</h1>

<p align="center">
  An experimental embedded relational database engine written in Rust.
</p>

AeroDB combines a page-based B-Tree storage engine with a compact SQL layer,
constraints, secondary indexes, write-ahead logging, and multi-version
concurrency control (MVCC). It can be used through the interactive CLI, the
single-session `Engine` API, or the thread-safe, multi-session `Database` API.

> [!IMPORTANT]
> AeroDB is under active development and is not production-ready. SQL coverage
> is intentionally limited, some metadata is still process-local, and the
> on-disk format may change between versions.

## Highlights

- 4 KiB page-based B-Tree storage
- Persistent table and sequence catalogs
- SQL DDL, DML, joins, aggregates, aliases, and nested queries
- Primary key, `NOT NULL`, default-value, and foreign-key constraints
- Single-column secondary indexes for equality lookups
- Automatic and explicit transactions
- Snapshot isolation with MVCC row versions
- Multiple live transactions through a thread-safe database handle
- First-committer-wins write/write conflict detection
- WAL-based recovery and a durable commit-status log
- Logical rollback and explicit MVCC vacuum
- A library API and an interactive command-line interface

The current package version is `0.5.0`.

## Quick start

### Requirements

- Git
- A Rust toolchain with Rust 2024 edition support (Rust 1.85 or newer)

Clone and run AeroDB:

```bash
git clone https://github.com/urunsiyabend/aerodb.git
cd aerodb
cargo run --release
```

The CLI opens `data.aerodb` in the current directory and displays the
`aerodb>` prompt. Enter one statement per line. For maximum parser
compatibility, omit trailing semicolons.

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN DEFAULT true)
INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', DEFAULT)
SELECT id, name FROM users WHERE active = true
UPDATE users SET name = 'Bobby' WHERE id = 2
DELETE FROM users WHERE id = 1
```

Use `.exit` or `exit` to close the CLI.

You can also install the binary from the local checkout:

```bash
cargo install --path .
aerodb
```

## Database files

Opening `data.aerodb` creates and manages these files:

| File | Purpose |
| --- | --- |
| `data.aerodb` | Main database pages, table catalog, sequence catalog, and durable transaction counters |
| `data.aerodb.wal` | Write-ahead log used for recovery |
| `data.aerodb.clog` | Durable committed/aborted transaction status log used by MVCC |

Keep the three files together when moving or backing up a database. The CLI
database path is currently fixed to `data.aerodb`; the Rust APIs accept any
path.

## SQL support

AeroDB implements a focused SQL subset. The examples below reflect behavior
covered by the current test suite.

### Schema definition

```sql
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL,
    balance DOUBLE(12, 2) UNSIGNED DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)

CREATE INDEX idx_accounts_email ON accounts(email)
DROP INDEX idx_accounts_email
DROP TABLE IF EXISTS accounts

CREATE SEQUENCE invoice_ids START WITH 1000 INCREMENT BY 5
```

Supported column features:

- Inline and table-level primary keys, including composite primary keys
- `NULL` and `NOT NULL`
- Literal defaults and the `DEFAULT` keyword in inserts
- `AUTO_INCREMENT` on a single `NOT NULL` integer column per table
- Foreign keys with `NO ACTION` or `ON DELETE CASCADE`
- `CURRENT_TIMESTAMP`, `CURRENT_TIMESTAMP()`, `GETDATE()`, and
  `GETUTCDATE()` defaults

### Data manipulation

```sql
INSERT INTO accounts (id, email, balance)
VALUES
    (1, 'alice@example.com', 125.50),
    (2, 'bob@example.com', 80.00)

UPDATE accounts
SET balance = 140.00
WHERE id = 1

DELETE FROM accounts WHERE id = 2
```

`INSERT` supports column lists, multiple value tuples, omitted nullable/default
columns, and explicit `DEFAULT` values. In autocommit mode, a failed multi-row
insert rolls back the entire statement.

The first column of every stored table must currently produce an `INTEGER`
value because AeroDB uses it as the physical B-Tree row key.

### Queries

```sql
SELECT * FROM accounts

SELECT id, email AS address
FROM accounts
WHERE balance >= 100 AND id <> 5

SELECT department, COUNT(*) AS employee_count, AVG(salary) AS average_salary
FROM employees
WHERE active = true
GROUP BY department
HAVING COUNT(*) >= 2
```

Supported query features include:

- `*`, column projections, literals, arithmetic expressions, and aliases
- `SELECT` expressions without `FROM`, such as `SELECT 2 + 3, 'hello'`
- `WHERE` with `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `AND`, and
  `OR`
- Arithmetic and bitwise operators: `+`, `-`, `*`, `/`, `%`, `&`, `|`, and `^`
- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`
- `GROUP BY` and `HAVING`, with grouped-column validation
- Single-column `ORDER BY`, plus `LIMIT` and `OFFSET` parsing
- Quoted string literals and quoted identifiers

The parser recognizes `ORDER BY`, `LIMIT`, and `OFFSET`, but the main execution
path does not yet apply them consistently to every query shape.

### Joins and subqueries

```sql
SELECT u.name, o.total
FROM users AS u
JOIN orders AS o ON u.id = o.user_id

SELECT u.name, o.total
FROM users u
LEFT JOIN orders o ON u.id = o.user_id

SELECT id
FROM users
WHERE id IN (SELECT user_id FROM orders)

SELECT name
FROM users
WHERE EXISTS (
    SELECT 1 FROM orders WHERE orders.user_id = users.id
)

SELECT *
FROM (SELECT id, name FROM users) AS selected_users
```

AeroDB supports `INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS` joins, table
aliases, multiple joins, `IN` subqueries, correlated `EXISTS`, scalar
subqueries, and subqueries in `FROM`.

### Data types

| Type | Notes |
| --- | --- |
| `INTEGER` / `INT` | Signed 32-bit integer |
| `SMALLINT[(width)] [UNSIGNED]` | Signed or unsigned range validation |
| `MEDIUMINT[(width)] [UNSIGNED]` | Signed or unsigned range validation |
| `DOUBLE[(precision, scale)] [UNSIGNED]` | Stored as a 64-bit floating-point value |
| `TEXT` | Variable-length UTF-8 text |
| `CHAR[(length)]` | Fixed-length, space-padded text |
| `BOOLEAN` / `BOOL` | `true` or `false` |
| `DATE` | `YYYY-MM-DD` |
| `DATETIME` | `YYYY-MM-DD HH:MM:SS` |
| `TIMESTAMP` | `YYYY-MM-DD HH:MM:SS` |
| `TIME` | `[-]HH:MM:SS`, up to 838 hours |
| `YEAR` | `0000` or `1901` through `2155` |

## Transactions and MVCC

Mutating statements execute in automatic transactions unless an explicit
transaction is active:

```sql
BEGIN
INSERT INTO accounts VALUES (3, 'carol@example.com', 50, DEFAULT)
UPDATE accounts SET balance = 75 WHERE id = 3
COMMIT
```

Use `ROLLBACK` to abort the active transaction:

```sql
BEGIN TRANSACTION balance_change
UPDATE accounts SET balance = 0 WHERE id = 3
ROLLBACK
```

The current isolation level is snapshot isolation:

- A transaction reads from the snapshot captured at `BEGIN`.
- It sees its own writes.
- It does not see another transaction's uncommitted work.
- A transaction keeps a stable view when another transaction commits.
- Concurrent writes to the same logical key use first-committer-wins conflict
  resolution.
- Rolled-back versions remain physically present but invisible until vacuum
  reclaims them.

Transaction IDs and final transaction states survive database reopen. WAL
recovery treats transactions left active by a crash as aborted.

## Rust API

### Concurrent, multi-session API

`Database` is `Clone`, `Send`, and `Sync`. Clones share one catalog, pager, and
transaction manager.

```rust
use aerodb::{
    db::Database,
    error::DbError,
    sql::{ast::Statement, parser::parse_statement},
};

fn sql(input: &str) -> Statement {
    parse_statement(input).expect("valid SQL")
}

fn main() -> Result<(), DbError> {
    let db = Database::open("app.aerodb")?;

    db.autocommit(sql(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    ))?;

    let tx = db.begin()?;
    db.execute(&tx, sql("INSERT INTO messages VALUES (1, 'hello')"))?;
    db.commit(tx)?;

    let reader = db.begin()?;
    let rows = db.query_all(&reader, "messages")?;
    db.abort(reader)?;

    println!("{} visible row(s)", rows.len());
    Ok(())
}
```

The main methods are:

| Method | Purpose |
| --- | --- |
| `Database::open(path)` | Open or create a shared database |
| `begin()` | Start a snapshot-isolated transaction |
| `execute(&tx, statement)` | Run a DDL or DML statement in a transaction |
| `query_all(&tx, table)` | Read all rows visible to the transaction |
| `commit(tx)` | Commit, including conflict and constraint validation |
| `abort(tx)` | Logically abort a transaction |
| `autocommit(statement)` | Run one statement in its own transaction |
| `vacuum_table(table)` | Reclaim obsolete MVCC row versions |

Physical pager access is currently protected by one shared mutex. This provides
thread-safe, correct multi-session behavior, but operations do not yet execute
in parallel at the page level.

### Single-session API

`Engine` is the simpler, owner-oriented API that mirrors the CLI execution
flow:

```rust
use aerodb::{engine::Engine, sql::parser::parse_statement};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut engine = Engine::new("app.aerodb");

    engine.execute(parse_statement("BEGIN")?)?;
    engine.execute(parse_statement(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)",
    )?)?;
    engine.execute(parse_statement("COMMIT")?)?;

    let report = engine.vacuum_table("events")?;
    println!("removed {} version(s)", report.versions_removed);
    Ok(())
}
```

`Engine::execute` handles automatic transactions and intercepts `BEGIN`,
`COMMIT`, and `ROLLBACK`. Call `Engine::vacuum_table` directly because there is
not yet a SQL `VACUUM` statement.

## Storage architecture

AeroDB separates logical isolation from physical storage:

1. The pager reads and writes fixed-size 4 KiB pages and keeps a simple
   in-memory page cache.
2. Tables and secondary indexes are B-Trees; linked leaf pages support scans.
3. Row payloads carry creator/deleter transaction IDs and version links.
4. Snapshot visibility selects the correct row version for each transaction.
5. The WAL records page images and transaction state for crash recovery.
6. The commit-status log preserves transaction outcomes after WAL truncation.
7. Vacuum removes versions created by aborted transactions and versions deleted
   below the oldest live snapshot boundary, then rebuilds affected indexes.

Reserved main-file pages:

| Page | Contents |
| --- | --- |
| `0` | Engine metadata and durable transaction counters |
| `1` | Table catalog |
| `2` | Sequence catalog |
| `3+` | Table and index B-Tree pages |

For the implemented concurrency design and its invariants, see
[`specs/mvcc-concurrent-transactions.md`](specs/mvcc-concurrent-transactions.md).

## Current limitations

- AeroDB implements a SQL subset, not a complete SQL standard.
- The CLI uses the fixed `data.aerodb` path and accepts one statement per line.
- Snapshot isolation is the only isolation level; serializable isolation and
  `SET TRANSACTION ISOLATION LEVEL` are not implemented.
- Shared storage is process-local. Multiple `Database` transactions can run in
  one process, but cross-process coordination is not supported.
- Physical storage operations are serialized by a coarse mutex rather than
  per-page latches.
- Secondary-index metadata is currently kept in memory and is not rebuilt when
  the database is reopened; recreate indexes after opening a new process.
- Secondary indexes currently optimize equality predicates only.
- Standalone sequences are available through SQL creation and the catalog API,
  but there is no SQL `NEXTVAL` expression yet.
- `ORDER BY`, `LIMIT`, and `OFFSET` are not consistently applied across every
  execution path.
- Vacuum is explicit; there is no SQL command or background auto-vacuum.
- The first table column must be integer-compatible because it supplies the
  physical row key.

## Project layout

```text
src/
├── catalog/       Persistent table/sequence metadata and index management
├── constraints/   Primary key, foreign key, NOT NULL, and default constraints
├── execution/     Statement execution, joins, grouping, and query output
├── planner/       Query-planning structures
├── sql/           AST, tokenizer/parser, expressions, and system functions
├── storage/       Pager, pages, rows, B-Trees, and vacuum
├── transaction/   MVCC, snapshots, WAL, clog, sessions, and transaction manager
├── db.rs          Concurrent multi-session Database facade
├── engine/        Single-session Engine facade
├── error.rs       Public database errors
├── lib.rs         Library exports
└── main.rs        Interactive CLI

tests/             Integration and concurrency tests
specs/             Design specifications
images/            Documentation assets
```

## Development

Build and test the project with Cargo:

```bash
cargo build
cargo test
cargo fmt --check
cargo clippy --all-targets
```

The full test suite covers SQL parsing/execution, constraints, types, joins,
nested queries, auto-commit behavior, durable transaction state, MVCC visibility,
rollback, vacuum, real concurrent transactions, and multi-threaded conflict
stress.

When contributing:

1. Add or update tests for the behavior being changed.
2. Keep existing tests passing.
3. Run `cargo fmt`.
4. Run the full test suite.
5. Explain user-visible or on-disk behavior changes in the pull request.

Issues and pull requests are welcome at
[github.com/urunsiyabend/aerodb](https://github.com/urunsiyabend/aerodb).

## Roadmap

- Persist and reload secondary-index metadata
- Replace the coarse storage mutex with finer-grained page latching
- Make ordering and pagination consistent across all query paths
- Add SQL-level vacuum and automatic maintenance
- Expand SQL coverage and improve query planning
- Stabilize and document the on-disk format
- Add storage-engine and B-Tree implementation documentation

## License

AeroDB is available under the [MIT License](LICENSE).
