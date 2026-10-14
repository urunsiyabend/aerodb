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
> on-disk format may change between versions. AeroDB is not certified against
> any SQL standard and makes no overall conformance claim. The per-feature
> audit evidence is in
> [`specs/sql-conformance-0.6.0.md`](specs/sql-conformance-0.6.0.md).

## Highlights

- 4 KiB page-based B-Tree storage
- Persistent table, view and sequence catalogs
- A fail-closed SQL parser: unsupported syntax is rejected, never ignored
- Typed values, three-valued logic, and exact `DECIMAL`/`NUMERIC`
- SQL DDL, DML, joins, aggregates, set operations, views, and nested queries
- `PRIMARY KEY` (composite, text, any column), `UNIQUE`, `CHECK`, `NOT NULL`,
  default-value, and foreign-key constraints
- A read-only `information_schema` subset
- SQLSTATE codes for every error
- Single-column secondary indexes for equality lookups
- Automatic and explicit transactions
- Snapshot isolation with MVCC row versions
- Multiple live transactions through a thread-safe database handle
- First-committer-wins write/write conflict detection
- WAL-based recovery and a durable commit-status log
- Logical rollback and explicit MVCC vacuum
- A library API and an interactive command-line interface

The current package version is `0.6.0`. See [`CHANGELOG.md`](CHANGELOG.md).

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
`aerodb>` prompt. Enter one statement per line; a single trailing semicolon is
optional.

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN DEFAULT true)
INSERT INTO users VALUES (1, 'Alice', true), (2, 'Bob', DEFAULT)
SELECT id, name FROM users WHERE active = true
UPDATE users SET name = 'Bobby' WHERE id = 2
DELETE FROM users WHERE id = 1
```

Use `.exit` or `exit` to close the CLI. Errors, including syntax errors, are
printed with their SQLSTATE, for example `Error: no such table: t (42P01)`.

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
covered by the current test suite. The parser is fail-closed:
- A statement must consume its whole input.
- Unknown syntax is a syntax error (SQLSTATE 42601).
- Unknown functions are 42883.
- Recognized but unimplemented features are rejected with 0A000 instead of
  being ignored.

Identifiers follow the standard rule: unquoted names match
case-insensitively, `"quoted"` names match exactly, and an ambiguous reference
is 42702. The same rule applies to the names of tables, views, indexes and
sequences:
- `CREATE TABLE`/`VIEW`/`INDEX`/`SEQUENCE` with an unquoted name that matches
  an existing one case-insensitively is 42P07 (`CREATE TABLE O` next to `o`).
  A quoted name collides only with the exact same name.
- `DROP TABLE`, `DROP VIEW` and `DROP INDEX` find the object like every other
  reference (`DROP VIEW vx` drops `Vx`).
- An unquoted name that matches several objects (created with quoted names
  differing only in case) is 42702; quote it to choose one.

Comments (`-- …` and `/* … */`) are accepted. A bareword is always an
identifier, never a string literal.

### Schema definition

```sql
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    email VARCHAR(200) NOT NULL UNIQUE,
    balance DECIMAL(12, 2) DEFAULT 0 CHECK (balance >= 0),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE orders (
    account_id INTEGER,
    order_no INTEGER,
    total DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (account_id, order_no),
    CONSTRAINT orders_account_fk FOREIGN KEY (account_id)
        REFERENCES accounts(id) ON DELETE CASCADE ON UPDATE CASCADE
)

ALTER TABLE accounts ADD COLUMN nickname TEXT DEFAULT 'none'
CREATE VIEW big_orders AS SELECT account_id, total FROM orders WHERE total > 100
DROP VIEW big_orders

CREATE INDEX idx_accounts_email ON accounts(email)
DROP INDEX idx_accounts_email
DROP TABLE IF EXISTS orders RESTRICT

CREATE SEQUENCE invoice_ids START WITH 1000 INCREMENT BY 5
```

Supported schema features:

- `PRIMARY KEY` on any column or column set (composite, text, not necessarily
  the first column). Rows have a logical row identity, so the first column
  does not have to be an `INTEGER` storage key.
- `NULL`/`NOT NULL`, literal and expression defaults, and the `DEFAULT`
  keyword in `INSERT` and `UPDATE`.
- `UNIQUE` and `CHECK` at column or table level, optionally named with
  `CONSTRAINT name`. NULLs never collide under `UNIQUE`, and a `CHECK` fails
  only when it evaluates to FALSE.
- `FOREIGN KEY … REFERENCES parent(cols)`:
  - The referenced columns must be the parent's PRIMARY KEY or a UNIQUE
    column set (42830).
  - Composite keys use `MATCH SIMPLE`: a row with any NULL key column is not
    checked.
  - Actions are `NO ACTION` (default, checked at the end of the statement),
    `RESTRICT` (checked immediately), `ON DELETE CASCADE` and
    `ON UPDATE CASCADE`.
  - Foreign keys are rechecked at COMMIT.
- `AUTO_INCREMENT` on a single `NOT NULL` integer column per table. Its
  sequence is internal to the table and is dropped with it.
- `CURRENT_TIMESTAMP`, `CURRENT_TIMESTAMP()`, `GETDATE()`, and `GETUTCDATE()`
  defaults.
- `ALTER TABLE t ADD [COLUMN]` with `DEFAULT`, `NOT NULL`, `UNIQUE`, and
  `CHECK`, validated against existing rows. Other `ALTER` forms are 0A000.
- Views:
  - `CREATE VIEW v [(columns)] AS query` and `DROP VIEW [IF EXISTS] v
    [RESTRICT]`.
  - Views persist across reopen and can be joined, filtered, aggregated, and
    nested.
  - `*` is expanded when the view is created.
  - Views are read-only.
- `DROP TABLE [IF EXISTS] t [RESTRICT]` fails with 2BP01 while a foreign key
  or a view depends on `t`. `CASCADE` is 0A000.
- Single-column secondary indexes, plus standalone sequences.

### Data manipulation

```sql
INSERT INTO accounts (id, email, balance)
VALUES
    (1, 'alice@example.com', 125.50),
    (2, 'bob@example.com', 80.00)

UPDATE accounts
SET balance = balance + 14.50, nickname = DEFAULT
WHERE id = 1

DELETE FROM accounts WHERE id = 2
DELETE FROM accounts
```

`INSERT` supports column lists, multiple value tuples, omitted nullable or
default columns, and explicit `DEFAULT` values. `UPDATE` assignments are
expressions evaluated per row, and `DELETE` without `WHERE` deletes every
visible row. Each statement is atomic: if any row fails a constraint, the
statement has no effect.

Assignment rules:
- A character literal converts to the column type when the statement is bound
  (an invalid literal is 22xxx).
- Numeric values go into numeric columns, and integers into `YEAR`.
- Any value may go into `CHAR`/`VARCHAR`/`TEXT`. This follows PostgreSQL and
  is more permissive than the SQL standard.
- Other cross-category assignments are 42804.
- `DECIMAL` values are rounded half away from zero to the column scale;
  overflow is 22003.
- Too-long `CHAR`/`VARCHAR` values are 22001.

### Queries

```sql
SELECT a.id, a.email AS address
FROM accounts AS a
WHERE a.balance >= 100 AND a.email LIKE '%@example.com' AND a.nickname IS NOT NULL
ORDER BY a.balance DESC NULLS LAST, 2
LIMIT 10 OFFSET 0

SELECT account_id, COUNT(*) AS n, SUM(total), AVG(DISTINCT total)
FROM orders
GROUP BY account_id
HAVING COUNT(*) >= 2

SELECT id FROM accounts
UNION ALL
SELECT account_id FROM orders
EXCEPT
SELECT 3
```

Supported query features include:

- `*`, `t.*`, projections, literals, expressions, and aliases. `SELECT`
  without `FROM`, such as `SELECT 2 + 3, 'hello'`.
- Three-valued logic. NULL comparisons are UNKNOWN; `AND`/`OR`/`NOT` follow
  the SQL truth tables. `WHERE`, `HAVING` and `ON` keep only TRUE rows.
- Comparisons: `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN (list)`,
  `LIKE … [ESCAPE …]`, `IS [NOT] NULL`, and `IS [NOT] TRUE/FALSE/UNKNOWN`.
- Operators: `+`, `-`, `*`, `/`, `%`, `||`, and the integer-only bitwise
  operators `&`, `|`, `^`.
  - `INTEGER / INTEGER` truncates toward zero.
  - Division by zero is 22012.
  - Overflow is 22003.
- `CAST`, searched and simple `CASE`, `COALESCE`, `NULLIF`, `UPPER`, `LOWER`,
  `CHAR_LENGTH`, `ABS`, and `DATE 'YYYY-MM-DD'` literals. The results of a
  `CASE` or `COALESCE` take one numeric type: integers next to `DECIMAL`
  become `DECIMAL`, and anything next to `DOUBLE` becomes `DOUBLE`.
- Aggregates: `COUNT(*)`, `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`, each with
  optional `DISTINCT`. NULLs are ignored, and an aggregate over no rows
  returns NULL (`COUNT` returns 0). `SUM`/`AVG` over `DECIMAL` are exact.
- `GROUP BY` (columns, expressions, select-list ordinals) and `HAVING`, with
  grouping validation (42803).
- `SELECT DISTINCT`, and `UNION`/`EXCEPT`/`INTERSECT` with or without `ALL`.
  `INTERSECT` binds tighter; arity and column categories are checked. With
  `DISTINCT` or a set operation, `ORDER BY` must name output columns.
- `ORDER BY`:
  - Keys can be expressions, output aliases, ordinals, and columns not in the
    select list.
  - Multiple keys, `ASC`/`DESC`, and `NULLS FIRST/LAST` are supported.
  - `LIMIT`/`OFFSET` are applied after ordering.
- Quoted string literals (`''` escapes) and quoted identifiers.

Every statement is statically type-checked before any row is read. Mixing
type categories, such as comparing a number with a non-literal string or
adding a boolean, is 42804, even on an empty table. A character literal next
to a date or time value is converted (`d = '2024-01-02'`).

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
WHERE id NOT IN (SELECT user_id FROM orders WHERE user_id IS NOT NULL)

SELECT name, (SELECT MAX(total) FROM orders o WHERE o.user_id = u.id) AS top
FROM users u

SELECT *
FROM (SELECT id, name FROM users) AS selected_users
```

AeroDB supports:
- `INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS` joins, comma-separated
  `FROM` lists, table aliases, and multiple joins.
- `IN`/`NOT IN` subqueries with SQL NULL semantics.
- Correlated and uncorrelated `EXISTS`.
- Scalar subqueries: more than one row is 21000, and zero rows give NULL.
- Subqueries in `FROM`.

Correlated references bind to the innermost scope that has a matching column.

### information_schema

A read-only subset describes the catalog:
- `information_schema.tables`
- `columns`
- `table_constraints`
- `key_column_usage`
- `referential_constraints`
- `check_constraints`
- `views`

Its rows reflect the current transaction's own DDL. They can be filtered,
joined, sorted, aggregated, and used in views. Writes or DDL against
`information_schema` are 0A000. Tables report `table_schema = 'public'`, but
`public.t` is not accepted as a qualifier (see limitations).

```sql
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'accounts'
ORDER BY ordinal_position
```

### Data types

| Type | Notes |
| --- | --- |
| `INTEGER` / `INT` | Signed 32-bit integer column; expressions compute in 64-bit and overflow is 22003 on assignment |
| `SMALLINT[(width)] [UNSIGNED]` | Signed or unsigned range validation |
| `MEDIUMINT[(width)] [UNSIGNED]` | Signed or unsigned range validation |
| `DECIMAL[(p[, s])]` / `NUMERIC[(p[, s])]` | Exact fixed-point, precision 1–38 (default `(18, 0)`), rounded half away from zero to the scale |
| `DOUBLE[(precision, scale)] [UNSIGNED]` | 64-bit floating point; inf/NaN results are 22003 |
| `TEXT` | Variable-length UTF-8 text |
| `VARCHAR(n)` / `CHARACTER VARYING(n)` | At most `n` characters (1–65535); longer values are 22001 |
| `CHAR[(n)]` / `CHARACTER[(n)]` | Fixed length, space-padded by characters; compared with PAD SPACE |
| `BOOLEAN` / `BOOL` | `TRUE`, `FALSE`, or NULL (UNKNOWN) |
| `DATE` | `YYYY-MM-DD`; `DATE '…'` literal |
| `DATETIME` | `YYYY-MM-DD HH:MM:SS` |
| `TIMESTAMP` | `YYYY-MM-DD HH:MM:SS` (no `TIMESTAMP '…'` literal; use a string) |
| `TIME` | `[-]HH:MM:SS`, up to 838 hours (no `TIME '…'` literal) |
| `YEAR` | `0000` or `1901` through `2155` |

A stored row must fit in one 4 KiB page, about 4 KB of serialized data.
Larger table rows are rejected with 54000 before anything is written. (An
index entry that outgrows a page is a different case, 58030; see Current
limitations.)

### Error codes (SQLSTATE)

Every error is a `DbError`. `DbError::sqlstate()` returns its five-character
SQLSTATE, with exactly one code per variant:

| SQLSTATE | Condition | `DbError` variants |
| --- | --- | --- |
| 0A000 | feature not supported (incl. writes to information_schema) | `FeatureNotSupported` |
| 21000 | cardinality violation | `CardinalityViolation` |
| 22001 | string data, right truncation | `StringTooLong` |
| 22003 | numeric value out of range | `Overflow`, `NumericOverflow` |
| 22012 | division by zero | `DivisionByZero` |
| 22018 | invalid character value for cast | `InvalidCast` |
| 22023 | invalid parameter value | `InvalidValue` |
| 23502 | not-null violation | `NullViolation` |
| 23503 | foreign key violation (statement end, RESTRICT, or COMMIT recheck) | `ForeignKeyViolation` |
| 23505 | unique violation (PRIMARY KEY or UNIQUE) | `DuplicateKey`, `UniqueViolation` |
| 23514 | check violation | `CheckViolation` |
| 25P01 | no active SQL transaction | `NoActiveTransaction` |
| 25P02 | in failed SQL transaction | `InFailedTransaction` |
| 2BP01 | dependent objects still exist | `DependentObjects` |
| 40001 | serialization failure (write/write conflict) | `WriteConflict` |
| 42601 | syntax error | `Syntax`, `ParseError` |
| 42701 | duplicate column | `DuplicateColumn` |
| 42702 | ambiguous column or relation name | `AmbiguousColumn`, `AmbiguousRelation` |
| 42703 | undefined column | `ColumnNotFound` |
| 42704 | undefined object (sequence, index, ...) | `NotFound` |
| 42710 | duplicate object (constraint name) | `DuplicateConstraint` |
| 42803 | grouping error | `GroupByMismatch` |
| 42804 | datatype mismatch | `TypeMismatch` |
| 42809 | wrong object type | `WrongObjectType` |
| 42830 | invalid foreign key | `InvalidForeignKey` |
| 42883 | undefined function | `UndefinedFunction` |
| 42P01 | undefined table (incl. unknown schema qualifier) | `TableNotFound` |
| 42P07 | duplicate table, view, index or sequence | `DuplicateObject` |
| 54000 | program limit exceeded (row too large for a page) | `RowTooLarge` |
| 58030 | I/O error | `Io` |

The codes classify errors the way the SQL standard and PostgreSQL do. The
message text is AeroDB's own. For example, the missing-table message keeps
the 0.5.1 wording `no such table: t`.

## Transactions and MVCC

Mutating statements execute in automatic transactions unless an explicit
transaction is active:

```sql
BEGIN
INSERT INTO accounts (id, email, balance) VALUES (3, 'carol@example.com', 50)
UPDATE accounts SET balance = 75 WHERE id = 3
COMMIT
```

Use `ROLLBACK` to abort the active transaction:

```sql
BEGIN TRANSACTION balance_change
UPDATE accounts SET balance = 0 WHERE id = 3
ROLLBACK
```

Every statement is atomic. In autocommit mode, a failed statement leaves no
trace. Inside an explicit transaction, a failed statement puts the transaction
into the failed state:
- Every further statement is rejected with 25P02 until `ROLLBACK`.
- `COMMIT` of a failed transaction rolls it back and returns 25P02.

A syntax error counts as a failed statement in the CLI and through
`Engine::execute_sql`, `Engine::query` and `Engine::parse`. Only a caller that
parses with the free function `parse_statement` and then calls
`Engine::execute` bypasses this, because the parse never reaches the engine.

DDL (`CREATE`/`DROP`/`ALTER`, views) is transactional within one session and
rolls back with the transaction.

The only isolation level is snapshot isolation. It is **not SERIALIZABLE**:
snapshot isolation does not prevent write skew, and `SET TRANSACTION` is
rejected with 0A000.

- A transaction reads from the snapshot captured at `BEGIN`.
- It sees its own writes.
- It does not see another transaction's uncommitted work.
- A transaction keeps a stable view when another transaction commits.
- Concurrent writes to the same logical key use first-committer-wins conflict
  resolution (40001).
- Primary key, `UNIQUE`, and foreign-key constraints are rechecked at
  `COMMIT` against concurrently committed work.
- Rolled-back versions remain physically present but invisible until vacuum
  reclaims them.
- Savepoints are not supported.

Transaction IDs and final transaction states survive database reopen. WAL
recovery treats transactions left active by a crash as aborted.

## Rust API

These are embedded Rust APIs. They are not an implementation of the SQL
standard's SQL/CLI binding, and AeroDB provides no ODBC or JDBC driver. Errors
are `DbError` values; `DbError::sqlstate()` returns the SQLSTATE (see
[Error codes](#error-codes-sqlstate)).

### Typed queries

`Engine::query` and `Database::query` return a `QueryResult`:
- `columns`: name and SQL type per result column.
- `rows`: typed `Value`s, with SQL NULL as `Value::Null`.

```rust
use aerodb::{engine::Engine, error::DbError};

fn main() -> Result<(), DbError> {
    let mut engine = Engine::open("app.aerodb")?;
    engine.execute_sql("CREATE TABLE items (id INTEGER PRIMARY KEY, price DECIMAL(8, 2))")?;
    engine.execute_sql("INSERT INTO items VALUES (1, 9.99), (2, NULL)")?;

    let result = engine.query("SELECT id, price FROM items ORDER BY id")?;
    for column in &result.columns {
        println!("{} {}", column.name, column.ty.name());
    }
    for row in &result.rows {
        let cells: Vec<String> = row.iter().map(|v| v.to_string()).collect();
        println!("{}", cells.join(" | ")); // "1 | 9.99", then "2 | NULL"
    }

    match engine.query("SELECT missing FROM items") {
        Err(e) => println!("{} ({})", e, e.sqlstate()), // 42703
        Ok(_) => unreachable!(),
    }
    Ok(())
}
```

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
    let result = db.query(&reader, "SELECT id, body FROM messages WHERE id = 1")?;
    db.abort(reader)?;

    println!("{} visible row(s)", result.rows.len());
    Ok(())
}
```

The main methods are:

| Method | Purpose |
| --- | --- |
| `Database::open(path)` | Open or create a shared database |
| `begin()` | Start a snapshot-isolated transaction |
| `execute(&tx, statement)` | Run a DDL or DML statement in a transaction |
| `query(&tx, sql)` | Run a SQL query in a transaction and return typed rows |
| `query_all(&tx, table)` | Read all raw rows of a table visible to the transaction |
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
use aerodb::engine::Engine;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut engine = Engine::open("app.aerodb")?;

    engine.execute_sql("BEGIN")?;
    engine.execute_sql("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)")?;
    engine.execute_sql("COMMIT")?;

    let report = engine.vacuum_table("events")?;
    println!("removed {} version(s)", report.versions_removed);
    Ok(())
}
```

`Engine::execute_sql` (or `Engine::execute` with a parsed statement):
- Handles automatic transactions and intercepts `BEGIN`, `COMMIT`, and
  `ROLLBACK`.
- Prints query results the way the CLI does. Use `Engine::query` to get them
  as values instead.

`Engine::new(path)` panics if the file cannot be opened, so prefer
`Engine::open`. Call `Engine::vacuum_table` directly because there is not yet a
SQL `VACUUM` statement.

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
   below the oldest live snapshot boundary, clears delete stamps left by
   rolled-back transactions, then rebuilds affected indexes. An all-tables
   vacuum also advances the frozen watermark and compacts the commit-status log.
8. Point lookups descend to the first leaf that can hold a key and follow the
   leaf chain across that key's version chain, so they cost O(tree height +
   versions of the key) rather than a full scan.

Reserved main-file pages:

| Page | Contents |
| --- | --- |
| `0` | Engine metadata, storage format version, and durable transaction counters |
| `1` | Table and view catalog |
| `2` | Sequence catalog |
| `3+` | Table and index B-Tree pages |

For the implemented concurrency design and its invariants, see
[`specs/mvcc-concurrent-transactions.md`](specs/mvcc-concurrent-transactions.md).

## Current limitations

AeroDB implements a SQL subset. It is not a complete implementation of any SQL
standard and is not certified against SQL:2023. The audited feature-by-feature
status is in [`specs/sql-conformance-0.6.0.md`](specs/sql-conformance-0.6.0.md).

Not implemented. These are rejected with 0A000, or 42883 for unknown
functions, and never silently ignored:
- `SAVEPOINT`, `RELEASE`, `ROLLBACK TO`, and `SET TRANSACTION` (isolation
  level, `READ ONLY`).
- `WITH` (CTEs), window functions and `WINDOW`, `JSON_VALUE` and other JSON
  functions, and cursors (`DECLARE`).
- `TIMESTAMP '…'`, `TIME '…'` and `INTERVAL` literals.
- `INSERT … SELECT`, `VALUES` as a query, `NATURAL`/`USING` joins,
  `FETCH FIRST`, parenthesized joins, `IS DISTINCT FROM`, `ANY`/`SOME`/`ALL`,
  row value constructors, and `SIMILAR TO`.
- `ON DELETE`/`ON UPDATE SET NULL`/`SET DEFAULT`, `DROP … CASCADE`,
  `CREATE OR REPLACE VIEW`, DML on views, `ALTER TABLE` other than
  `ADD COLUMN`, `CREATE UNIQUE INDEX`, and multi-column indexes.

Transactions and DDL:
- Snapshot isolation is the only isolation level; it is not SERIALIZABLE.
- DDL is not snapshot-versioned.
  - Concurrent `Database` transactions see another transaction's DDL
    immediately, including in `information_schema`.
  - An older snapshot sees a newly added column as NULL.
  - `ALTER TABLE ADD COLUMN` does not rewrite rows of concurrent uncommitted
    transactions; they read NULL, not the DEFAULT.
  - Concurrent DDL in separate `Database` transactions is not isolated from
    each other.
- Shared storage is process-local. Multiple `Database` transactions can run in
  one process, but cross-process coordination is not supported.
- The pager latches pages individually, but the `Database` facade still
  serializes operations behind one storage mutex, so readers and writers do not
  yet run in parallel.

Naming:
- `public.t` is rejected with 42P01, although `information_schema` reports
  `table_schema = 'public'`. Schema-qualified column references are 0A000.
  The only usable schema qualifier is `information_schema`.

Storage and performance:
- Secondary-index metadata is kept in memory and is not rebuilt when the
  database is reopened. Recreate indexes after opening a new process.
- Secondary indexes optimize single-column equality predicates only.
- A table row must fit in one page (about 4 KB serialized); otherwise the
  write fails with 54000. This applies to table rows only.
- A secondary index keeps one entry per distinct value holding all row keys
  with that value, and the entry must fit in one page. About 810 rows sharing
  one value on an indexed column (INTEGER: INSERT number 811; TEXT 'active':
  row 810) make `INSERT` fail with 58030 "Leaf overflow: not enough space". A
  1000-row multi-row `INSERT` with one repeated value fails (200 succeed).
  `CREATE INDEX` over more than about 810 equal values fails, and because
  indexes are not persisted across reopen, recreating them on such data after
  reopen fails. `CREATE INDEX` reads every stored row version (including
  aborted or deleted ones not yet vacuumed), so the limit can be hit before
  810 visible rows. The rejection is clean: no partial statement effect, and
  inside `BEGIN` the transaction fails (25P02 policy). This predates 0.6.0.
  Planned fix: composite (hash, row key) index keys or overflow buckets.
- `UPDATE`/`DELETE … WHERE`, primary-key and `UNIQUE` checks scan the visible
  rows. Foreign-key checks scan the parent rows (O(n·m)).
- `ROLLBACK` writes the aborted transaction's pages durably, so it costs
  fsyncs like a commit.
- Pages abandoned by aborted transactions are never reused (there is no
  free-page list).
- Vacuum is explicit; there is no SQL command or background auto-vacuum.

Other:
- Standalone sequences are available through SQL creation and the catalog API,
  but there is no SQL `NEXTVAL` expression yet.
- The CLI uses the fixed `data.aerodb` path, accepts one statement per line,
  and prints results as text.
- The library prints status lines to stdout, such as `Table t created`. This
  happens even through `Engine::execute_sql`, `Engine::query` and the
  `Database` API, and is inherited from the CLI.

The full list of known deviations is in the
[0.6.0 changelog](CHANGELOG.md#060---2026-10-14).

## Project layout

```text
src/
├── catalog/       Persistent table/view/sequence metadata and index management
├── constraints/   Key (PRIMARY KEY/UNIQUE/FK), CHECK, NOT NULL, and default constraints
├── execution/     Binder, evaluator, query executor, DDL/DML runtime, information_schema
├── sql/           Lexer, parser, AST, typed values, DECIMAL, and system functions
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
cargo test --all-targets
cargo clippy --all-targets
```

The full test suite covers SQL parsing/execution, constraints, types, joins,
nested queries, auto-commit behavior, durable transaction state, MVCC visibility,
rollback, vacuum, real concurrent transactions, and multi-threaded conflict
stress.

When contributing:

1. Add or update tests for the behavior being changed.
2. Keep existing tests passing.
3. Run `rustfmt` on the files you change.
4. Run the full test suite.
5. Explain user-visible or on-disk behavior changes in the pull request.

Issues and pull requests are welcome at
[github.com/urunsiyabend/aerodb](https://github.com/urunsiyabend/aerodb).

## Roadmap

- Persist and reload secondary-index metadata
- Split the `Database` storage mutex so per-page latching yields real
  reader/writer parallelism
- Add SQL-level vacuum and automatic maintenance
- Expand SQL coverage (see the remaining gaps in
  [`specs/sql-conformance-0.6.0.md`](specs/sql-conformance-0.6.0.md)) and
  improve query planning
- Stabilize and document the on-disk format
- Add storage-engine and B-Tree implementation documentation

## License

AeroDB is available under the [MIT License](LICENSE).
