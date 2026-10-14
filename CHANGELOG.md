# Changelog

All notable changes to AeroDB are documented here. The project follows
[Semantic Versioning](https://semver.org/); while below 1.0, the on-disk format
may still change between minor versions.

## [0.6.0] - 2026-10-14

This release replaces the SQL front end and executor. It closes every bug the
0.5.1 SQL audit found and adds the core features listed below. It is **not** a
SQL:2023 certification and claims no overall conformance percentage; the
per-feature evidence and every remaining gap are in
[`specs/sql-conformance-0.6.0.md`](specs/sql-conformance-0.6.0.md).

### Added

- **Fail-closed parser.** One recursive-descent parser over a real lexer
  (comments `--` and `/* */`, quoted identifiers, `''` escapes). A statement
  must consume all of its input. Unknown syntax is a syntax error (42601),
  unknown functions are 42883, and recognised but unimplemented features are
  rejected with 0A000 instead of being ignored.
- **Typed values and evaluator.** Expressions evaluate to typed `Value`s
  (NULL, BOOLEAN, INTEGER, DECIMAL, DOUBLE, CHAR/VARCHAR/TEXT, DATE, TIME,
  DATETIME, TIMESTAMP, YEAR). Every query, DML statement, DEFAULT and CHECK is
  statically bound before any row is read. Type-category mismatches are 42804
  even on empty tables.
- **Exact `DECIMAL(p,s)` / `NUMERIC(p,s)`** (precision 1–38, default
  `(18,0)`), with exact arithmetic, half-away-from-zero rounding on assignment,
  and 22003 on overflow. `DOUBLE` operations that would produce inf/NaN are
  22003.
- **`VARCHAR(n)` / `CHARACTER VARYING(n)`.** Lengths count characters; too long
  is 22001. `CHAR(n)` now also pads and counts by characters.
- **Three-valued logic.** NULL comparisons are UNKNOWN; `AND`/`OR`/`NOT` use
  the SQL truth tables; NULL propagates through arithmetic. Added
  `IS [NOT] NULL` and `IS [NOT] TRUE/FALSE/UNKNOWN`. WHERE, HAVING, ON and
  CHECK keep only TRUE (CHECK rejects only FALSE).
- **ORDER BY, fully applied:** multiple keys, `ASC`/`DESC`, `NULLS
  FIRST/LAST`, expressions, output aliases, ordinals, and columns not in the
  select list. `LIMIT`/`OFFSET` are applied after ordering.
- **Aggregate rules.** `COUNT(expr)` skips NULLs. `AVG`/`SUM`/`MIN`/`MAX`
  ignore NULLs and return NULL over no rows. A global aggregate over zero rows
  returns one row. `DISTINCT` works in every aggregate. `SUM`/`AVG` over
  DECIMAL are exact, and integer `SUM` overflow is 22003. Grouping is
  validated (42803).
- **Scalar subquery cardinality.** More than one row is 21000; zero rows give
  NULL. IN and NOT IN subqueries follow NULL semantics. `EXISTS` and
  correlated subqueries bind to the innermost matching scope.
- **Predicates and expressions:** `IN (list)`, `LIKE … [ESCAPE]`, `BETWEEN`,
  `CAST`, searched and simple `CASE`, `COALESCE`, `NULLIF`, `||`, and
  `DATE '…'` literals.
- **`SELECT DISTINCT`** and **`UNION` / `EXCEPT` / `INTERSECT` with `[ALL]`**
  (`INTERSECT` binds tighter). Set operations check arity (42601) and column
  categories (42804).
- **Constraints:**
  - `UNIQUE` and `CHECK` at column and table level, with `CONSTRAINT name`
    (duplicate names are 42710).
  - PRIMARY KEY on any column, including composite, text, and non-first
    columns. Rows get a logical row identity; the first column no longer has
    to be an INTEGER storage key.
  - Composite foreign keys with `MATCH SIMPLE` semantics: a row with any NULL
    key column is not checked.
  - `ON UPDATE CASCADE`, and `ON DELETE` / `ON UPDATE RESTRICT`. RESTRICT is
    checked immediately; NO ACTION is checked at the end of the statement.
  - Foreign keys are rechecked at COMMIT for concurrent `Database`
    transactions.
- **Views:** `CREATE VIEW v [(cols)] AS query` and `DROP VIEW [IF EXISTS] v
  [RESTRICT]`. Views persist across reopen. `*` is expanded when the view is
  created. Views are read-only (0A000 for DML).
- **`ALTER TABLE t ADD [COLUMN] coldef`**, with `DEFAULT`, `NOT NULL`,
  `UNIQUE` and `CHECK` validated against existing rows.
- **`DROP TABLE … RESTRICT`.** Dropping a table that a foreign key or view
  depends on is 2BP01 (`CASCADE` is 0A000).
- **`information_schema` subset (read-only):** `tables`, `columns`,
  `table_constraints`, `key_column_usage`, `referential_constraints`,
  `check_constraints`, and `views`.
- **SQLSTATE codes.** Every `DbError` variant maps to exactly one SQLSTATE via
  `DbError::sqlstate()`; the table is in the README.
- **Typed Rust query API:** `Engine::query(sql) -> QueryResult` (typed rows
  and result column types), `Engine::execute_sql(sql)`, `Engine::open(path)`,
  and `Database::query(&tx, sql)`. These are embedded Rust APIs, not SQL/CLI,
  ODBC or JDBC.
- **Storage format version 2** recorded in the metadata page (see Storage).

### Fixed

These are the 0.5.1 audit bugs.

- `v = NULL` and `v <> NULL` matched rows, NULL+1 returned 1, and UNKNOWN in
  `AND`/`OR` was wrong. `UPDATE … SET v = NULL` was rejected on nullable
  INTEGER columns.
- `ORDER BY` was parsed but not applied (DESC, non-selected columns,
  `NULLS FIRST`).
- `UNION`/`UNION ALL`/`EXCEPT`/`INTERSECT` returned only the first branch, and
  trailing garbage after a statement was silently ignored.
- `COUNT(v)` counted NULLs, `AVG` divided by NULL rows, global aggregates over
  no rows returned nothing, all-NULL groups returned 0, and `COUNT(DISTINCT v)`
  ignored DISTINCT.
- Scalar subqueries took the first of many rows and returned `''` for zero
  rows. `IN (subquery)` treated NULL as equal.
- Foreign keys compared only the first column. Nullable foreign-key values
  were rejected, and a foreign key to a non-first parent key rejected valid
  rows.
- `DROP TABLE … RESTRICT` dropped a parent that still had dependent foreign
  keys.
- Composite primary keys sharing their first column, such as `(1,2)` and
  `(1,3)`, were rejected. `CHAR` primary keys could not be inserted.
  `ON UPDATE CASCADE` was rejected as NO ACTION.
- Defects found while building this release:
  - The catalog B-Tree corrupted itself (hang) at about 25 tables.
  - Catalog keys were reused after DROP.
  - ROLLBACK reverted only catalog pages 1–2.
  - A rolled-back root split left a misrouted tree.
  - A transaction id could be reused after ROLLBACK and reopen.
  - A failed statement left 0.5.1's transaction installed.
  - DML on a missing table affected 0 rows instead of failing.
  - `CREATE TABLE t (x INTEGER, x INTEGER)` was accepted.
  - An oversized table row failed with an I/O "Leaf overflow" (now 54000
    before any write). Index entries with many rows per value still fail with
    58030, see Known limitations.
- Found in the final review of this release (the first three also affected
  0.5.1):
  - Secondary indexes keyed entries by a lossy hash. A second value with the
    same hash failed with an I/O "Duplicate key" error (58030) on `INSERT`,
    `UPDATE` or `CREATE INDEX`. Examples: NULL next to 0, `DOUBLE` 1.5 next
    to 1.7, and colliding TEXT or DECIMAL values. After such an `UPDATE`,
    `CREATE INDEX` kept failing. An index entry now keeps one group of rows
    per distinct value, lookups use only the exact value, and NULL is never
    indexed.
  - Dropping a table left its `AUTO_INCREMENT` sequence behind, so creating
    the table again failed.
  - The sequences of `a_b(c AUTO_INCREMENT)` and `a(b_c AUTO_INCREMENT)` had
    the same name, so the second `CREATE TABLE` failed.
  - `CREATE TABLE O` succeeded next to an existing `o` (and `CREATE TABLE OO`
    next to a view `oo`). Every unquoted reference to either then failed as
    ambiguous.
  - A duplicate index name was an I/O error (58030), and a missing index was
    reported as `index 'x' not found not found`.
  - The CLI logged syntax errors at the hidden `warn` level, and a syntax
    error inside `BEGIN` did not fail the transaction.
  - `CASE WHEN … THEN dec_col ELSE 1 END` returned a mix of INTEGER and
    DECIMAL values.

### Changed / breaking

- `INTEGER / INTEGER` truncates toward zero (`7 / 2` is `3`); use DECIMAL or
  CAST for exact quotients. Division by zero is 22012.
- Identifiers follow the standard rule: unquoted identifiers match
  case-insensitively, quoted identifiers match exactly. Ambiguous references
  are 42702.
  - The rule also applies to the names of tables, views, indexes and
    sequences. `CREATE TABLE`/`VIEW`/`INDEX`/`SEQUENCE` with an unquoted name
    that matches an existing name case-insensitively is 42P07 (`IF NOT EXISTS`
    makes it a no-op). A quoted name collides only with the exact same name.
  - `DROP TABLE`, `DROP VIEW` and `DROP INDEX` find names by the same rule.
  - An unquoted table, view or index name that matches several objects is
    42702 (`AmbiguousRelation`); quote it to choose one.
- A bareword is no longer read as a string literal. `WHERE name = alice` is
  now a column reference (42703 if no such column exists).
- A FOREIGN KEY must reference the parent's PRIMARY KEY or a UNIQUE column
  set; otherwise CREATE TABLE fails with 42830. Existing 0.5.x catalogs still
  load.
- `DROP TABLE` of a missing table (without `IF EXISTS`) is 42P01 instead of
  succeeding silently. `CREATE TABLE` of an existing name (without
  `IF NOT EXISTS`) is 42P07 instead of an I/O error. `DROP TABLE` on a view
  and `DROP VIEW` on a table are 42809. The missing-table message keeps the
  0.5.1 wording `no such table: t`.
- Inside an explicit transaction, a failed statement puts the transaction
  into the failed state. Every further statement is 25P02 until `ROLLBACK`.
  `COMMIT` of a failed transaction rolls it back and returns 25P02.
  - A syntax error is a failed statement in the CLI and through
    `Engine::execute_sql`, `Engine::query` and `Engine::parse`.
  - Only callers that parse with the free function `parse_statement` and
    then call `Engine::execute` bypass this.
- The CLI prints every error, including syntax errors, as
  `Error: <message> (<SQLSTATE>)`. Its line handler is
  `aerodb::repl::run_line`.
- Duplicate index and sequence names are 42P07; a missing index is 42704.
- The results of `CASE` and `COALESCE` are converted to one numeric type.
  Integers next to `DECIMAL` become `DECIMAL` (scale 0, as in PostgreSQL).
  Integers and `DECIMAL` next to `DOUBLE` become `DOUBLE`.
- `AUTO_INCREMENT` sequences of new tables get an internal name that cannot
  collide. Sequences named `{table}_{column}` by 0.5.x are still used, and
  `DROP TABLE` drops the table's sequences. A 0.5.x sequence name shared by
  two tables is kept until neither uses it.
- Result headers show SQL type labels derived from the expression (for
  example, `SUM` over DOUBLE is `DOUBLE`, and DECIMAL shows `DECIMAL(p,s)`).
  Some labels differ from 0.5.1.
- Assignment rules (INSERT VALUES / UPDATE SET):
  - Character literals convert at bind time to the column type; an invalid
    literal is 22xxx.
  - Integers go into YEAR, and numeric values into numeric columns.
  - Any value, including numeric, date and boolean values, may be assigned to
    `CHAR`/`VARCHAR`/`TEXT`. This follows PostgreSQL's assignment behaviour
    and is more permissive than the standard.
  - Mixed categories are rejected at bind time with 42804: non-literal
    character expressions into numeric, date or boolean columns, and boolean
    to or from numeric or date.
- RESTRICT semantics: an old key still referenced by a child row that the same
  statement does not delete or re-point fails with 23503. A key swap fails
  under RESTRICT and succeeds under NO ACTION.
- The `GROUP BY` restriction that rejected extra grouping columns was removed
  (it rejected valid SQL).
- `src/main.rs` links the `aerodb` library instead of recompiling its modules.
  Library unit tests therefore run once, not twice, and test totals are not
  comparable 1:1 with 0.5.1.
- Removed dead, uncompiled sources `src/planner/*`,
  `src/execution/plan.rs` and `src/execution/executor.rs`.

### Storage

- The metadata page records storage format version 2.
- Files written by 0.5.x (version 0) open read/write; the first write stamps
  version 2.
- Opening a file written by 0.6.0 with 0.5.x is **unsupported** and may
  misbehave silently. 0.5.x ignores the version field, and 0.6.0 stores rows
  differently. For example, a row's storage key is its single integer
  primary-key column wherever that column is, or an allocated row id (no
  primary key, a text or a composite key), while 0.5.x assumes the key is the
  first column. Only some new content fails explicitly in 0.5.x: a view
  row ("root page not int"), new type codes ("bad type"), or new value tags
  ("Unknown type tag"). No 0.5.x reader was tested against v2 files.
- An unknown future version is rejected with an `InvalidData` error before WAL
  recovery. The main file, `.wal` and `.clog` are left unmodified.
- Covered by two committed 0.5.1 fixtures (`tests/storage_format.rs`).

### Known limitations

- Not implemented (rejected with 0A000 or 42883):
  - `SAVEPOINT` / `RELEASE` / `ROLLBACK TO`, `SET TRANSACTION` (isolation
    level, `READ ONLY`).
  - `WITH` (CTEs), window functions and `WINDOW`, `JSON_VALUE` / JSON, cursors
    (`DECLARE`).
  - `TIMESTAMP '…'` and `TIME '…'` literals, `INTERVAL`.
  - `INSERT … SELECT`, `VALUES` as a query, `NATURAL` / `USING` joins,
    `FETCH FIRST`.
  - `IS DISTINCT FROM`, `ANY`/`SOME`/`ALL`, row value constructors,
    `SIMILAR TO`.
  - `ON DELETE/UPDATE SET NULL/SET DEFAULT`, `DROP … CASCADE`,
    `CREATE OR REPLACE VIEW`, `ALTER TABLE` other than `ADD COLUMN`,
    `CREATE UNIQUE INDEX`, multi-column indexes.
- Isolation is snapshot isolation only, **not SERIALIZABLE**. Snapshot
  isolation does not prevent write skew, and `SET TRANSACTION` is rejected.
- DDL is not snapshot-versioned:
  - A concurrent older snapshot sees a newly added column as NULL.
  - `ALTER TABLE ADD COLUMN` does not rewrite rows of concurrent uncommitted
    transactions; they read NULL, not the DEFAULT.
  - DDL in concurrent detached `Database` transactions is not isolated:
    another transaction's commit can flush an uncommitted transaction's
    catalog page.
  - Concurrent DDL is visible in `information_schema` immediately.
- Secondary indexes are still not persisted across reopen; recreate them after
  opening.
- A row's serialized image must fit in one 4 KiB page (about 4 KB). Larger
  rows are rejected with 54000 before any write.
- Schema qualifiers:
  - `public.t` is 42P01 even though `information_schema` reports
    `table_schema = 'public'`.
  - Schema-qualified column references are 0A000.
  - The only schema accepted in FROM is `information_schema`.
- Performance:
  - Foreign-key checks scan the parent rows (O(n·m)).
  - `UPDATE`/`DELETE … WHERE` and primary-key or UNIQUE checks scan visible
    rows.
  - Many rows with the same indexed value in one transaction grow
    quadratically.
  - A secondary index stores one entry per distinct indexed value holding
    every row key with that value, and that entry must fit in one page. About
    810 rows sharing one value on an indexed column (INTEGER: INSERT number
    811 fails; TEXT 'active': row 810) make `INSERT` fail with 58030 "Leaf
    overflow: not enough space". A 1000-row multi-row `INSERT` into an
    indexed table with one repeated value fails (200 rows succeed).
    `CREATE INDEX` over more than about 810 equal values fails too; since
    indexes are not persisted across reopen, recreating them after reopen
    fails on such data. `CREATE INDEX` reads every stored row version
    (including aborted or deleted ones not yet vacuumed), so the limit can be
    reached before 810 visible rows. The rejection is clean: no partial
    statement effect, and inside `BEGIN` the transaction fails (25P02
    policy). This predates 0.6.0 (0.5.1 used the same entry layout). Planned
    fix: composite (hash, row key) index keys or overflow buckets.
- Durability costs and space:
  - ROLLBACK writes the aborted transaction's pages durably and costs fsyncs
    proportional to the pages it touched, plus a metadata write.
  - Pages abandoned by aborted transactions or root pinning are never reused
    (there is no free-page list).
- Crash windows:
  - The first transaction of a brand-new database can restart transaction ids
    at 1 if it crashes between its abort record and the metadata write.
  - A page image may contain another live transaction's uncommitted split.
- Binding and evaluation:
  - Some type errors are still found per value at run time rather than at
    bind time. Examples: `CASE`/`COALESCE` of only string constants assigned
    into numeric or boolean columns.
  - A date-only string is not accepted for TIMESTAMP/DATETIME.
  - Chained comparisons such as `a = b = c` are accepted.
  - `BEGIN IMMEDIATE`/`DEFERRED` parse the word as a transaction name.
  - `GROUP BY -1` and `GROUP BY 1.0` are not ordinals (42803).
  - A grouped outer query with an outer-only aggregate in a subquery is 42803,
    not 0A000.
  - The grouping check compares column references, not whole expressions
    (fail-closed).
- Metadata:
  - `unique_constraint_name` prefers the PRIMARY KEY when a UNIQUE constraint
    covers the same columns.
  - Primary key violation messages do not name the constraint.
  - Defaults created by 0.5.1 are shown as quoted literals (`'5'`).
  - The precision and scale of a computed DECIMAL result column in the
    result header come from its first value, for example
    `SUM(total) DECIMAL(5,2)` on a `DECIMAL(12,2)` column.
- The CLI still uses the fixed `data.aerodb` path and one statement per line.
  It prints results as text.
- The library still prints status lines to stdout, such as `Table t created`
  and `View v dropped`. This also happens through the Rust APIs.
- `Engine::new` still panics if the file cannot be opened; use
  `Engine::open`.

## [0.5.1] - 2026-10-14

### Fixed

- **Rows lost after a rolled-back DELETE/UPDATE plus `vacuum_all` and reopen.**
  Logical abort leaves the live version stamped with the aborted transaction as
  its deleter. `vacuum_all` then compacted that transaction's clog status, and
  after the next commit truncated the WAL, a reopened database read the absent
  deleter as committed and hid the row. Vacuum now clears delete stamps that
  belong to aborted transactions before the frozen watermark can pass them.
  Regression: `tests/mvcc_frozen_xid.rs::rolled_back_delete_survives_compaction_and_reopen`.
- **Misordered B-Tree internal nodes under repeated updates of one key.** Every
  MVCC update adds another version of the same key, so separators repeat. Node
  splits positioned the new right sibling by key comparison, which placed it
  *before* its left sibling among equal separators; later descents were misrouted
  and a `DELETE` could report success while the row stayed visible. The new
  sibling is now inserted directly after the page it was split from.
  Regressions: `tests/btree_versions.rs`.
- **INSERT failing with "insert_in_parent: parent is not internal"** after
  random-order key loads (e.g. hash-keyed index trees). When an internal node
  split, a freshly split-off child that stayed in the left half kept a stale
  parent pointer, so its own next split could not find its parent. Latent in
  0.5.0 (bulk loads were too slow to reach it); the faster point lookups below
  made it reachable. Regression:
  `storage::btree::tests::random_order_inserts_survive_internal_splits`.
- `mark_deleted_visible` now stamps the exact slot holding the visible version,
  even when that version lives in an earlier leaf than the key's insertion leaf.

### Performance

- Point lookups (`find_visible`, write-conflict checks, delete stamping, index
  probes, the INSERT duplicate check and FK parent probes) no longer scan every
  leaf of the tree and clone the whole transaction-status table. They descend to
  the first leaf that may hold the key and walk only that key's version chain,
  evaluating visibility under a read lock.
- Measured with `examples/point_lookup_bench.rs` (release build, 8000 rows,
  2000 probes, 200 updates). These are single local runs, not guarantees, and
  results vary by machine and run:

  | Metric (µs/op)         | Developer run    | Independent run   |
  |------------------------|------------------|-------------------|
  | Indexed equality probe | 1051.9 → 9.5     | 1060.8 → 9.8      |
  | Bulk INSERT, per row   | 1260.2 → 70.6    | 1266.1 → 180.5    |
  | Single-row UPDATE      | 5490.4 → 2758.3  | 5599.5 → 6731.6   |

  The indexed-lookup improvement reproduced. The INSERT gain varied between
  runs. UPDATE did **not** consistently improve: it was faster in one run and
  slower in the other. `UPDATE ... WHERE` still finds its rows with a full
  table scan (see Known issues), so this change does not claim to speed up
  UPDATE.

### Changed

- Removed a stale TODO: scalar subqueries in the SELECT list are already parsed
  and executed (`tests/nested_queries.rs`).
- README and `specs/mvcc-followups.md` now reflect that Phase 7 steps 1–2
  (per-page latching inside the pager/B-Tree) landed, while the `Database`
  facade still serializes operations.

### Known issues / deferred

- Databases written by ≤ 0.5.0 that already contain misordered internal nodes
  or stale parent pointers are not repaired; recreate heavily-updated tables
  (or dump and reload). Point lookups previously scanned every leaf and so
  tolerated misrouting; they now rely on routing.
- `UPDATE`/`DELETE ... WHERE` still evaluate their predicate with a full table
  scan, and the primary-key uniqueness check still scans all visible rows.
- `Pager::transaction_table()` (used by vacuum) still clones the whole status
  table, and the in-memory table is not pruned when the frozen watermark
  advances (only the durable clog is compacted).
- On reopen, WAL-recovered statuses are re-recorded into the clog even if they
  are below the frozen watermark (harmless: the statuses are accurate).
- Phase 7 steps 3–5 of `specs/mvcc-followups.md` remain open.

## [0.5.0]

Baseline for this changelog: MVCC snapshot isolation, durable clog, explicit
vacuum with frozen-watermark advancement, concurrent `Database` facade, and
per-page latching inside the pager and B-Tree.
