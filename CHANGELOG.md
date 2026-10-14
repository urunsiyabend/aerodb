# Changelog

All notable changes to AeroDB are documented here. The project follows
[Semantic Versioning](https://semver.org/); while below 1.0, the on-disk format
may still change between minor versions.

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
