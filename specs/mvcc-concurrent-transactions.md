# Spec: Concurrent Multi-Transaction MVCC for AeroDB

Status: **Implemented (phases 1–5)** — one deviation: physical access uses a
single shared-storage latch, not per-page latches (see §5.5 / §11).
Author: prepared for a follow-up implementation session
Depends on: the MVCC correctness work already merged in this line of development
(authoritative visibility via the real transaction table, durable transaction-id
counter in page 0, transactional DDL). See "Current state" below.

## Implementation status (summary)

- **Phase 1 ✅** Per-session `Transaction` (`src/transaction/session.rs`) bundles
  id/snapshot/isolation with its write state; pager holds `Option<Transaction>`.
- **Phase 2 ✅** `TransactionManager` owns the active set + begin/commit/abort
  orchestration + `global_xmin`; `Engine` and the REPL route through it.
- **Phase 3 ✅** Durable clog (`src/transaction/clog.rs`, `<db>.clog`): status
  survives commit; WAL-overlay recovery; `frozen_xid` in page-0 meta.
- **Phase 4 ✅** Logical abort: no physical data-page revert; aborted versions
  hidden by visibility and reclaimed by vacuum (aborted-creator rule). Only the
  non-versioned, lock-serialized schema catalog pages (1,2) are still reverted.
- **Phase 5 ✅ (with deviation)** Concurrent multi-session `Database`
  (`src/db.rs`): N live snapshot-isolated transactions over shared storage,
  version-in-place writes, first-committer-wins (write-time + commit-time
  re-validation), thread-safe. **Deviation:** physical access is guarded by one
  storage latch (`Arc<Mutex<Core>>`), not the per-page buffer-pool latches of
  §5.5 — correctness holds, but readers and writers do not run in parallel. See
  §11.

---

## 1. Motivation

AeroDB today has a correct **single-active-transaction-per-process** MVCC engine:
version chains (`created_tx`/`deleted_tx`), snapshots, an authoritative
visibility predicate, write-conflict detection, vacuum, and MVCC-aware
constraints — all validated by the `tests/mvcc_*` suites.

What it is **not** is a concurrent DBMS. Only one transaction can be live in a
process at a time. The existing "concurrency" tests fake concurrency with two
separate `Engine` handles over one file, relying on physical isolation of
unflushed pages rather than shared MVCC state.

This spec defines the work to make AeroDB support **multiple simultaneously live
transactions** sharing one storage backend, with snapshot isolation enforced
through MVCC visibility and first-committer-wins conflict resolution.

## 2. Goals

- N concurrently live transactions in one process, each with its own snapshot.
- Snapshot isolation: each transaction reads a consistent snapshot taken at
  BEGIN; it never sees another transaction's uncommitted or later-committed work.
- Writers do not block readers; readers do not block writers (MVCC).
- Write/write conflicts resolved by first-committer-wins: the losing transaction
  aborts with a `WriteConflict` error.
- Correct, durable abort semantics that work even when uncommitted versions have
  reached disk (logical abort, not physical page revert).
- Thread-safe access to shared storage (the engine may be driven from multiple
  threads / connections).

## 3. Non-goals (explicitly out of scope for this spec)

- Serializable / SSI isolation (separate future spec). Snapshot isolation only.
- `SET TRANSACTION ISOLATION LEVEL` SQL syntax.
- Distributed transactions, replication, MVCC across processes.
- Auto-vacuum scheduling (manual `vacuum_table` stays; a background trigger is a
  follow-up).
- Schema/catalog snapshot isolation. DDL keeps its current transactional-but-not-
  versioned behavior (a committed schema change is visible to handles/txns that
  take their snapshot afterward). Concurrent DDL vs DML on the same table is
  serialized by a coarse catalog lock (see §6.4).

## 4. Current state (what exists, with anchors)

- `src/transaction/state.rs` — `Snapshot { xmin, xmax, active_tx_ids,
  current_tx_id }`, `IsolationLevel::Snapshot`, `TransactionState` (single
  transaction held by the pager), `TransactionMode`.
- `src/transaction/tx_table.rs` — `TransactionTable = HashMap<TransactionId,
  TransactionStatus>` with `Active | Committed(ts) | Aborted`.
- `src/transaction/mvcc.rs` — `is_visible` + `committed_before_snapshot`
  (frozen rule: absent-below-bound = committed; explicit `Aborted`/`Active`
  excluded). **Already correct for concurrent tx_table content.**
- `src/transaction/manager.rs` — `TransactionManager` tracks only a `mode`
  (None/Implicit/Explicit); one transaction at a time. Owns BEGIN/COMMIT/ROLLBACK
  routing.
- `src/storage/pager.rs` — owns a single `transaction: TransactionState`, a
  single `dirty_pages: DirtyPageSet`, the shared `tx_table`, the durable
  `next_transaction_id` (page-0 meta, `persist_meta`/`read_meta_page`), and
  `next_commit_ts`. `commit_transaction` writes dirty pages then persists meta;
  `rollback_transaction` **physically reverts** dirty pages from disk.
- `src/storage/btree.rs` — `find_visible` / `scan_visible` / `find_latest_logical`
  clone `pager.transaction_table()`; `has_write_conflict` (bounds-based);
  `vacuum_deleted_versions`.
- `src/catalog/mod.rs` — holds `active_tx_ids: Vec<TransactionId>`, delegates id
  allocation to the pager, `begin/commit/rollback_transaction`,
  `recheck_constraints_for_commit`, `global_xmin`, index-snapshot restore on
  rollback.
- `src/engine/mod.rs` — `Engine { catalog, transaction_manager }`, single-owner.

### The three hard constraints today

1. **Single transaction slot.** `Pager.transaction` and `Pager.dirty_pages` are
   singular. Concurrency needs per-transaction write state.
2. **Physical abort.** `rollback_transaction` restores page images. With shared
   pages and concurrent writers this would clobber others' committed/uncommitted
   work. Abort must become **logical**.
3. **No durable clog past commit.** The WAL is truncated on commit, so committed
   status is not durably retained; the visibility "frozen rule" treats absent tx
   ids as committed. That is safe only because aborted work is physically
   reverted today. Once abort is logical, an aborted tx's status MUST remain
   durably recorded until its versions are vacuumed — otherwise absent→committed
   would resurrect aborted rows.

## 5. Target architecture

Split responsibilities into **shared storage** vs **per-transaction session**.

```
            ┌─────────────────────────────────────────────┐
            │              Shared (one instance)           │
            │  ┌────────────┐  ┌───────────────────────┐   │
            │  │  Pager /   │  │  TransactionManager    │   │
            │  │ BufferPool │  │  - id allocator (dur.) │   │
            │  │ (page I/O, │  │  - clog (durable)      │   │
            │  │  WAL)      │  │  - active-tx set       │   │
            │  └────────────┘  │  - global_xmin()       │   │
            │  ┌────────────┐  └───────────────────────┘   │
            │  │  Catalog   │  (schema; coarse-locked)      │
            │  └────────────┘                               │
            └───────────────▲───────────────▲──────────────┘
                            │               │
                 ┌──────────┴───┐   ┌───────┴──────────┐
                 │ Transaction  │   │  Transaction     │  … N sessions
                 │  - id, snap  │   │   - id, snap     │
                 │  - write set │   │   - write set    │
                 │  - isolation │   │   - isolation    │
                 └──────────────┘   └──────────────────┘
```

### 5.1 `TransactionManager` (rewritten, shared, thread-safe)

Owns the global transaction bookkeeping currently split across pager + catalog:

- `next_transaction_id` (durable via page-0 meta — move `persist_meta` ownership
  here or keep in pager but drive it from the manager).
- `next_commit_ts`.
- `active: BTreeSet<TransactionId>` — currently-live transactions (replaces
  `Catalog.active_tx_ids` and `Pager.transaction`).
- `clog: durable transaction status store` (see §5.4).

API:
- `begin(isolation) -> Transaction` — allocate id, capture snapshot
  `{ xmax = next_transaction_id, active_tx_ids = active.clone(), current = id }`,
  insert id into `active`, record `Active` in clog.
- `commit(tx) -> Result<()>` — run conflict validation (§6.3) + constraint
  recheck; assign commit ts; write clog `Committed(ts)`; flush the tx's dirty
  pages; remove from `active`; persist meta.
- `abort(tx)` — write clog `Aborted`; remove from `active`; **do not** revert
  pages. Leave versions for vacuum.
- `snapshot_status(tx_id) -> TransactionStatus` and `active_snapshot()` for
  visibility and `global_xmin`.
- `global_xmin() = active.iter().min().unwrap_or(next_transaction_id)`.

Concurrency: guard shared state with a `Mutex` (or `RwLock` where reads dominate).
Keep the critical sections tiny (id allocation, active-set mutation, clog write).

### 5.2 `Transaction` (new per-session type)

Replaces the single `Pager.transaction`/`dirty_pages`:

- `id: TransactionId`
- `snapshot: Snapshot`
- `isolation: IsolationLevel`
- `write_set: {table_root -> set of (key, version)}` or a private
  `DirtyPageSet` — the pages/versions this transaction created, used for
  commit-time flush and conflict bookkeeping.
- `undo/among`: not needed if abort is logical (§6.2).

The engine exposes one `Transaction` per connection/session. All reads/writes
take `&Transaction` (for the snapshot + id) plus `&SharedStorage`.

### 5.3 Uncommitted writes: version-in-place, logical abort (Postgres-style)

**Decision: write new versions directly into the shared B-Tree pages**, stamped
with `created_tx = tx.id` (whose clog status is `Active`). Visibility already
hides Active-creator versions from everyone else (`is_visible` →
`committed_before_snapshot` returns false for `Active`). This reuses the existing
version-chain machinery instead of inventing private buffers.

Consequences:
- **Abort is logical**: mark the tx `Aborted` in the clog; its versions become
  permanently invisible and are reclaimed by `vacuum_deleted_versions`
  (extend vacuum to also drop versions whose *creator* aborted, not only
  committed-deleted versions — see §6.5).
- Uncommitted versions may be flushed to disk by buffer eviction; that is safe
  because visibility consults the clog. This is exactly why Part A made
  visibility authoritative.
- Remove/replace `Pager.rollback_transaction`'s physical page revert.

### 5.4 Durable commit log (clog)

The visibility "frozen rule" treats a tx id that is *absent* from the table and
below the snapshot bound as committed. With logical abort this is only safe if
**aborted (and still-relevant committed) statuses are retained durably until the
corresponding versions are frozen/vacuumed.**

Requirements:
- A durable status store keyed by tx id: `Active`(implicit/crash→Aborted),
  `Committed(ts)`, `Aborted`.
- Survives commit (must NOT be truncated with the WAL like today).
- Truncatable: statuses below `global_xmin` whose versions have been vacuumed can
  be dropped (frozen rule then safely treats them as committed).
- Crash recovery: any `Active` at startup → `Aborted` (already done in
  `Wal::recover_internal`); persist that into the clog.

Implementation options (pick during design):
- (a) Dedicated clog pages in the DB file (bitmap/array of 2-bit states like
  Postgres CLOG), indexed by tx id; page-0 meta records the clog extent +
  frozen watermark.
- (b) Keep the in-memory `tx_table` authoritative during a run, and persist a
  compact status log appended on each commit/abort, replayed at startup, and
  compacted at vacuum. Lighter but needs its own file/section.

Option (a) is the more standard, scalable choice; (b) is a smaller step. Recommend
(a) with a `frozen_xid` watermark below which all ids are treated committed.

### 5.5 Buffer pool / page access

- Pager becomes a shared, latched buffer pool: per-page latches (short-lived
  read/write) for physical consistency of concurrent page mutations; MVCC handles
  logical isolation.
- `DirtyPageSet` moves out of the pager's single-transaction field; dirty pages
  are tracked per transaction (for commit flush) while the buffer pool tracks
  physical residency.
- WAL: append page images + tx status records as today, but the commit protocol
  becomes: log tx's page images → log `Commit(ts)` to WAL + clog → flush → (no
  truncate-on-every-commit; checkpoint-driven truncation instead, since the clog
  must persist).

## 6. Correctness model

### 6.1 Snapshot capture
At BEGIN: `xmax = next_transaction_id`, `active_tx_ids = manager.active` (sorted),
`current_tx_id = id`. Reuse `Snapshot::new_for_transaction`. Unchanged semantics;
the only difference is `active_tx_ids` now reflects genuinely concurrent txns.

### 6.2 Visibility
`is_visible` + `committed_before_snapshot` are **unchanged** — they already model
concurrent statuses correctly (this is the payoff of Part A). Verify with the
existing matrix plus new concurrent integration tests.

### 6.3 Write/write conflict (first-committer-wins)
On UPDATE/DELETE of a key, call `has_write_conflict(key, visible_created_tx,
snapshot)`. Detect conflict when another transaction, not visible to this
snapshot, has created a newer version or deleted the visible version. Because
concurrent writers may commit between check and commit, **re-validate at commit**:
for each key in the tx's write set, ensure no other transaction committed a
conflicting change after this tx's snapshot. Losing tx → `abort` +
`DbError::WriteConflict`.

Optional hardening: a per-key in-memory write-intent lock so the second concurrent
writer fails fast instead of at commit.

### 6.4 Catalog / DDL serialization
DDL takes an exclusive catalog lock for its duration; DML takes a shared catalog
lock (schema read). This prevents a table being dropped mid-scan. DDL remains
transactional (commit/rollback with the tx) and non-versioned. `pre_tx_indexes`
snapshot/restore stays.

### 6.5 Vacuum
Extend `deleted_version_is_removable` / `vacuum_deleted_versions` to reclaim:
1. versions deleted by a committed tx below `global_xmin` (today), **and**
2. versions whose **creator aborted** (new — needed because abort is logical).
After a version's tx statuses are below `frozen_xid` and vacuumed, the clog entry
may be dropped.

### 6.6 Invariants
- A committed transaction's effects are all-or-nothing (atomic clog flip).
- No transaction observes another's uncommitted or post-snapshot committed work.
- An aborted transaction's versions are never visible to anyone, ever.
- `global_xmin` never advances past a live snapshot; vacuum never removes a
  version visible to any live snapshot.
- tx ids are strictly monotonic and durable (already true post Part B).

## 7. Implementation phases

Each phase should compile, pass existing tests, and add its own.

1. **Extract shared state.** Introduce a `SharedStorage`/rework `Pager` so the
   single `transaction`/`dirty_pages` fields move into a new per-session
   `Transaction`. Keep single-threaded behavior; no concurrency yet. Green tests.
2. **Rewrite `TransactionManager`** to own the active set + id allocator +
   commit-ts, exposing `begin/commit/abort` returning/consuming `Transaction`.
   Route `Engine` through it. Still one session, but state relocated.
3. **Durable clog (§5.4).** Replace reliance on WAL-truncation-safe status; stop
   truncating status on commit; add recovery + frozen watermark. Update page-0
   meta to record clog extent + `frozen_xid`.
4. **Logical abort.** Remove physical page revert; mark aborted in clog; extend
   vacuum to reclaim aborted-creator versions. Update `mvcc_rollback` tests to the
   new mechanism (behavioral outcome identical: aborted work invisible).
5. **Multi-session + latching.** Allow N `Transaction`s; add buffer-pool page
   latches; make manager state thread-safe. Commit-time conflict re-validation.
6. **Concurrent test suite (§8).** Real two-transaction interleavings in one
   process.

Phases 1–2 are pure refactors (low risk). 3–4 change durability semantics (high
risk — needs crash tests). 5 introduces concurrency (highest risk — needs stress
tests).

## 8. Testing

Replace the two-handle "fake concurrency" pattern with genuine interleavings.

- **Snapshot isolation:** T1 BEGIN, T2 BEGIN; T1 INSERT/COMMIT; assert T2 (older
  snapshot) does NOT see T1's row; a T3 begun after sees it.
- **Read stability:** T1 reads key; T2 updates+commits; T1 re-reads → same value
  (its snapshot).
- **Write/write conflict:** T1, T2 both update key K; first commit wins, second
  gets `WriteConflict`.
- **Abort invisibility across the flush boundary:** force buffer eviction of an
  uncommitted version, abort, assert never visible; assert vacuum reclaims it.
- **Crash recovery:** kill after commit-logged-but-before-checkpoint; after
  restart the committed tx is durable and aborted/in-flight are gone; clog
  consistent.
- **tx-id + clog durability across reopen** (extends `mvcc_tx_id_persistence`).
- **Vacuum with a live snapshot:** old versions needed by a live T are retained;
  reclaimed only after it ends.
- Keep the existing `mvcc_visibility_matrix` unit tests as the visibility oracle.
- Add stress/property tests: random interleavings of begin/insert/update/delete/
  commit/abort across K threads; assert no torn reads, no lost committed writes,
  no visible aborted writes.

## 9. Risks & mitigations

- **Durability regressions** from stopping commit-time WAL truncation → gate with
  crash-recovery tests before/after each phase 3–4 change.
- **Deadlocks/latch ordering** in the buffer pool → define a strict latch
  acquisition order (page number ascending) and keep latches short.
- **Clog growth** if vacuum lags → frozen watermark + compaction; document that
  vacuum is required to bound clog size (until auto-vacuum lands).
- **Performance** of per-key commit-time conflict re-validation → optional write-
  intent locks for fail-fast; measure before optimizing.
- **Scope creep into Serializable** → explicitly deferred; keep the isolation
  enum extensible (it already is) but implement Snapshot only.

## 10. Acceptance criteria

- ✅ Multiple transactions live simultaneously in one process, each snapshot-
  isolated, validated by the §8 concurrent suite (`tests/mvcc_concurrent.rs`).
- ✅ First-committer-wins conflicts produce `WriteConflict` for the loser.
- ✅ Aborted work is never visible and is reclaimable by vacuum, including across
  a disk-flush boundary (`tests/mvcc_logical_abort.rs`, `tests/mvcc_clog.rs`).
- ✅ All pre-existing `tests/mvcc_*` suites still pass (visibility matrix
  unchanged). Full suite: 202 tests across 38 suites.
- ✅ No physical page-revert remains in the *data* abort path (only the
  non-versioned, lock-serialized schema catalog pages 1,2 are reverted).

## 11. Deferred: per-page latching

Phase 5 guards physical storage access with a single `Mutex` (`src/db.rs`).
This is correct and thread-safe (MVCC gives logical isolation; the latch
serializes physical page access), and passes the multi-threaded stress test.
It does **not** meet the §2 goal that writers not block readers: with one latch
they do. Delivering per-page latches (§5.5) requires reworking `BTree`'s page
access — which today holds `&mut Page` guards across multi-page operations — into
a buffer-pool guard model. That is a large, self-contained follow-up.
```
