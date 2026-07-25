# Spec: MVCC follow-ups — clog compaction + per-page latching

Status: **Draft — plan for a follow-up session**
Depends on: `specs/mvcc-concurrent-transactions.md` phases 1–5 (all merged).
Scope: two independent follow-ups. **Phase 6** is small and low-risk (do first).
**Phase 7** is the large per-page-latching rewrite (§11 of the parent spec).

Each numbered step must compile, keep the existing suite green (202 tests / 38
suites at time of writing), and add its own tests before moving on.

---

## Phase 6 — vacuum-driven `frozen_xid` advancement + clog compaction

### Problem
`frozen_xid` is persisted in page-0 meta (`Pager`, bytes 24..32) but is **never
advanced**, so the durable clog (`<db>.clog`) grows without bound: every
transaction id keeps a 2-bit status forever. Vacuum reclaims dead *versions* but
never reclaims the *status entries* of transactions whose versions are all gone.

### Goal
After a vacuum pass, advance `frozen_xid` to a safe watermark and drop clog
entries below it. Below `frozen_xid` every id is treated as committed (the
"frozen rule" already treats an *absent* id below the snapshot bound as
committed), so clearing those entries is safe once no live snapshot and no
remaining on-disk version depends on them being distinguishable.

### Current-state anchors
- `src/storage/pager.rs`: `frozen_xid` field; `read_meta_page` /`persist_meta`
  (meta bytes 24..32); `commit_transaction` calls `persist_meta`.
- `src/transaction/clog.rs`: `Clog` — packed 2-bit array, `byte_offset(tx_id) =
  HEADER_LEN + tx_id/4`, `record`, `load`, `reset`.
- `src/transaction/manager.rs`: `global_xmin(&Catalog)`; `commit`/`rollback`.
- `src/catalog/mod.rs`: `vacuum_table(name, global_xmin)`.
- `src/engine/mod.rs` `Engine::vacuum_table`, `src/db.rs` `Database::vacuum_table`
  both compute `global_xmin` from the manager, then call `catalog.vacuum_table`.
- `src/transaction/mvcc.rs`: `committed_before_snapshot` — treats absent-below-
  bound ids as committed. Note `COMMITTED_BOOTSTRAP_TX = 0`.

### Safe watermark
The advance target is **`min(global_xmin, oldest live snapshot xmin)`** — in
practice `global_xmin` already equals the oldest live transaction id (or
`next_transaction_id` when none are live). A status entry for `tx_id` may be
dropped only when:
1. `tx_id < global_xmin` (no live snapshot can still be evaluating it), **and**
2. every version created-or-deleted by `tx_id` has been vacuumed (so no on-disk
   version still needs `tx_id`'s status to be seen as aborted/committed).

Condition (2) is the subtle one. The current single-table `vacuum_table` cannot
know about other tables. Two safe options:

- **Option A (conservative, recommended first):** only advance `frozen_xid`
  after a vacuum that swept **all** tables in one pass. Add
  `Catalog::vacuum_all()` (loops every table, same `global_xmin`), and advance
  `frozen_xid = global_xmin` only from that all-tables entry point. Per-table
  `vacuum_table` does **not** advance the watermark.
- **Option B (later):** track, per tx id, whether any unvacuumed version
  references it; advance lazily. More bookkeeping; defer.

### Steps
1. **Clog compaction primitive.** Add `Clog::compact(below: TransactionId)`:
   clear (set to `ST_UNKNOWN`) every 2-bit slot for `tx_id < below`, fsync. This
   does not shrink the file (the array is absolute-indexed) but makes those
   entries read back as absent → committed. Add a unit test: record
   Committed/Aborted for ids 1..5, `compact(4)`, assert `load()` returns only
   ids ≥ 4, aborted/committed below 4 are gone.
   - *Optional (nice-to-have):* rebase the array to a stored base offset =
     `frozen_xid` so the file can actually shrink. Larger change; skip unless
     file size matters. Document the choice.
2. **Pager plumbing.** Add `Pager::advance_frozen_xid(&mut self, watermark)`:
   `if watermark > self.frozen_xid { self.frozen_xid = watermark;
   self.clog.compact(watermark)?; self.persist_meta()?; }`. Expose the current
   value via `Pager::frozen_xid()`.
3. **All-tables vacuum.** Add `Catalog::vacuum_all(global_xmin) -> VacuumReport`
   (sum per-table reports). After sweeping every table, call
   `pager.advance_frozen_xid(global_xmin)`.
4. **Wire entry points.** `Engine::vacuum_all()` and `Database::vacuum_all()`
   compute `global_xmin` from the manager and call `catalog.vacuum_all`. Keep the
   existing per-table `vacuum_table` (no watermark advance) for targeted use.
5. **Visibility check.** Confirm `committed_before_snapshot` needs **no** change:
   a cleared (absent) id below the bound already reads as committed. Add an
   assertion/comment. Only thread `frozen_xid` into `mvcc.rs` if a case is found
   where an absent id must be forced committed *above* the snapshot bound (should
   not happen — do not add unless a test proves it).

### Tests (add `tests/mvcc_frozen_xid.rs`)
- After committing + deleting rows and running `vacuum_all`, reopen and assert
  the clog file no longer reports the reclaimed txns (load smaller), and rows
  visible/reclaimed exactly as before (no behavior change).
- Watermark never passes a live snapshot: begin a long-lived transaction, run
  `vacuum_all`, assert `frozen_xid` did not advance past that tx's id, and its
  snapshot still reads correctly.
- `frozen_xid` persists across reopen (extends `mvcc_tx_id_persistence`).

### Risks
- Advancing past a live snapshot → wrongly freezing an aborted id as committed.
  Guard strictly with `global_xmin` (which never passes the oldest live tx).
- Multi-table safety: only advance from `vacuum_all`, never per-table.

Estimated size: small (~150–250 lines + tests).

---

## Phase 7 — per-page latching (real reader/writer concurrency)

### Problem
`src/db.rs` guards all physical access with one `Arc<Mutex<Core>>`. Correct and
thread-safe, but writers block readers — the §2 goal of the parent spec is unmet.
Root cause: `BTree<'a> { pager: &'a mut Pager }` and `get_page -> &mut Page`, with
`&mut Page` guards held across multi-page (recursive) operations, force exclusive
access to the whole pager.

### Goal
A latched buffer pool: short-lived per-page read/write latches for physical
consistency; MVCC continues to provide logical isolation. Remove the global
storage mutex; readers and writers proceed in parallel except when touching the
same page.

### Target shape
```
BufferPool {                          // interior mutability, shared &self
    frames: Mutex<HashMap<u32, Arc<Frame>>>,   // or a fixed frame table + map
    file/wal/clog: Mutex<...>,                 // I/O serialized separately
}
Frame { latch: RwLock<Page>, ... }
pool.read(page_num)  -> PageReadGuard   (RwLockReadGuard)
pool.write(page_num) -> PageWriteGuard  (RwLockWriteGuard)
```
`Catalog` schema behind `RwLock` (DDL = write lock, DML = read lock — realizes
§6.4). `TransactionManager` behind its own `Mutex`. `Database` holds these
separately instead of one `Mutex<Core>`.

### The hard part: BTree rewrite
`btree.rs` (~1300 lines) accesses pages as `self.pager.get_page(n)? -> &mut Page`
and frequently:
- reads a page, then reads another (parent/child) — would hold two latches;
- copies page bytes into a local `buf` then mutates another page;
- holds a `&mut Page` across a recursive call (`insert_in_parent`, `split_*`).

Convert every access to: acquire latch → operate on the guard → **release before
descending/ascending** (copy out the bytes you need first). This removes
long-held cross-page borrows. It is mechanical but pervasive and error-prone.

### Latch-ordering rule (deadlock avoidance)
Acquire page latches in **ascending page-number order**; never hold a latch while
blocking to acquire a lower-numbered one. For splits that allocate a new
(higher-numbered) page this is natural. Document and assert the rule in debug
builds where feasible.

### Steps (each compiles + green)
1. **Interior-mutability pager, single-threaded first.** Change `Pager::get_page`
   to `&self` returning a guard (back it with `RefCell`/`RwLock` per page) WITHOUT
   yet adding threads. Rewrite `BTree` to take `&Pager` (shared) and use guards.
   This is the bulk of the work; keep `Database`'s single `Mutex` for now so
   semantics are unchanged. Green the whole suite here — highest-risk step.
2. **Per-page RwLock frames + latch ordering.** Replace the coarse per-page cell
   with `RwLock<Page>` frames; enforce ascending acquisition. I/O (file/wal/clog)
   stays behind its own small mutex. Still under the outer `Database` mutex.
3. **Split the Database lock.** Replace `Mutex<Core>` with: buffer pool (internal
   locks), `RwLock<Catalog schema>`, `Mutex<TransactionManager>`. DML takes the
   catalog read lock + page latches; DDL takes the catalog write lock. The
   manager mutex covers begin/commit/abort bookkeeping only (keep critical
   sections tiny per §5.1).
4. **Commit/flush under concurrency.** `commit` must WAL-log + flush the tx's
   touched pages while other txns may be latching them; take page read latches to
   snapshot images for the WAL, keep the manager/commit-ts critical section
   short. Re-check the write-intent `table_root` staleness noted in the parent
   spec (root may split concurrently) — resolve `table_root` at commit from the
   catalog under the read lock.
5. **Eviction (optional).** If a real frame cap is added, evict only unlatched,
   clean frames; never evict a page a live tx still needs unflushed. Until then,
   keep the grow-only cache.

### Tests
- Keep all existing suites green after **each** step (esp. after step 1).
- `tests/mvcc_concurrent.rs` stress test: raise threads/iters; assert same
  invariants (no lost/torn/aborted-visible state).
- Add a **parallel-progress** test: one long reader transaction runs while many
  writers commit to *other* keys — assert the reader is not blocked to completion
  and its snapshot stays stable (demonstrates readers don't block writers).
- Add a **deadlock guard** test: many threads updating overlapping key sets;
  bounded completion time (latch ordering prevents deadlock).

### Risks
- **Deadlock** from inconsistent latch order → strict ascending order; short
  latches; never latch-then-block-lower.
- **Borrow rewrite regressions** in `btree.rs` → do step 1 in isolation, lean on
  the existing btree unit tests + full suite; consider a temporary debug
  assertion that no two latches are held in descending order.
- **Commit durability under concurrency** → keep the WAL append → commit record →
  clog Committed → flush → truncate ordering; verify with crash/reopen tests.
- Scope creep — do NOT attempt Serializable/SSI here (parent spec non-goal).

Estimated size: large (rewrite of `btree.rs` page access + `pager.rs` + `db.rs`;
multi-session, multi-step, needs stress + deadlock tests).

### Order
Do **Phase 6 first** (small, independent, bounds clog growth). Then Phase 7,
strictly step-by-step with a green suite between steps — step 1 (interior
mutability + BTree guard rewrite) is the make-or-break checkpoint.
