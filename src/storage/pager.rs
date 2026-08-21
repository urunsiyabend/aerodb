use crate::storage::page::PAGE_SIZE;
use crate::transaction::{
    IsolationLevel, Snapshot, Transaction, TransactionId, TransactionStatus, TransactionTable,
    WriteIntent, clog::Clog, wal::Wal,
};
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

/// Magic marking page 0 as the durable engine meta page. Older databases leave
/// page 0 zeroed (allocated but unused), so a mismatch means "no meta yet".
const META_MAGIC: &[u8; 8] = b"AERODBM1";

/// Fixed pages holding the non-versioned schema catalog: page 1 is the table
/// catalog, page 2 the sequence catalog. Their scans are not MVCC-filtered, so
/// [`Pager::rollback_transaction`] physically reverts just these pages on abort;
/// see the logical-abort note there.
const CATALOG_TABLE_PAGE: u32 = 1;
const CATALOG_SEQUENCE_PAGE: u32 = 2;

/// The raw bytes of one 4 KiB page. Callers never hold a page reference across
/// an unrelated operation: page access goes through the latched
/// [`Pager::with_page`] / [`Pager::with_page_mut`] closures, which release the
/// latch as soon as the closure returns.
pub type PageData = [u8; PAGE_SIZE];

/// One cached page, latched independently of every other page.
///
/// This is what makes concurrent access possible: physical consistency of a
/// single page is enforced by its own `RwLock` (a *latch*, held for the duration
/// of one read or mutation), while logical isolation between transactions
/// remains the job of MVCC visibility. Two transactions touching different pages
/// never contend.
struct Frame {
    data: RwLock<PageData>,
}

/// Serialized I/O resources. Kept behind their own mutex so page latches never
/// have to wait on disk work being done for an unrelated page.
struct PagerIo {
    file: File,
    wal: Wal,
    /// Durable transaction-status store. Survives commit (unlike the WAL) so
    /// aborted/committed statuses remain available to MVCC visibility and vacuum
    /// across reopen.
    clog: Clog,
    /// The number of pages that already existed on disk (grows as new pages are
    /// written out).
    file_length_pages: u32,
}

impl PagerIo {
    fn write_page_raw(&mut self, page_num: u32, data: &PageData) -> io::Result<()> {
        let offset = (page_num as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        self.file.flush()?;
        if page_num >= self.file_length_pages {
            self.file_length_pages = page_num + 1;
        }
        Ok(())
    }

    fn read_page_raw(&mut self, page_num: u32, data: &mut PageData) -> io::Result<()> {
        let offset = (page_num as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(data)
    }
}

/// The durable engine counters, kept together so allocation and persistence see
/// a consistent set.
struct Counters {
    next_transaction_id: TransactionId,
    next_commit_ts: u64,
    /// Watermark below which every transaction id is treated as committed
    /// (its clog entry may be dropped). Persisted in page-0 meta; advanced by
    /// vacuum once the corresponding versions are reclaimed.
    frozen_xid: TransactionId,
}

/// Pager: manages reading/writing 4 KiB pages from/into the database file, and
/// keeps an in-memory buffer pool of latched frames. Distinguishes pages already
/// on disk from pages newly allocated in memory.
///
/// Every method takes `&self`: the pager is an interior-mutability, `Sync`
/// buffer pool rather than an exclusively-owned object. That is what lets
/// several transactions operate on the shared storage at once, contending only
/// on the individual pages they touch.
///
/// Lock order (never acquire upwards):
/// `transaction` -> `commit` -> `counters` -> `tx_table` -> `frames` -> page
/// latch -> `io`. Page latches themselves are taken in ascending page-number
/// order, which is asserted in debug builds; see [`LatchOrderGuard`].
pub struct Pager {
    io: Mutex<PagerIo>,

    /// The buffer pool: `frames[page_num] = Some(frame)` if that page is
    /// resident. Grow-only — frames are never evicted, so a page a live
    /// transaction still needs unflushed can never disappear from under it.
    frames: RwLock<Vec<Option<Arc<Frame>>>>,

    /// The total number of pages the pager knows about right now (on-disk plus
    /// newly allocated). Atomic so allocation does not need the frame table.
    num_pages: AtomicU32,

    /// The shared, authoritative transaction-status table. Read on every
    /// visibility check, so it gets its own lock and the shortest possible write
    /// critical sections.
    tx_table: RwLock<TransactionTable>,

    counters: Mutex<Counters>,

    /// Serializes the commit/abort durability sequence (WAL append -> commit
    /// record -> clog -> data pages -> truncate) without holding the
    /// `tx_table`/`counters` locks across the fsyncs.
    commit: Mutex<()>,

    /// The transaction currently installed for execution, if any. Still a single
    /// slot: several transactions may be *live* (held detached by the
    /// [`crate::transaction::TransactionManager`]), and one is installed here for
    /// the duration of each operation.
    transaction: Mutex<Option<Transaction>>,
}

thread_local! {
    /// Page numbers this thread currently holds latches on, innermost last.
    /// Debug-only bookkeeping for the ascending-order rule.
    static HELD_LATCHES: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
}

/// Debug-build enforcement of the latch-ordering rule.
///
/// Deadlock freedom rests on a single invariant: page latches are acquired in
/// **ascending page-number order**, and a thread never blocks for a lower-numbered
/// page while holding a higher-numbered one. Splits allocate higher-numbered
/// pages, so the natural direction of B-Tree work already satisfies it; the
/// rewritten access paths additionally copy the bytes they need and release the
/// latch before descending, so in practice at most one latch is held at a time.
struct LatchOrderGuard;

impl LatchOrderGuard {
    fn acquire(page_num: u32) -> Self {
        #[cfg(debug_assertions)]
        HELD_LATCHES.with(|held| {
            let mut held = held.borrow_mut();
            debug_assert!(
                held.last().map_or(true, |&innermost| page_num > innermost),
                "page latches must be taken in ascending order: tried {} while holding {:?}",
                page_num,
                held
            );
            held.push(page_num);
        });
        let _ = page_num;
        LatchOrderGuard
    }
}

impl Drop for LatchOrderGuard {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        HELD_LATCHES.with(|held| {
            held.borrow_mut().pop();
        });
    }
}

impl Pager {
    /// Open (or create) the database file at `filename`.
    /// - `file_length_pages` is set to floor(file_size / PAGE_SIZE).
    /// - `num_pages` is initially the same as `file_length_pages`.
    pub fn new(filename: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;
        let wal_path = format!("{}.wal", filename);
        let (wal, wal_tx_table) = Wal::open(&wal_path, &mut file)?;
        let mut next_commit_ts = wal_tx_table
            .values()
            .filter_map(|status| match status {
                TransactionStatus::Committed(commit_ts) => Some(*commit_ts),
                _ => None,
            })
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        // Determine file length after WAL recovery in case pages were replayed
        let file_len_after = file.metadata()?.len();
        let file_length_pages = (file_len_after as usize / PAGE_SIZE) as u32;

        // Restore the durable transaction-id counter, commit timestamp, and
        // frozen watermark from page 0. The WAL is truncated on commit, so these
        // counters cannot be derived from transaction-status records alone;
        // page 0 is their home.
        let meta = Pager::read_meta_page(&mut file, file_length_pages)?;
        let mut next_transaction_id: TransactionId = 1;
        let mut frozen_xid: TransactionId = 0;

        let clog_path = format!("{}.clog", filename);
        let mut clog = Clog::open(&clog_path)?;

        let tx_table = if let Some((persisted_tx_id, persisted_commit_ts, persisted_frozen)) = meta {
            next_transaction_id = persisted_tx_id.max(1);
            next_commit_ts = next_commit_ts.max(persisted_commit_ts);
            frozen_xid = persisted_frozen;

            // The clog is the durable status store; the WAL holds the crash-truth
            // for any transaction that was in-flight at the last (unclean) exit.
            // Load the clog, then overlay the WAL-recovered statuses and persist
            // those deltas so the clog reflects the recovered outcome.
            let mut table = clog.load()?;
            for (tx_id, status) in &wal_tx_table {
                table.insert(*tx_id, *status);
                clog.record(*tx_id, *status)?;
            }
            table
        } else {
            // No durable meta means a fresh database (ids restart at 1). A clog
            // sidecar left over from a previous database at this path would map
            // stale statuses onto the new ids, so discard it.
            clog.reset()?;
            wal_tx_table
        };

        Ok(Pager {
            io: Mutex::new(PagerIo {
                file,
                wal,
                clog,
                file_length_pages,
            }),
            frames: RwLock::new(Vec::new()),
            num_pages: AtomicU32::new(file_length_pages),
            tx_table: RwLock::new(tx_table),
            counters: Mutex::new(Counters {
                next_transaction_id,
                next_commit_ts,
                frozen_xid,
            }),
            commit: Mutex::new(()),
            transaction: Mutex::new(None),
        })
    }

    fn io(&self) -> MutexGuard<'_, PagerIo> {
        self.io.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn counters(&self) -> MutexGuard<'_, Counters> {
        self.counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Resolve (loading on demand) the frame for `page_num`.
    ///
    /// Allocates blank pages up to `page_num` if it is past the end, reads the
    /// page from disk if it was already there, and leaves a brand-new page
    /// zeroed. The returned `Arc` keeps the frame alive independently of the
    /// frame table, so the caller can latch it after releasing the table lock.
    fn frame(&self, page_num: u32) -> io::Result<Arc<Frame>> {
        // Extend the logical page count if the caller asked past the end.
        let mut known = self.num_pages.load(Ordering::SeqCst);
        while page_num >= known {
            match self.num_pages.compare_exchange(
                known,
                page_num + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => known = actual,
            }
        }

        if let Some(frame) = self.resident_frame(page_num) {
            return Ok(frame);
        }

        let mut frames = self
            .frames
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if frames.len() <= page_num as usize {
            frames.resize_with(page_num as usize + 1, || None);
        }
        // Another thread may have installed the frame while we waited.
        if let Some(frame) = &frames[page_num as usize] {
            return Ok(Arc::clone(frame));
        }

        let mut data: PageData = [0; PAGE_SIZE];
        {
            let mut io = self.io();
            if page_num < io.file_length_pages {
                io.read_page_raw(page_num, &mut data)?;
            }
        }

        let frame = Arc::new(Frame {
            data: RwLock::new(data),
        });
        frames[page_num as usize] = Some(Arc::clone(&frame));
        Ok(frame)
    }

    /// The frame for `page_num` if it is already resident, without loading it.
    fn resident_frame(&self, page_num: u32) -> Option<Arc<Frame>> {
        let frames = self
            .frames
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        frames
            .get(page_num as usize)
            .and_then(|slot| slot.as_ref())
            .map(Arc::clone)
    }

    /// Run `f` against `page_num` under a **shared** page latch, releasing it as
    /// soon as `f` returns. Copy out whatever you need instead of holding the
    /// latch across a descent into another page.
    pub fn with_page<R>(&self, page_num: u32, f: impl FnOnce(&PageData) -> R) -> io::Result<R> {
        let frame = self.frame(page_num)?;
        let _order = LatchOrderGuard::acquire(page_num);
        let guard = frame
            .data
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(f(&guard))
    }

    /// Run `f` against `page_num` under an **exclusive** page latch, releasing it
    /// as soon as `f` returns.
    pub fn with_page_mut<R>(
        &self,
        page_num: u32,
        f: impl FnOnce(&mut PageData) -> R,
    ) -> io::Result<R> {
        let frame = self.frame(page_num)?;
        let _order = LatchOrderGuard::acquire(page_num);
        let mut guard = frame
            .data
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(f(&mut guard))
    }

    /// Copy the current image of `page_num`. This is the normal way to inspect a
    /// page: the latch is dropped before the caller looks at the bytes, so no
    /// latch is held while descending the tree.
    pub fn read_page(&self, page_num: u32) -> io::Result<PageData> {
        self.with_page(page_num, |data| *data)
    }

    /// Overwrite the whole cached image of `page_num`.
    pub fn write_page(&self, page_num: u32, data: &PageData) -> io::Result<()> {
        self.with_page_mut(page_num, |dst| dst.copy_from_slice(data))
    }

    /// Allocate a new page at the end (in memory). Increments `num_pages`.
    /// Does NOT change `file_length_pages` until we actually flush it.
    pub fn allocate_page(&self) -> io::Result<u32> {
        Ok(self.num_pages.fetch_add(1, Ordering::SeqCst))
    }

    /// Persist the cached image of `page_num`.
    ///
    /// Inside a transaction this only records the page as touched: under
    /// version-in-place the mutation already sits in the shared frame, and its
    /// current image is written at commit. Outside a transaction (catalog
    /// bootstrap, vacuum) the page goes straight to the WAL and the file.
    /// A page that is not resident is skipped — there is no image to write.
    pub fn flush_page(&self, page_num: u32) -> io::Result<()> {
        {
            let mut installed = self.transaction();
            if let Some(transaction) = installed.as_mut() {
                transaction.mark_touched(page_num);
                return Ok(());
            }
        }

        let Some(data) = self.resident_image(page_num) else {
            return Ok(());
        };
        let mut io = self.io();
        io.wal.append_page(page_num, &data)?;
        io.write_page_raw(page_num, &data)?;
        Ok(())
    }

    /// Copy the current image of `page_num`, if the page is resident. The latch
    /// is released before returning, so callers can then take the `io` lock.
    fn resident_image(&self, page_num: u32) -> Option<PageData> {
        let frame = self.resident_frame(page_num)?;
        let _order = LatchOrderGuard::acquire(page_num);
        let guard = frame
            .data
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Some(*guard)
    }

    /// How many pages were already in the file when we opened it?
    pub fn file_length_pages(&self) -> u32 {
        self.io().file_length_pages
    }

    /// How many pages does the pager know about right now (on-disk + newly allocated)?
    pub fn num_pages(&self) -> u32 {
        self.num_pages.load(Ordering::SeqCst)
    }

    fn transaction(&self) -> MutexGuard<'_, Option<Transaction>> {
        self.transaction
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn transaction_active(&self) -> bool {
        self.transaction().is_some()
    }

    pub fn begin_transaction(
        &self,
        id: TransactionId,
        snapshot: Snapshot,
        name: Option<String>,
        isolation_level: IsolationLevel,
    ) -> io::Result<()> {
        *self.transaction() = Some(Transaction::new(id, snapshot, name, isolation_level));
        self.tx_table
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, TransactionStatus::Active);
        let mut io = self.io();
        io.wal.append_tx_status(id, TransactionStatus::Active)?;
        // Record the in-flight status durably too, so a crash leaves an Active
        // clog entry that recovery converts to Aborted.
        io.clog.record(id, TransactionStatus::Active)?;
        Ok(())
    }

    /// Install a previously-begun transaction as the currently-executing one.
    ///
    /// The pager has a single execution slot; with multiple live transactions
    /// the manager keeps them detached and installs one for the duration of each
    /// operation (operations are serialized by the shared-storage lock).
    pub fn install_transaction(&self, transaction: Transaction) {
        let mut slot = self.transaction();
        debug_assert!(slot.is_none(), "a transaction is already installed");
        *slot = Some(transaction);
    }

    /// Remove the currently-executing transaction so the manager can hold it
    /// between operations. Returns `None` if no transaction is installed.
    pub fn uninstall_transaction(&self) -> Option<Transaction> {
        self.transaction().take()
    }

    /// Record a write intent on the installed transaction for commit-time
    /// first-committer-wins re-validation. No-op outside a transaction.
    pub fn record_write_intent(
        &self,
        table_root: u32,
        key: i32,
        visible_created_tx: TransactionId,
    ) {
        if let Some(transaction) = self.transaction().as_mut() {
            transaction.record_write(WriteIntent {
                table_root,
                key,
                visible_created_tx,
            });
        }
    }

    /// The write intents recorded by the installed transaction.
    pub fn transaction_write_set(&self) -> Vec<WriteIntent> {
        self.transaction()
            .as_ref()
            .map(|transaction| transaction.write_set().to_vec())
            .unwrap_or_default()
    }

    pub fn transaction_id(&self) -> Option<TransactionId> {
        self.transaction().as_ref().map(Transaction::id)
    }

    /// The installed transaction's snapshot, cloned. The snapshot cannot be
    /// borrowed out: the slot lives behind a lock that must be released before
    /// the caller goes on to touch pages.
    pub fn transaction_snapshot(&self) -> Option<Snapshot> {
        self.transaction()
            .as_ref()
            .map(|transaction| transaction.snapshot().clone())
    }

    /// A consistent copy of the shared transaction-status table, for the
    /// visibility checks of one operation.
    pub fn transaction_table(&self) -> TransactionTable {
        self.tx_table
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Allocate the next monotonic transaction id. The counter is made durable
    /// by [`Pager::persist_meta`] on commit, so ids never collide with
    /// `created_tx`/`deleted_tx` values already written by earlier sessions.
    pub fn allocate_transaction_id(&self) -> TransactionId {
        let mut counters = self.counters();
        let id = counters.next_transaction_id;
        counters.next_transaction_id = counters.next_transaction_id.saturating_add(1);
        id
    }

    /// The id that the next `allocate_transaction_id` would hand out. Used as the
    /// snapshot `xmax` boundary once the current transaction's id is allocated.
    pub fn peek_next_transaction_id(&self) -> TransactionId {
        self.counters().next_transaction_id
    }

    /// The frozen watermark: every transaction id below it is unconditionally
    /// treated as committed, and its clog status has been reclaimed.
    pub fn frozen_xid(&self) -> TransactionId {
        self.counters().frozen_xid
    }

    /// Advance the frozen watermark to `watermark` and reclaim the clog statuses
    /// below it.
    ///
    /// Only safe once (a) no live snapshot can still be evaluating a transaction
    /// below `watermark` -- guaranteed by passing `global_xmin`, which never
    /// exceeds the oldest live transaction id -- and (b) every version created or
    /// deleted by those transactions has been vacuumed, so no on-disk version
    /// still needs their status to be seen as aborted. Condition (b) is why the
    /// only caller is the all-tables vacuum ([`crate::catalog::Catalog::vacuum_all`]):
    /// a single-table pass cannot know whether another table still holds versions
    /// belonging to those ids.
    ///
    /// Reclaiming a status is safe because MVCC visibility treats an *absent* id
    /// below the snapshot boundary as committed (the "frozen rule" in
    /// [`crate::transaction::mvcc`]), which is exactly the meaning the watermark
    /// assigns to it.
    pub fn advance_frozen_xid(&self, watermark: TransactionId) -> io::Result<()> {
        let (next_transaction_id, next_commit_ts) = {
            let mut counters = self.counters();
            if watermark <= counters.frozen_xid {
                return Ok(());
            }
            counters.frozen_xid = watermark;
            (counters.next_transaction_id, counters.next_commit_ts)
        };
        self.io().clog.compact(watermark)?;
        self.persist_meta(next_transaction_id, next_commit_ts, watermark)
    }

    /// Read the durable counters from page 0. Returns `None` when page 0 is
    /// missing or does not carry the meta magic (legacy databases).
    fn read_meta_page(
        file: &mut File,
        file_length_pages: u32,
    ) -> io::Result<Option<(TransactionId, u64, TransactionId)>> {
        if file_length_pages < 1 {
            return Ok(None);
        }
        let mut buf = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(0))?;
        if file.read_exact(&mut buf).is_err() || &buf[0..8] != META_MAGIC {
            return Ok(None);
        }
        let next_tx_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let next_commit_ts = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        // frozen_xid was added later; a database written before it left these
        // bytes zeroed, which correctly reads back as "nothing frozen yet".
        let frozen_xid = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        Ok(Some((next_tx_id, next_commit_ts, frozen_xid)))
    }

    /// Durably write the transaction-id, commit-timestamp, and frozen-watermark
    /// counters to page 0. Takes the values explicitly so the caller can release
    /// the counters lock before the fsync.
    fn persist_meta(
        &self,
        next_transaction_id: TransactionId,
        next_commit_ts: u64,
        frozen_xid: TransactionId,
    ) -> io::Result<()> {
        let mut buf = [0u8; PAGE_SIZE];
        buf[0..8].copy_from_slice(META_MAGIC);
        buf[8..16].copy_from_slice(&next_transaction_id.to_le_bytes());
        buf[16..24].copy_from_slice(&next_commit_ts.to_le_bytes());
        buf[24..32].copy_from_slice(&frozen_xid.to_le_bytes());
        self.write_page(0, &buf)?;
        let mut io = self.io();
        io.write_page_raw(0, &buf)?;
        io.file.sync_all()?;
        Ok(())
    }

    pub fn commit_transaction(&self) -> io::Result<()> {
        let Some(transaction) = self.transaction().take() else {
            return Ok(());
        };
        let transaction_id = transaction.id();

        // Snapshot the touched pages' current images under short read latches,
        // *before* taking the io lock: a page latch is never held while blocking
        // on disk work, and other transactions may be latching the same pages.
        // Under version-in-place an image may also carry another live
        // transaction's uncommitted versions, which is safe: visibility hides
        // them via the clog.
        let images: Vec<(u32, PageData)> = transaction
            .touched_pages()
            .filter_map(|page_num| self.resident_image(page_num).map(|data| (page_num, data)))
            .collect();

        // One commit at a time: the WAL protocol below is a sequence, not a set.
        let _commit = self
            .commit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let commit_ts = {
            let mut counters = self.counters();
            let commit_ts = counters.next_commit_ts;
            counters.next_commit_ts = counters.next_commit_ts.saturating_add(1);
            commit_ts
        };
        let committed = TransactionStatus::Committed(commit_ts);

        // WAL protocol: first log every touched page image, then log the commit
        // record. Recovery only replays page records that appear before the
        // commit/checkpoint marker and restores the transaction table from
        // transaction-status records. Database pages are written only after the
        // commit record is durable.
        {
            let mut io = self.io();
            for (page_num, data) in &images {
                io.wal.append_page(*page_num, data)?;
            }
            io.wal.append_tx_status(transaction_id, committed)?;
        }

        // The commit record is durable, so the transaction may now be published
        // as committed. This is the point visibility flips for every other
        // transaction, and it is a single short write critical section.
        self.tx_table
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(transaction_id, committed);

        {
            let mut io = self.io();
            for (page_num, data) in &images {
                io.write_page_raw(*page_num, data)?;
            }
            // Record the commit durably in the clog before truncating the WAL:
            // once the WAL is gone the clog is the only cross-reopen witness that
            // this transaction committed. A crash before this point leaves the
            // WAL commit record, which recovery replays into the clog instead.
            io.clog.record(transaction_id, committed)?;
            io.wal.append_checkpoint()?;
            io.wal.truncate()?;
            io.file.sync_all()?;
        }

        // Persist the advanced transaction-id counter so a later session does
        // not restart ids and collide with versions committed by this one. Read
        // the counters fresh: a concurrent commit may have advanced them further,
        // and page-0 meta must never move backwards.
        let (next_transaction_id, next_commit_ts, frozen_xid) = {
            let counters = self.counters();
            (
                counters.next_transaction_id,
                counters.next_commit_ts,
                counters.frozen_xid,
            )
        };
        self.persist_meta(next_transaction_id, next_commit_ts, frozen_xid)
    }

    pub fn rollback_transaction(&self) -> io::Result<()> {
        let Some(transaction) = self.transaction().take() else {
            return Ok(());
        };
        let transaction_id = transaction.id();

        let _commit = self
            .commit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        self.tx_table
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(transaction_id, TransactionStatus::Aborted);
        {
            let mut io = self.io();
            io.wal
                .append_tx_status(transaction_id, TransactionStatus::Aborted)?;
            // Durable abort: the clog must remember this so the frozen rule never
            // resurrects the aborted transaction's versions after the WAL that
            // held its abort record is truncated by a later commit.
            io.clog
                .record(transaction_id, TransactionStatus::Aborted)?;
        }

        // Logical abort. The transaction's versioned data is left in place,
        // stamped with an aborted creator (and, for UPDATE/DELETE, an aborted
        // deleter on the prior version). MVCC visibility hides that work from
        // every snapshot and vacuum reclaims it later, so there is no need to
        // physically revert data pages — and doing so would clobber other
        // writers' versions once several transactions share a page.
        //
        // The exception is the non-versioned schema catalog (pages 1 and 2):
        // its scans are not visibility-filtered, so aborted CREATE/DROP TABLE
        // rows there would otherwise be read back as live. DDL is serialized
        // by a coarse catalog lock, so reverting just those two pages is safe.
        for page_num in transaction.touched_pages() {
            if page_num != CATALOG_TABLE_PAGE && page_num != CATALOG_SEQUENCE_PAGE {
                continue;
            }
            let mut buf = [0u8; PAGE_SIZE];
            {
                let mut io = self.io();
                if page_num < io.file_length_pages {
                    io.read_page_raw(page_num, &mut buf)?;
                }
            }
            if self.resident_frame(page_num).is_some() {
                self.write_page(page_num, &buf)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn cleanup(path: &std::path::Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}.wal", path.display()));
        let _ = fs::remove_file(format!("{}.clog", path.display()));
    }

    #[test]
    fn rollback_status_is_restored_from_wal() {
        let path =
            std::env::temp_dir().join(format!("aerodb-pager-rollback-{}.db", std::process::id()));
        cleanup(&path);

        {
            let pager = Pager::new(path.to_str().unwrap()).unwrap();
            pager
                .begin_transaction(
                    7,
                    Snapshot::new_for_transaction(7, 8, vec![]),
                    None,
                    IsolationLevel::Snapshot,
                )
                .unwrap();
            pager.rollback_transaction().unwrap();
        }

        let pager = Pager::new(path.to_str().unwrap()).unwrap();
        assert_eq!(
            pager.transaction_table().get(&7),
            Some(&TransactionStatus::Aborted)
        );

        cleanup(&path);
    }

    #[test]
    fn aborted_status_survives_wal_truncation_via_clog() {
        // An aborted transaction's status must outlive the WAL that recorded it.
        // A later commit truncates the WAL, erasing the abort record there; only
        // the durable clog can still report the earlier transaction as aborted.
        let path = std::env::temp_dir()
            .join(format!("aerodb-pager-clog-abort-{}.db", std::process::id()));
        cleanup(&path);

        {
            let pager = Pager::new(path.to_str().unwrap()).unwrap();
            // Transaction 7 aborts (WAL now holds its begin/abort records).
            pager
                .begin_transaction(
                    7,
                    Snapshot::new_for_transaction(7, 8, vec![]),
                    None,
                    IsolationLevel::Snapshot,
                )
                .unwrap();
            pager.rollback_transaction().unwrap();
            // Transaction 8 commits, which truncates the WAL and persists meta.
            pager
                .begin_transaction(
                    8,
                    Snapshot::new_for_transaction(8, 9, vec![]),
                    None,
                    IsolationLevel::Snapshot,
                )
                .unwrap();
            pager.commit_transaction().unwrap();
        }

        let pager = Pager::new(path.to_str().unwrap()).unwrap();
        assert_eq!(
            pager.transaction_table().get(&7),
            Some(&TransactionStatus::Aborted),
            "abort must persist in the clog after the WAL was truncated by a later commit"
        );

        cleanup(&path);
    }

    #[test]
    fn concurrent_writes_to_distinct_pages_do_not_serialize_on_one_latch() {
        // Two threads mutating different pages must both make progress through
        // the shared pager: page latches are per-page, not global.
        let path = std::env::temp_dir()
            .join(format!("aerodb-pager-parallel-{}.db", std::process::id()));
        cleanup(&path);

        let pager = Arc::new(Pager::new(path.to_str().unwrap()).unwrap());
        let handles: Vec<_> = (1u32..=4)
            .map(|page_num| {
                let pager = Arc::clone(&pager);
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        pager
                            .with_page_mut(page_num, |data| {
                                data[0] = data[0].wrapping_add(1);
                            })
                            .unwrap();
                    }
                    pager.read_page(page_num).unwrap()[0]
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.join().unwrap(), 200u8);
        }

        cleanup(&path);
    }
}
