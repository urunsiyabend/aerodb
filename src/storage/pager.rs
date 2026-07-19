use crate::storage::page::PAGE_SIZE;
use crate::transaction::{
    IsolationLevel, Snapshot, Transaction, TransactionId, TransactionStatus, TransactionTable,
    WriteIntent, clog::Clog, wal::Wal,
};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

/// Magic marking page 0 as the durable engine meta page. Older databases leave
/// page 0 zeroed (allocated but unused), so a mismatch means "no meta yet".
const META_MAGIC: &[u8; 8] = b"AERODBM1";

/// Fixed pages holding the non-versioned schema catalog: page 1 is the table
/// catalog, page 2 the sequence catalog. Their scans are not MVCC-filtered, so
/// [`Pager::rollback_transaction`] physically reverts just these pages on abort;
/// see the logical-abort note there.
const CATALOG_TABLE_PAGE: u32 = 1;
const CATALOG_SEQUENCE_PAGE: u32 = 2;

/// A single 4 KiB page of data.
pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: [0; PAGE_SIZE],
        }
    }
}

/// Pager: manages reading/writing 4 KiB pages from/into the database file,
/// and keeps a simple in-memory cache. Distinguishes pages already on disk
/// from pages newly allocated in memory.
pub struct Pager {
    file: File,
    wal: Wal,
    /// Durable transaction-status store. Survives commit (unlike the WAL) so
    /// aborted/committed statuses remain available to MVCC visibility and vacuum
    /// across reopen.
    clog: Clog,

    /// The number of pages that already existed on disk when we opened this file.
    file_length_pages: u32,

    /// The total number of pages that the pager knows about right now
    /// (including any newly allocated ones not yet flushed).
    num_pages: u32,

    /// A very basic cache: `cache[page_num] = Some(Box<Page>)` if that page is loaded.
    cache: Vec<Option<Box<Page>>>,

    /// The single live transaction, if any. Replaces the former inline
    /// `transaction` + `dirty_pages` fields: a `Transaction` now owns both its
    /// MVCC identity and its private write set. Still one at a time here; a later
    /// phase relocates ownership out of the pager to allow several at once.
    transaction: Option<Transaction>,
    tx_table: TransactionTable,
    next_transaction_id: TransactionId,
    next_commit_ts: u64,
    /// Watermark below which every transaction id is treated as committed
    /// (its clog entry may be dropped). Persisted in page-0 meta; advanced by
    /// vacuum once the corresponding versions are reclaimed.
    frozen_xid: TransactionId,
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
            file,
            wal,
            clog,
            file_length_pages,
            num_pages: file_length_pages,
            cache: Vec::new(),
            transaction: None,
            tx_table,
            next_transaction_id,
            next_commit_ts,
            frozen_xid,
        })
    }

    /// Return a mutable reference to the requested page, loading from disk if it already existed.
    ///
    /// If `page_num >= num_pages`, we allocate blank pages up to that index. If `page_num < file_length_pages`,
    /// we also read from disk. Otherwise (a brand-new page), we leave it zeroed.
    pub fn get_page(&mut self, page_num: u32) -> io::Result<&mut Page> {
        // If caller asks beyond the current total, allocate blank pages up to there.
        if page_num >= self.num_pages {
            while page_num >= self.num_pages {
                self.allocate_page()?;
            }
        }

        // Ensure our cache vector is large enough.
        if self.cache.len() <= page_num as usize {
            self.cache.resize_with(page_num as usize + 1, || None);
        }

        // If not already in cache, create a new Page and load from disk if needed.
        if self.cache[page_num as usize].is_none() {
            // Always start with a zeroed page.
            let mut page = Box::new(Page::new());

            // Only attempt to read from disk if this page existed when we opened file.
            if page_num < self.file_length_pages {
                let offset = (page_num as u64) * (PAGE_SIZE as u64);
                self.file.seek(SeekFrom::Start(offset))?;
                self.file.read_exact(&mut page.data)?;
            }
            self.cache[page_num as usize] = Some(page);
        }

        // Safe to unwrap: we just inserted a Page if it was None.
        Ok(self.cache[page_num as usize].as_mut().unwrap())
    }

    /// Allocate a new page at the end (in memory). Increments `num_pages`.
    /// Does NOT change `file_length_pages` until we actually flush it.
    pub fn allocate_page(&mut self) -> io::Result<u32> {
        let new_page_num = self.num_pages;
        self.num_pages += 1;
        if self.cache.len() <= new_page_num as usize {
            self.cache.resize_with(new_page_num as usize + 1, || None);
        }
        Ok(new_page_num)
    }

    /// Write the cached page `page_num` back to disk. If this is a brand-new page (i.e. ≥ `file_length_pages`),
    /// we update `file_length_pages` so subsequent reads know it’s on disk.
    pub fn flush_page(&mut self, page_num: u32) -> io::Result<()> {
        if let Some(tx) = self.transaction.as_mut() {
            // Version-in-place: the mutation is already in the shared cache page.
            // Just remember the page so its current image is flushed at commit;
            // do not write to disk mid-transaction.
            tx.mark_touched(page_num);
        } else if let Some(page_box) = &self.cache[page_num as usize] {
            let data = page_box.data;
            self.wal.append_page(page_num, &data)?;
            self.write_page_raw(page_num, &data)?;
        }
        Ok(())
    }

    /// Copy the current cached image of `page_num`, if the page is resident.
    fn cached_page_image(&self, page_num: u32) -> Option<[u8; PAGE_SIZE]> {
        self.cache
            .get(page_num as usize)
            .and_then(|slot| slot.as_ref())
            .map(|page_box| page_box.data)
    }

    fn write_page_raw(&mut self, page_num: u32, data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        let offset = (page_num as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        self.file.flush()?;
        if page_num >= self.file_length_pages {
            self.file_length_pages = page_num + 1;
        }
        Ok(())
    }

    /// How many pages were already in the file when we opened it?
    pub fn file_length_pages(&self) -> u32 {
        self.file_length_pages
    }

    /// How many pages does the pager know about right now (on-disk + newly allocated)?
    pub fn num_pages(&self) -> u32 {
        self.num_pages
    }

    pub fn transaction_active(&self) -> bool {
        self.transaction.is_some()
    }

    pub fn begin_transaction(
        &mut self,
        id: TransactionId,
        snapshot: Snapshot,
        name: Option<String>,
        isolation_level: IsolationLevel,
    ) -> io::Result<()> {
        self.transaction = Some(Transaction::new(id, snapshot, name, isolation_level));
        self.tx_table.insert(id, TransactionStatus::Active);
        self.wal.append_tx_status(id, TransactionStatus::Active)?;
        // Record the in-flight status durably too, so a crash leaves an Active
        // clog entry that recovery converts to Aborted.
        self.clog.record(id, TransactionStatus::Active)?;
        Ok(())
    }

    /// Install a previously-begun transaction as the currently-executing one.
    ///
    /// The pager has a single execution slot; with multiple live transactions
    /// the manager keeps them detached and installs one for the duration of each
    /// operation (operations are serialized by the shared-storage lock).
    pub fn install_transaction(&mut self, transaction: Transaction) {
        debug_assert!(self.transaction.is_none(), "a transaction is already installed");
        self.transaction = Some(transaction);
    }

    /// Remove the currently-executing transaction so the manager can hold it
    /// between operations. Returns `None` if no transaction is installed.
    pub fn uninstall_transaction(&mut self) -> Option<Transaction> {
        self.transaction.take()
    }

    /// Record a write intent on the installed transaction for commit-time
    /// first-committer-wins re-validation. No-op outside a transaction.
    pub fn record_write_intent(
        &mut self,
        table_root: u32,
        key: i32,
        visible_created_tx: TransactionId,
    ) {
        if let Some(tx) = self.transaction.as_mut() {
            tx.record_write(WriteIntent {
                table_root,
                key,
                visible_created_tx,
            });
        }
    }

    /// The write intents recorded by the installed transaction.
    pub fn transaction_write_set(&self) -> Vec<WriteIntent> {
        self.transaction
            .as_ref()
            .map(|tx| tx.write_set().to_vec())
            .unwrap_or_default()
    }

    pub fn transaction_id(&self) -> Option<TransactionId> {
        self.transaction.as_ref().map(Transaction::id)
    }

    pub fn transaction_snapshot(&self) -> Option<&Snapshot> {
        self.transaction.as_ref().map(Transaction::snapshot)
    }

    pub fn transaction_table(&self) -> &TransactionTable {
        &self.tx_table
    }

    /// Allocate the next monotonic transaction id. The counter is made durable
    /// by [`Pager::persist_meta`] on commit, so ids never collide with
    /// `created_tx`/`deleted_tx` values already written by earlier sessions.
    pub fn allocate_transaction_id(&mut self) -> TransactionId {
        let id = self.next_transaction_id;
        self.next_transaction_id = self.next_transaction_id.saturating_add(1);
        id
    }

    /// The id that the next `allocate_transaction_id` would hand out. Used as the
    /// snapshot `xmax` boundary once the current transaction's id is allocated.
    pub fn peek_next_transaction_id(&self) -> TransactionId {
        self.next_transaction_id
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
    /// counters to page 0.
    fn persist_meta(&mut self) -> io::Result<()> {
        let mut buf = [0u8; PAGE_SIZE];
        buf[0..8].copy_from_slice(META_MAGIC);
        buf[8..16].copy_from_slice(&self.next_transaction_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.next_commit_ts.to_le_bytes());
        buf[24..32].copy_from_slice(&self.frozen_xid.to_le_bytes());
        self.write_page_raw(0, &buf)?;
        self.file.sync_all()?;
        if let Some(page_box) = self.cache.get_mut(0).and_then(|slot| slot.as_mut()) {
            page_box.data = buf;
        }
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> io::Result<()> {
        if let Some(transaction) = self.transaction.take() {
            let transaction_id = transaction.id();
            let commit_ts = self.next_commit_ts;
            self.next_commit_ts = self.next_commit_ts.saturating_add(1);

            // WAL protocol: first log every touched page image, then log the
            // commit record. Recovery only replays page records that appear
            // before the commit/checkpoint marker and restores the transaction
            // table from transaction-status records. Database pages are written
            // only after the commit record is durable. Under version-in-place the
            // image to persist is the page's *current* shared-cache content (it
            // may also carry other live transactions' uncommitted versions, which
            // is safe: visibility hides them via the clog).
            let touched: Vec<u32> = transaction.touched_pages().collect();
            for &page_num in &touched {
                if let Some(data) = self.cached_page_image(page_num) {
                    self.wal.append_page(page_num, &data)?;
                }
            }
            let committed = TransactionStatus::Committed(commit_ts);
            self.wal.append_tx_status(transaction_id, committed)?;
            self.tx_table.insert(transaction_id, committed);
            for &page_num in &touched {
                if let Some(data) = self.cached_page_image(page_num) {
                    self.write_page_raw(page_num, &data)?;
                }
            }
            // Record the commit durably in the clog before truncating the WAL:
            // once the WAL is gone the clog is the only cross-reopen witness that
            // this transaction committed. A crash before this point leaves the
            // WAL commit record, which recovery replays into the clog instead.
            self.clog.record(transaction_id, committed)?;
            self.wal.append_checkpoint()?;
            self.wal.truncate()?;
            self.file.sync_all()?;
            // Persist the advanced transaction-id counter so a later session does
            // not restart ids and collide with versions committed by this one.
            self.persist_meta()?;
        }
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        if let Some(transaction) = self.transaction.take() {
            let transaction_id = transaction.id();
            self.wal
                .append_tx_status(transaction_id, TransactionStatus::Aborted)?;
            self.tx_table
                .insert(transaction_id, TransactionStatus::Aborted);
            // Durable abort: the clog must remember this so the frozen rule never
            // resurrects the aborted transaction's versions after the WAL that
            // held its abort record is truncated by a later commit.
            self.clog
                .record(transaction_id, TransactionStatus::Aborted)?;

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
            let touched: Vec<u32> = transaction.touched_pages().collect();
            for page_num in touched {
                if page_num != CATALOG_TABLE_PAGE && page_num != CATALOG_SEQUENCE_PAGE {
                    continue;
                }
                let mut buf = [0u8; PAGE_SIZE];
                if page_num < self.file_length_pages {
                    let offset = (page_num as u64) * (PAGE_SIZE as u64);
                    self.file.seek(SeekFrom::Start(offset))?;
                    self.file.read_exact(&mut buf)?;
                }
                if let Some(page_box) = &mut self.cache[page_num as usize] {
                    page_box.data = buf;
                }
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
            let mut pager = Pager::new(path.to_str().unwrap()).unwrap();
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
            let mut pager = Pager::new(path.to_str().unwrap()).unwrap();
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
}

// #[test]
// fn test_leaf_multiple_inserts_and_find() {
//     let pager = Pager::new("test.aerodb").unwrap();
//     let mut btree = BTree::new(pager).unwrap();
//     assert!(btree.find(10).unwrap().is_none());
//     assert!(btree.insert(10, "Ten").is_ok());
//     assert!(btree.insert(5, "Five").is_ok());
//     assert!(btree.insert(20, "Twenty").is_ok());
//     let row = btree.find(5).unwrap().unwrap();
//     assert_eq!(row.payload, "Five");
//     let row2 = btree.find(20).unwrap().unwrap();
//     assert_eq!(row2.payload, "Twenty");
// }
