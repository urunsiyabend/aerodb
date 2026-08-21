use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

use super::{TransactionId, TransactionStatus, TransactionTable};

/// Durable commit log ("clog"): the authoritative record of each transaction's
/// final status, kept in a `<db>.clog` sidecar so it survives across reopen.
///
/// Why it exists: the WAL is truncated on every clean commit, so it cannot be
/// the durable home of committed/aborted status. The MVCC "frozen rule" treats a
/// tx id absent from the table as committed; that is only safe if *aborted* (and
/// still-relevant committed) statuses are retained durably until their versions
/// are vacuumed. The clog provides that durable store. The WAL remains the redo
/// log for crash recovery of an in-flight commit; at open its recovered statuses
/// are merged on top of the clog (the WAL is the crash-truth for in-flight tx).
///
/// Layout: an 8-byte magic header followed by a packed array of 2-bit states
/// indexed by transaction id (4 states per byte). tx id 0 is the committed
/// bootstrap sentinel and is never stored.
pub struct Clog {
    file: File,
}

const CLOG_MAGIC: &[u8; 8] = b"AEROCLG1";
const HEADER_LEN: u64 = 8;

const ST_UNKNOWN: u8 = 0;
const ST_ACTIVE: u8 = 1;
const ST_COMMITTED: u8 = 2;
const ST_ABORTED: u8 = 3;

impl Clog {
    /// Open (or create) the clog sidecar, validating/initializing its header.
    pub fn open(path: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let len = file.metadata()?.len();
        if len < HEADER_LEN {
            Clog::write_header(&mut file)?;
        } else {
            let mut magic = [0u8; 8];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut magic)?;
            if &magic != CLOG_MAGIC {
                // Unknown/legacy format: start clean rather than misread it.
                Clog::write_header(&mut file)?;
            }
        }

        Ok(Clog { file })
    }

    fn write_header(file: &mut File) -> io::Result<()> {
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(CLOG_MAGIC)?;
        file.sync_all()?;
        Ok(())
    }

    /// Discard all recorded statuses (header only). Used when the database file
    /// is fresh (no durable meta) so a stale sidecar cannot leak old statuses
    /// onto a database whose transaction ids restart from 1.
    pub fn reset(&mut self) -> io::Result<()> {
        Clog::write_header(&mut self.file)
    }

    /// Drop the recorded status of every transaction below `below`, making those
    /// slots read back as absent (and therefore, under the MVCC frozen rule, as
    /// committed). Called from [`crate::storage::pager::Pager::advance_frozen_xid`]
    /// once a vacuum pass proves no live snapshot and no remaining on-disk version
    /// still needs to distinguish those transactions.
    ///
    /// The packed array is absolute-indexed by transaction id, so compaction
    /// zeroes bytes in place rather than shrinking the file. Rebasing the array
    /// onto a stored base offset would let it shrink, but the file is one byte per
    /// four transactions -- small enough that the extra indirection is not worth
    /// it.
    pub fn compact(&mut self, below: TransactionId) -> io::Result<()> {
        if below == 0 {
            return Ok(());
        }

        let len = self.file.metadata()?.len();
        if len <= HEADER_LEN {
            return Ok(());
        }

        // Bytes wholly below the watermark are zeroed; the byte straddling it
        // keeps the slots at or above `below`.
        let whole_bytes = ((below / 4) as u64).min(len - HEADER_LEN);
        if whole_bytes > 0 {
            let zeros = vec![0u8; whole_bytes as usize];
            self.file.seek(SeekFrom::Start(HEADER_LEN))?;
            self.file.write_all(&zeros)?;
        }

        let partial_slots = below % 4;
        let partial_offset = HEADER_LEN + whole_bytes;
        if partial_slots > 0 && partial_offset < len {
            let mut byte = [0u8; 1];
            self.file.seek(SeekFrom::Start(partial_offset))?;
            self.file.read_exact(&mut byte)?;
            for slot in 0..partial_slots {
                byte[0] &= !(0b11 << (slot * 2));
            }
            self.file.seek(SeekFrom::Start(partial_offset))?;
            self.file.write_all(&byte)?;
        }

        self.file.sync_all()?;
        Ok(())
    }

    fn byte_offset(tx_id: TransactionId) -> u64 {
        HEADER_LEN + tx_id / 4
    }

    fn shift(tx_id: TransactionId) -> u32 {
        ((tx_id % 4) * 2) as u32
    }

    fn code_for(status: TransactionStatus) -> u8 {
        match status {
            TransactionStatus::Active => ST_ACTIVE,
            TransactionStatus::Committed(_) => ST_COMMITTED,
            TransactionStatus::Aborted => ST_ABORTED,
        }
    }

    /// Durably record `status` for `tx_id` (read-modify-write of one packed byte,
    /// fsync'd). The commit timestamp is not stored: MVCC visibility and vacuum
    /// only need the discrete state, and the `next_commit_ts` counter is kept in
    /// page-0 meta.
    pub fn record(&mut self, tx_id: TransactionId, status: TransactionStatus) -> io::Result<()> {
        let code = Clog::code_for(status);
        let offset = Clog::byte_offset(tx_id);
        let shift = Clog::shift(tx_id);

        let mut byte = [0u8; 1];
        let len = self.file.metadata()?.len();
        if offset < len {
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut byte)?;
        } else {
            // Zero-fill the gap up to and including this byte.
            self.file.set_len(offset + 1)?;
        }

        byte[0] = (byte[0] & !(0b11 << shift)) | (code << shift);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&byte)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Rebuild the in-memory transaction table from the durable statuses.
    ///
    /// Any transaction still recorded as `Active` was live when the process last
    /// died: it is treated as `Aborted` (crash recovery) and that decision is
    /// persisted back so a later frozen-rule read never resurrects it. The stored
    /// commit timestamp is a placeholder (`0`); it is unused by visibility.
    pub fn load(&mut self) -> io::Result<TransactionTable> {
        let len = self.file.metadata()?.len();
        let mut table = TransactionTable::new();
        if len <= HEADER_LEN {
            return Ok(table);
        }

        let mut buf = vec![0u8; (len - HEADER_LEN) as usize];
        self.file.seek(SeekFrom::Start(HEADER_LEN))?;
        self.file.read_exact(&mut buf)?;

        let mut crashed_active = Vec::new();
        for (byte_idx, &packed) in buf.iter().enumerate() {
            for slot in 0..4u64 {
                let code = (packed >> (slot * 2)) & 0b11;
                if code == ST_UNKNOWN {
                    continue;
                }
                let tx_id = (byte_idx as u64) * 4 + slot;
                match code {
                    ST_ACTIVE => {
                        table.insert(tx_id, TransactionStatus::Aborted);
                        crashed_active.push(tx_id);
                    }
                    ST_COMMITTED => {
                        table.insert(tx_id, TransactionStatus::Committed(0));
                    }
                    ST_ABORTED => {
                        table.insert(tx_id, TransactionStatus::Aborted);
                    }
                    _ => {}
                }
            }
        }

        for tx_id in crashed_active {
            self.record(tx_id, TransactionStatus::Aborted)?;
        }

        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!("aerodb-clog-{}-{}.clog", tag, std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn records_and_reloads_terminal_statuses() {
        let path = temp_path("terminal");
        let _ = std::fs::remove_file(&path);
        {
            let mut clog = Clog::open(&path).unwrap();
            clog.record(1, TransactionStatus::Committed(5)).unwrap();
            clog.record(2, TransactionStatus::Aborted).unwrap();
            clog.record(3, TransactionStatus::Committed(9)).unwrap();
        }

        let mut clog = Clog::open(&path).unwrap();
        let table = clog.load().unwrap();
        assert_eq!(table.get(&1), Some(&TransactionStatus::Committed(0)));
        assert_eq!(table.get(&2), Some(&TransactionStatus::Aborted));
        assert_eq!(table.get(&3), Some(&TransactionStatus::Committed(0)));
        assert_eq!(table.get(&4), None, "unrecorded ids stay absent (frozen)");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn active_at_reload_becomes_persisted_abort() {
        let path = temp_path("crash");
        let _ = std::fs::remove_file(&path);
        {
            let mut clog = Clog::open(&path).unwrap();
            clog.record(7, TransactionStatus::Active).unwrap();
        }

        // First reload treats the in-flight transaction as aborted...
        {
            let mut clog = Clog::open(&path).unwrap();
            assert_eq!(clog.load().unwrap().get(&7), Some(&TransactionStatus::Aborted));
        }
        // ...and persists that so a later reload still sees Aborted, not Active.
        let mut clog = Clog::open(&path).unwrap();
        assert_eq!(clog.load().unwrap().get(&7), Some(&TransactionStatus::Aborted));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn compact_drops_statuses_below_watermark() {
        let path = temp_path("compact");
        let _ = std::fs::remove_file(&path);
        let mut clog = Clog::open(&path).unwrap();
        clog.record(1, TransactionStatus::Committed(1)).unwrap();
        clog.record(2, TransactionStatus::Aborted).unwrap();
        clog.record(3, TransactionStatus::Committed(3)).unwrap();
        clog.record(4, TransactionStatus::Aborted).unwrap();
        clog.record(5, TransactionStatus::Committed(5)).unwrap();

        // Watermark 4 straddles a packed byte (ids 4..7 share byte 1), so this
        // exercises both the whole-byte and partial-byte paths.
        clog.compact(4).unwrap();

        let table = clog.load().unwrap();
        assert_eq!(table.get(&1), None, "committed below watermark is dropped");
        assert_eq!(table.get(&2), None, "aborted below watermark is dropped");
        assert_eq!(table.get(&3), None);
        assert_eq!(table.get(&4), Some(&TransactionStatus::Aborted));
        assert_eq!(table.get(&5), Some(&TransactionStatus::Committed(0)));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn compact_survives_reopen_and_tolerates_out_of_range_watermarks() {
        let path = temp_path("compact-reopen");
        let _ = std::fs::remove_file(&path);
        {
            let mut clog = Clog::open(&path).unwrap();
            clog.record(9, TransactionStatus::Aborted).unwrap();
            // A watermark past everything recorded clears the whole array...
            clog.compact(400).unwrap();
            assert!(clog.load().unwrap().is_empty());
            // ...and compaction is a no-op on an empty array or a zero watermark.
            clog.compact(0).unwrap();
            clog.compact(7).unwrap();
        }

        let mut clog = Clog::open(&path).unwrap();
        assert!(clog.load().unwrap().is_empty(), "compaction is durable");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reset_clears_recorded_statuses() {
        let path = temp_path("reset");
        let _ = std::fs::remove_file(&path);
        let mut clog = Clog::open(&path).unwrap();
        clog.record(1, TransactionStatus::Committed(1)).unwrap();
        clog.reset().unwrap();
        assert!(clog.load().unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
