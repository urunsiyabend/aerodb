use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};

use crate::storage::page::PAGE_SIZE;

use super::{CommitTimestamp, TransactionId, TransactionStatus, TransactionTable};

const WAL_MAGIC: &[u8; 8] = b"AEROWAL2";
const PAGE_IMAGE_TAG: u8 = 1;
const TX_BEGIN_TAG: u8 = 2;
const TX_COMMIT_TAG: u8 = 3;
const TX_ABORT_TAG: u8 = 4;
const CHECKPOINT_TAG: u8 = 5;

/// Record-oriented WAL entry format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    PageImage {
        page_num: u32,
        data: Box<[u8; PAGE_SIZE]>,
    },
    TxBegin {
        tx_id: TransactionId,
    },
    TxCommit {
        tx_id: TransactionId,
        commit_ts: CommitTimestamp,
    },
    TxAbort {
        tx_id: TransactionId,
    },
    Checkpoint,
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: &str, db_file: &mut File) -> io::Result<(Self, TransactionTable)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let tx_table = Wal::recover_internal(&mut file, db_file)?;
        Wal::initialize_empty_log(&mut file)?;
        Ok((Wal { file }, tx_table))
    }

    fn recover_internal(wal: &mut File, db: &mut File) -> io::Result<TransactionTable> {
        wal.seek(SeekFrom::Start(0))?;
        if !Wal::read_or_initialize_header(wal)? {
            return Ok(TransactionTable::new());
        }

        let mut tx_table = TransactionTable::new();
        while let Some(record) = Wal::read_record(wal)? {
            match record {
                WalRecord::PageImage { page_num, data } => {
                    db.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))?;
                    db.write_all(&*data)?;
                }
                WalRecord::TxBegin { tx_id } => {
                    tx_table.insert(tx_id, TransactionStatus::Active);
                }
                WalRecord::TxCommit { tx_id, commit_ts } => {
                    tx_table.insert(tx_id, TransactionStatus::Committed(commit_ts));
                }
                WalRecord::TxAbort { tx_id } => {
                    tx_table.insert(tx_id, TransactionStatus::Aborted);
                }
                WalRecord::Checkpoint => {}
            }
        }

        for status in tx_table.values_mut() {
            if matches!(status, TransactionStatus::Active) {
                *status = TransactionStatus::Aborted;
            }
        }

        wal.set_len(0)?;
        wal.sync_all()?;
        Ok(tx_table)
    }

    /// Returns true when the WAL uses the current record format. Older page-first
    /// WALs did not contain a magic header; they are treated as an incompatible
    /// legacy format and truncated for a clean start instead of being replayed as
    /// ambiguous records.
    fn read_or_initialize_header(wal: &mut File) -> io::Result<bool> {
        let len = wal.metadata()?.len();
        if len == 0 {
            wal.write_all(WAL_MAGIC)?;
            wal.sync_all()?;
            return Ok(true);
        }

        let mut magic = [0u8; WAL_MAGIC.len()];
        match wal.read_exact(&mut magic) {
            Ok(()) if &magic == WAL_MAGIC => Ok(true),
            Ok(()) | Err(_) => {
                wal.set_len(0)?;
                wal.seek(SeekFrom::Start(0))?;
                wal.write_all(WAL_MAGIC)?;
                wal.sync_all()?;
                Ok(false)
            }
        }
    }

    fn initialize_empty_log(wal: &mut File) -> io::Result<()> {
        if wal.metadata()?.len() == 0 {
            wal.seek(SeekFrom::Start(0))?;
            wal.write_all(WAL_MAGIC)?;
            wal.sync_all()?;
        }
        Ok(())
    }

    fn read_record(wal: &mut File) -> io::Result<Option<WalRecord>> {
        let mut tag = [0u8; 1];
        match wal.read_exact(&mut tag) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err),
        }

        let record = match tag[0] {
            PAGE_IMAGE_TAG => {
                let page_num = Wal::read_u32(wal)?;
                let mut data = Box::new([0u8; PAGE_SIZE]);
                wal.read_exact(data.as_mut())?;
                WalRecord::PageImage { page_num, data }
            }
            TX_BEGIN_TAG => WalRecord::TxBegin {
                tx_id: Wal::read_u64(wal)?,
            },
            TX_COMMIT_TAG => WalRecord::TxCommit {
                tx_id: Wal::read_u64(wal)?,
                commit_ts: Wal::read_u64(wal)?,
            },
            TX_ABORT_TAG => WalRecord::TxAbort {
                tx_id: Wal::read_u64(wal)?,
            },
            CHECKPOINT_TAG => WalRecord::Checkpoint,
            other => {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!("unknown WAL record tag {other}"),
                ));
            }
        };
        Ok(Some(record))
    }

    fn read_u32(wal: &mut File) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        wal.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64(wal: &mut File) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        wal.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn append_record(&mut self, record: WalRecord) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        match record {
            WalRecord::PageImage { page_num, data } => {
                self.file.write_all(&[PAGE_IMAGE_TAG])?;
                self.file.write_all(&page_num.to_le_bytes())?;
                self.file.write_all(&*data)?;
            }
            WalRecord::TxBegin { tx_id } => {
                self.file.write_all(&[TX_BEGIN_TAG])?;
                self.file.write_all(&tx_id.to_le_bytes())?;
            }
            WalRecord::TxCommit { tx_id, commit_ts } => {
                self.file.write_all(&[TX_COMMIT_TAG])?;
                self.file.write_all(&tx_id.to_le_bytes())?;
                self.file.write_all(&commit_ts.to_le_bytes())?;
            }
            WalRecord::TxAbort { tx_id } => {
                self.file.write_all(&[TX_ABORT_TAG])?;
                self.file.write_all(&tx_id.to_le_bytes())?;
            }
            WalRecord::Checkpoint => {
                self.file.write_all(&[CHECKPOINT_TAG])?;
            }
        }
        self.file.sync_all()?;
        Ok(())
    }

    pub fn append_page(&mut self, page_num: u32, data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.append_record(WalRecord::PageImage {
            page_num,
            data: Box::new(*data),
        })
    }

    pub fn append_tx_status(
        &mut self,
        tx_id: TransactionId,
        status: TransactionStatus,
    ) -> io::Result<()> {
        match status {
            TransactionStatus::Active => self.append_record(WalRecord::TxBegin { tx_id }),
            TransactionStatus::Committed(commit_ts) => {
                self.append_record(WalRecord::TxCommit { tx_id, commit_ts })
            }
            TransactionStatus::Aborted => self.append_record(WalRecord::TxAbort { tx_id }),
        }
    }

    pub fn append_checkpoint(&mut self) -> io::Result<()> {
        self.append_record(WalRecord::Checkpoint)
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(WAL_MAGIC)?;
        self.file.sync_all()?;
        Ok(())
    }
}
