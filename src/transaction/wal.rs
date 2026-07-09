use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::storage::page::PAGE_SIZE;

use super::{CommitTimestamp, TransactionId, TransactionStatus, TransactionTable};

const CHECKPOINT_RECORD: u32 = u32::MAX;
const TX_STATUS_RECORD: u32 = u32::MAX - 1;
const TX_STATUS_ACTIVE: u8 = 0;
const TX_STATUS_COMMITTED: u8 = 1;
const TX_STATUS_ABORTED: u8 = 2;

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
        Ok((Wal { file }, tx_table))
    }

    fn recover_internal(wal: &mut File, db: &mut File) -> io::Result<TransactionTable> {
        wal.seek(SeekFrom::Start(0))?;
        let mut tx_table = TransactionTable::new();
        let mut record_buf = [0u8; 4];
        loop {
            if wal.read_exact(&mut record_buf).is_err() {
                break;
            }
            match u32::from_le_bytes(record_buf) {
                CHECKPOINT_RECORD => break,
                TX_STATUS_RECORD => {
                    let mut tx_id_buf = [0u8; 8];
                    let mut status_buf = [0u8; 1];
                    let mut commit_ts_buf = [0u8; 8];
                    wal.read_exact(&mut tx_id_buf)?;
                    wal.read_exact(&mut status_buf)?;
                    wal.read_exact(&mut commit_ts_buf)?;
                    let tx_id = TransactionId::from_le_bytes(tx_id_buf);
                    let commit_ts = CommitTimestamp::from_le_bytes(commit_ts_buf);
                    let status = match status_buf[0] {
                        TX_STATUS_ACTIVE => TransactionStatus::Active,
                        TX_STATUS_COMMITTED => TransactionStatus::Committed(commit_ts),
                        TX_STATUS_ABORTED => TransactionStatus::Aborted,
                        other => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("unknown WAL transaction status tag {other}"),
                            ));
                        }
                    };
                    tx_table.insert(tx_id, status);
                }
                page_num => {
                    let mut data = [0u8; PAGE_SIZE];
                    wal.read_exact(&mut data)?;
                    db.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))?;
                    db.write_all(&data)?;
                }
            }
        }
        wal.set_len(0)?;
        wal.sync_all()?;
        Ok(tx_table)
    }

    pub fn append_page(&mut self, page_num: u32, data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&page_num.to_le_bytes())?;
        self.file.write_all(data)?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn append_tx_status(
        &mut self,
        tx_id: TransactionId,
        status: TransactionStatus,
    ) -> io::Result<()> {
        let (status_tag, commit_ts) = match status {
            TransactionStatus::Active => (TX_STATUS_ACTIVE, 0),
            TransactionStatus::Committed(commit_ts) => (TX_STATUS_COMMITTED, commit_ts),
            TransactionStatus::Aborted => (TX_STATUS_ABORTED, 0),
        };
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&TX_STATUS_RECORD.to_le_bytes())?;
        self.file.write_all(&tx_id.to_le_bytes())?;
        self.file.write_all(&[status_tag])?;
        self.file.write_all(&commit_ts.to_le_bytes())?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn append_checkpoint(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&CHECKPOINT_RECORD.to_le_bytes())?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        Ok(())
    }
}
