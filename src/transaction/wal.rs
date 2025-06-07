use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use crate::storage::page::PAGE_SIZE;

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: &str, db_file: &mut File) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        // replay existing log if any
        Wal::recover_internal(&mut file, db_file)?;
        Ok(Wal { file })
    }

    fn recover_internal(wal: &mut File, db: &mut File) -> io::Result<()> {
        wal.seek(SeekFrom::Start(0))?;
        let mut page_num_buf = [0u8; 4];
        loop {
            if wal.read_exact(&mut page_num_buf).is_err() {
                break;
            }
            let page_num = u32::from_le_bytes(page_num_buf);
            if page_num == u32::MAX {
                break;
            }
            let mut data = [0u8; PAGE_SIZE];
            wal.read_exact(&mut data)?;
            db.seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE as u64))?;
            db.write_all(&data)?;
        }
        wal.set_len(0)?;
        wal.sync_all()?;
        Ok(())
    }

    pub fn append_page(&mut self, page_num: u32, data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&page_num.to_le_bytes())?;
        self.file.write_all(data)?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn append_checkpoint(&mut self) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&u32::MAX.to_le_bytes())?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.sync_all()?;
        Ok(())
    }
}
