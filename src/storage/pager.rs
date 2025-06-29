use std::collections::HashMap;
use std::fs::{OpenOptions, File};
use std::io::{self, Read, Write, Seek, SeekFrom};
use crate::transaction::wal::Wal;
use crate::storage::page::PAGE_SIZE;

/// A single 4 KiB page of data.
pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Page { data: [0; PAGE_SIZE] }
    }
}

/// Pager: manages reading/writing 4 KiB pages from/into the database file,
/// and keeps a simple in-memory cache. Distinguishes pages already on disk
/// from pages newly allocated in memory.
pub struct Pager {
    file: File,
    wal: Wal,

    /// The number of pages that already existed on disk when we opened this file.
    file_length_pages: u32,

    /// The total number of pages that the pager knows about right now
    /// (including any newly allocated ones not yet flushed).
    num_pages: u32,

    /// A very basic cache: `cache[page_num] = Some(Box<Page>)` if that page is loaded.
    cache: Vec<Option<Box<Page>>>,

    transaction_active: bool,
    dirty_pages: HashMap<u32, [u8; PAGE_SIZE]>,
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
        let wal = Wal::open(&wal_path, &mut file)?;

        // Determine file length after WAL recovery in case pages were replayed
        let file_len_after = file.metadata()?.len();
        let file_length_pages = (file_len_after as usize / PAGE_SIZE) as u32;

        Ok(Pager {
            file,
            wal,
            file_length_pages,
            num_pages: file_length_pages,
            cache: Vec::new(),
            transaction_active: false,
            dirty_pages: HashMap::new(),
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
        if let Some(page_box) = &self.cache[page_num as usize] {
            let data = page_box.data;
            if self.transaction_active {
                self.dirty_pages.insert(page_num, data);
            } else {
                self.wal.append_page(page_num, &data)?;
                self.write_page_raw(page_num, &data)?;
            }
        }
        Ok(())
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
        self.transaction_active
    }

    pub fn begin_transaction(&mut self, _name: Option<String>) -> io::Result<()> {
        self.transaction_active = true;
        self.dirty_pages.clear();
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> io::Result<()> {
        if self.transaction_active {
            let pages = self.dirty_pages.clone();
            for (page_num, data) in pages {
                self.wal.append_page(page_num, &data)?;
                self.write_page_raw(page_num, &data)?;
            }
            self.wal.append_checkpoint()?;
            self.wal.truncate()?;
            self.file.sync_all()?;
            self.dirty_pages.clear();
            self.transaction_active = false;
        }
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        if self.transaction_active {
            for page_num in self.dirty_pages.keys().cloned().collect::<Vec<_>>() {
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
            self.wal.truncate()?;
            self.dirty_pages.clear();
            self.transaction_active = false;
        }
        Ok(())
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
