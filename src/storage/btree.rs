use std::io;

use crate::storage::pager::Pager;
use crate::storage::page::{
    get_cell_count, get_node_type, set_cell_count, set_node_type, set_parent, set_is_root,
    NODE_LEAF, HEADER_SIZE, PAGE_SIZE,
};

/// A single row in our leaf: (key, payload).
#[derive(Debug, Clone)]
pub struct Row {
    pub key: i32,
    pub payload: String,
}

// ┌──────────────────────────────────────────────────────────────────────────────┐
// │ Offset │ Length │ Description                                                │
// │────────┼────────┼────────────────────────────────────────────────────────────│
// │   0    │   1    │ NODE_TYPE (0=internal, 1=leaf)                             │
// │   1    │   1    │ IS_ROOT (0=false, 1=true)                                  │
// │   2    │   4    │ PARENT_PAGE (u32) – page number of parent (0 if none)      │
// │   6    │   2    │ CELL_COUNT (u16) – how many cells are in this node         │
// │────────┼────────┼────────────────────────────────────────────────────────────│
// │   8    │ (PAGE_SIZE − 8) │ Cells: [key,u32][payload_len,u32][payload_bytes]… │
// └──────────────────────────────────────────────────────────────────────────────┘


pub struct BTree {
    /// Always use page 0 as the root in this simple setup.
    root_page: u32,
    pager: Pager,
}

impl BTree {
    /// Create or open a B-Tree. If the file was empty, allocate page 0 as a new leaf root.
    /// Otherwise, just reuse page 0 as the existing root.
    pub fn new(mut pager: Pager) -> io::Result<Self> {
        let root_page: u32;

        // If the file contained no pages on disk, allocate and initialize page 0.
        if pager.file_length_pages() == 0 {
            // Allocate the new root page (this will be page 0).
            let new_root = pager.allocate_page()?;

            // Initialize it as an empty leaf:
            let page = pager.get_page(new_root)?;
            set_node_type(&mut page.data, NODE_LEAF);
            set_is_root(&mut page.data, true);
            set_parent(&mut page.data, 0);       // no parent
            set_cell_count(&mut page.data, 0);   // start with 0 cells

            // Immediately flush so that page 0 actually exists on disk.
            pager.flush_page(new_root)?;
            root_page = new_root;
        } else {
            // The file already has ≥1 page. Don’t overwrite page 0; use it as root.
            root_page = 0;
        }

        Ok(BTree { root_page, pager })
    }

    /// Read all rows (cells) from a leaf page into a Vec<Row>.
    /// Returns a vector of the existing rows in sorted order.
    fn read_all_rows_from_leaf(&mut self) -> io::Result<Vec<Row>> {
        let page = self.pager.get_page(self.root_page)?;
        let node_type = get_node_type(&page.data);
        if node_type != NODE_LEAF {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "read_all_rows_from_leaf: not a leaf",
            ));
        }

        let cell_count = get_cell_count(&page.data) as usize;
        let mut rows = Vec::with_capacity(cell_count);

        let mut offset = HEADER_SIZE;
        for _ in 0..cell_count {
            // Read 4-byte key
            let key_bytes = &page.data[offset..offset + 4];
            let key = i32::from_le_bytes(key_bytes.try_into().unwrap());

            // Read 4-byte payload length
            let len_bytes = &page.data[offset + 4..offset + 8];
            let payload_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

            // Read payload bytes
            let start = offset + 8;
            let end = start + payload_len;
            if end > PAGE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "read_all_rows_from_leaf: corrupt payload length",
                ));
            }
            let payload_bytes = &page.data[start..end];
            let payload = String::from_utf8_lossy(payload_bytes).to_string();

            rows.push(Row { key, payload });

            // Advance offset to next cell
            offset = end;
        }

        Ok(rows)
    }

    /// Write a sorted list of rows back into the leaf page (overwriting its body).
    /// Assumes `rows` is sorted by key. Returns Err if total size would overflow the page.
    fn write_all_rows_to_leaf(&mut self, rows: &[Row]) -> io::Result<()> {
        let page = self.pager.get_page(self.root_page)?;

        // Compute total size of all cells:
        let mut total_cells_size = 0;
        for row in rows {
            total_cells_size += 4;                // 4 bytes for key
            total_cells_size += 4;                // 4 bytes for payload_len
            total_cells_size += row.payload.len(); // payload bytes
        }

        // Check for overflow: header + all cells must fit in PAGE_SIZE.
        if HEADER_SIZE + total_cells_size > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Leaf overflow: not enough space for all rows",
            ));
        }

        // Zero out everything after the header (optional, but safer).
        for idx in HEADER_SIZE..PAGE_SIZE {
            page.data[idx] = 0;
        }

        // Write cells sequentially starting at offset HEADER_SIZE
        let mut offset = HEADER_SIZE;
        for row in rows {
            // 4-byte key
            let key_bytes = row.key.to_le_bytes();
            page.data[offset..offset + 4].copy_from_slice(&key_bytes);

            // 4-byte payload length
            let payload_len = row.payload.len() as u32;
            let len_bytes = payload_len.to_le_bytes();
            page.data[offset + 4..offset + 8].copy_from_slice(&len_bytes);

            // Payload bytes
            let payload_bytes = row.payload.as_bytes();
            let start = offset + 8;
            let end = start + payload_bytes.len();
            page.data[start..end].copy_from_slice(payload_bytes);

            // Advance to next cell
            offset = end;
        }

        // Update the header’s CELL_COUNT
        set_cell_count(&mut page.data, rows.len() as u16);

        // Flush the page so changes persist on disk
        self.pager.flush_page(self.root_page)?;
        Ok(())
    }

    /// Find a row by key. Returns Ok(Some(Row)) if found, otherwise Ok(None).
    pub fn find(&mut self, target_key: i32) -> io::Result<Option<Row>> {
        // Read all rows into memory (Vec<Row>) to scan them.
        let rows = self.read_all_rows_from_leaf()?;
        // Because they are stored in sorted order, we could binary-search.
        // But for simplicity, do a linear scan:
        for row in rows {
            if row.key == target_key {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    /// Insert a new (key, payload) into the B-Tree leaf. If the leaf becomes too large,
    /// this will return an error "Leaf overflow...". Duplicates are not allowed.
    pub fn insert(&mut self, key: i32, payload: &str) -> io::Result<()> {
        // Read existing rows
        let mut rows = self.read_all_rows_from_leaf()?;

        // Check for duplicate key
        if rows.iter().any(|r| r.key == key) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Duplicate key {} not allowed", key),
            ));
        }

        // Add the new row
        rows.push(Row { key, payload: payload.to_string() });

        // Sort by key
        rows.sort_by_key(|r| r.key);

        // Attempt to write all rows back to leaf (will error if overflow)
        self.write_all_rows_to_leaf(&rows)?;
        Ok(())
    }

    /// Flush all cached pages to disk (optional utility).
    pub fn flush_all(&mut self) -> io::Result<()> {
        for i in 0..self.pager.num_pages() {
            self.pager.flush_page(i)?;
        }
        Ok(())
    }
}