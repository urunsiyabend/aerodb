use std::io;
use log::debug;
use crate::storage::pager::Pager;
use crate::storage::page::{get_cell_count, get_node_type, set_cell_count, set_node_type, set_parent, set_is_root, NODE_LEAF, HEADER_SIZE, PAGE_SIZE, NODE_INTERNAL, get_parent};

/// A single row in our leaf: (key, payload).
#[derive(Debug, Clone)]
pub struct Row {
    pub key: i32,
    pub payload: Vec<u8>, // store raw bytes, not a UTF-8 string
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


/// A B-Tree that can grow to arbitrary height by splitting leaves and internal nodes.
/// Internally, pages with NODE_TYPE=NODE_LEAF store multiple rows; pages with NODE_TYPE=NODE_INTERNAL
/// store multiple separator keys and child pointers.
///
/// Page format is:
///   [0..8) = header (NODE_TYPE, IS_ROOT, PARENT, CELL_COUNT)
///   [8..]   = body
///
/// LEAF body: a sequence of cells:
///   for each cell:
///     [4B key][4B payload_len][payload_bytes]
///
/// INTERNAL body:
///   offset = HEADER_SIZE
///   [4B leftmost_child_page]
///   for i = 0..cell_count-1:
///     [4B separator_key_i][4B child_page_i]
///
///   number of children = cell_count + 1.
///
///—————————————————————————————————————————————————————————————————————————————————————————————
/// On insert:
///   1. Descend from root to appropriate leaf.
///   2. In leaf, read all rows, insert new row, sort by key, try to write.
///   3. If leaf overflows, split it:
///        • Allocate new leaf page,
///        • Distribute rows (left half, right half),
///        • Write both leaf pages,
///        • Call insert_in_parent for (old_leaf, separator_key, new_leaf).
///   4. insert_in_parent tries to insert (separator_key, new_leaf) into parent internal node:
///        • If the parent is a leaf (i.e. old root was leaf), root was page 0:
///            – Turn page 0 into an internal node with two children.
///        • Otherwise, read internal node’s keys & children, insert the new separator & child,
///            – If fits, write it; if overflows, split internal and recurse up.
///   5. If you reach the root and it overflows, allocate a new root page, make it internal,
///        with two children (old_root and new_internal), and update their parent pointers.
///—————————————————————————————————————————————————————————————————————————————————————————————



pub struct BTree<'a> {
    root_page: u32,
    pager: &'a mut Pager,
}

impl<'a> BTree<'a> {
    /// Open or create a B-Tree. If the file is empty, initialize page 0 as a leaf root.
    /// Otherwise, simply reuse page 0 as the existing root (leaf or internal).
    pub fn new(pager: &'a mut Pager) -> io::Result<BTree<'a>> {
        let root_page: u32;

        if pager.file_length_pages() == 0 {
            debug!("Initializing new database: allocating page 0 as leaf root.");

            // File empty: allocate page 0 and make it an empty leaf.
            let new_root = pager.allocate_page()?;
            let page = pager.get_page(new_root)?;
            set_node_type(&mut page.data, NODE_LEAF);
            set_is_root(&mut page.data, true);
            set_parent(&mut page.data, 0);
            set_cell_count(&mut page.data, 0);
            pager.flush_page(new_root)?;
            root_page = new_root;
        } else {
            debug!("Opening existing database: using page 0 as root.");

            // File already has ≥1 page: reuse page 0 as root.
            root_page = 0;
        }

        Ok(BTree { root_page, pager })
    }

    /// Public find: returns Some(Row) if found, else None.
    pub fn find(&mut self, key: i32) -> io::Result<Option<Row>> {
        debug!(
            "find() → starting at root page {} for key={}",
            self.root_page, key
        );

        self.find_in_page(self.root_page, key)
    }

    /// Recursive helper to find a key starting at page `page_num`.
    fn find_in_page(&mut self, page_num: u32, key: i32) -> io::Result<Option<Row>> {
        let page = self.pager.get_page(page_num)?;
        let node_type = get_node_type(&page.data);

        if node_type == NODE_LEAF {
            debug!("find_in_page: at leaf page {}.", page_num);

            // Leaf: scan all rows
            let rows = self.read_all_rows_from_leaf(page_num)?;
            for row in rows {
                if row.key == key {
                    debug!("  → Found key={} in leaf {}", key, page_num);
                    return Ok(Some(row));
                }
            }
            debug!("  → Key={} not found in leaf {}", key, page_num);
            return Ok(None);
        }

        debug!("find_in_page: at internal page {}.", page_num);
        // Otherwise internal node:
        let cell_count = get_cell_count(&page.data) as usize;

        // Read leftmost child pointer at offset HEADER_SIZE
        let mut offset = HEADER_SIZE;
        let leftmost_child_bytes = &page.data[offset..offset + 4];
        let mut child_page = u32::from_le_bytes(leftmost_child_bytes.try_into().unwrap());
        offset += 4;

        // For each separator key / child_i pair:
        for _ in 0..cell_count {
            // Read separator key_i
            let key_i_bytes = &page.data[offset..offset + 4];
            let key_i = i32::from_le_bytes(key_i_bytes.try_into().unwrap());
            offset += 4;

            // Read child_page_i
            let child_i_bytes = &page.data[offset..offset + 4];
            let right_child = u32::from_le_bytes(child_i_bytes.try_into().unwrap());
            offset += 4;

            if key < key_i {
                debug!(
                    "  → Descending to child {} (key < {})",
                    child_page, key_i
                );
                // Descend to `child_page`
                return self.find_in_page(child_page, key);
            } else {
                // Descend to `right_child`
                child_page = right_child;
            }
        }
        debug!(
            "  → Descending to rightmost child {} (no larger separator)",
            child_page
        );

        // If no separator was larger, descend to last child
        self.find_in_page(child_page, key)
    }

    /// Public insert: adds (key, payload) into the tree.
    pub fn insert(&mut self, key: i32, payload: &[u8]) -> io::Result<()> {
        debug!(
            "============================================\n\
             insert() → starting at root {} for key={}",
            self.root_page, key
        );


        // Start at root
        let res = self.insert_into_page(self.root_page, key, payload);
        debug!("insert() complete for key={}\n============================================", key);
        res
    }

    /// Recursive helper to insert into page `page_num`. May split leaf or internal pages.
    fn insert_into_page(&mut self, page_num: u32, key: i32, payload: &[u8]) -> io::Result<()> {
        let page = self.pager.get_page(page_num)?;
        let node_type = get_node_type(&page.data);

        if node_type == NODE_LEAF {
            debug!("insert_into_page: at leaf page {}.", page_num);

            // Read all existing rows
            let mut rows = self.read_all_rows_from_leaf(page_num)?;

            // Check duplicate
            if rows.iter().any(|r| r.key == key) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Duplicate key {} not allowed", key),
                ));
            }

            // Insert new row and sort
            rows.push(Row { key, payload: payload.to_vec() });
            rows.sort_by_key(|r| r.key);

            // Try writing back to leaf
            match self.write_all_rows_to_leaf(page_num, &rows) {
                Ok(()) => {
                    debug!(
                        "  → Wrote {} rows to leaf {} without overflow.",
                        rows.len(),
                        page_num
                    );
                    return Ok(());
                }
                Err(e) => {
                    if e.to_string().starts_with("Leaf overflow") {
                        debug!("  → Leaf overflow at page {}! Splitting...", page_num);

                        self.split_leaf(page_num, rows)?;
                        return Ok(());
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        debug!("insert_into_page: at internal page {}.", page_num);
        // If internal node, find the correct child to descend into
        let cell_count = get_cell_count(&page.data) as usize;
        let mut offset = HEADER_SIZE;
        let leftmost_child_bytes = &page.data[offset..offset + 4];
        let mut child_page = u32::from_le_bytes(leftmost_child_bytes.try_into().unwrap());
        offset += 4;

        for _ in 0..cell_count {
            let key_i_bytes = &page.data[offset..offset + 4];
            let key_i = i32::from_le_bytes(key_i_bytes.try_into().unwrap());
            offset += 4;

            let child_i_bytes = &page.data[offset..offset + 4];
            let right_child = u32::from_le_bytes(child_i_bytes.try_into().unwrap());
            offset += 4;

            if key < key_i {
                debug!(
                    "  → Inserting into left child {} of internal {}.",
                    child_page, page_num
                );

                return self.insert_into_page(child_page, key, payload);
            } else {
                child_page = right_child;
            }
        }

        debug!(
            "  → Inserting into rightmost child {} of internal {}.",
            child_page, page_num
        );

        // Descend to last child
        self.insert_into_page(child_page, key, payload)
    }

    /// Split a leaf page that has overflowed. `all_rows` is the full (sorted) list of rows.
    ///
    /// - Redistribute half to original page (`leaf_page_num`), half to newly allocated leaf.
    /// - Let `separator_key` = first key of right half.
    /// - Call `insert_in_parent(leaf_page_num, separator_key, new_leaf_page)`.
    fn split_leaf(&mut self, leaf_page_num: u32, all_rows: Vec<Row>) -> io::Result<()> {
        debug!(
            "split_leaf: splitting leaf {} with {} total rows.",
            leaf_page_num,
            all_rows.len()
        );

        let total = all_rows.len();
        let split_index = total / 2;

        let left_rows = &all_rows[..split_index];
        let right_rows = &all_rows[split_index..];

        debug!(
            "  → Left half size: {}, Right half size: {}.",
            left_rows.len(),
            right_rows.len()
        );

        // Rewrite left_rows into original leaf
        self.write_all_rows_to_leaf(leaf_page_num, left_rows)?;

        debug!(
            "  → Wrote {} rows back to leaf {}.",
            left_rows.len(),
            leaf_page_num
        );

        // Allocate new leaf for the right half
        let new_leaf = self.pager.allocate_page()?;
        {
            let p = self.pager.get_page(new_leaf)?;
            set_node_type(&mut p.data, NODE_LEAF);
            set_is_root(&mut p.data, false);
            set_parent(&mut p.data, 0); // will set properly below
            set_cell_count(&mut p.data, 0);
        }
        self.write_all_rows_to_leaf(new_leaf, right_rows)?;

        debug!(
            "  → Allocated new leaf {} and wrote {} rows.",
            new_leaf,
            right_rows.len()
        );

        // Get separator key = first key of right half
        let separator_key = right_rows[0].key;

        debug!("  → Separator key for parent: {}.", separator_key);

        // Insert into parent (could be root or deeper)
        self.insert_in_parent(leaf_page_num, separator_key, new_leaf)
    }

    /// Insert a new (separator_key, new_child_page) entry into the parent of `old_page`.
    ///
    /// If `old_page` was the root, create a new root (internal) at page 0 or a newly allocated page.
    /// Otherwise, read the parent internal node, insert the new separator/child, and split if needed.
    fn insert_in_parent(
        &mut self,
        old_page: u32,
        separator_key: i32,
        new_page: u32,
    ) -> io::Result<()> {
        // Find parent page number
        let parent_page = get_parent(&self.pager.get_page(old_page)?.data);

        if parent_page == 0 && old_page == self.root_page {
            debug!(
                "insert_in_parent: {} was root. Creating new internal root.",
                old_page
            );

            // old_page was the root. Create a new root internal node.
            // Allocate a fresh page to be the new root (we’ll keep old_page and new_page as children).
            let new_root = self.pager.allocate_page()?;
            {
                let root = self.pager.get_page(new_root)?;
                set_node_type(&mut root.data, NODE_INTERNAL);
                set_is_root(&mut root.data, true);
                set_parent(&mut root.data, 0); // root’s parent = 0

                // Leftmost child pointer = old_page
                root.data[HEADER_SIZE..HEADER_SIZE + 4]
                    .copy_from_slice(&old_page.to_le_bytes());

                // One separator: [separator_key][new_page]
                let offset = HEADER_SIZE + 4;
                root.data[offset..offset + 4]
                    .copy_from_slice(&separator_key.to_le_bytes());
                root.data[offset + 4..offset + 8]
                    .copy_from_slice(&new_page.to_le_bytes());

                // Set cell_count = 1
                set_cell_count(&mut root.data, 1);
            }

            // Update parent pointers of both children
            {
                let p_old = self.pager.get_page(old_page)?;
                set_parent(&mut p_old.data, new_root);
            }
            {
                let p_new = self.pager.get_page(new_page)?;
                set_parent(&mut p_new.data, new_root);
            }

            // Flush new root and children
            self.pager.flush_page(new_root)?;
            self.pager.flush_page(old_page)?;
            self.pager.flush_page(new_page)?;

            // Update BTree’s root_page
            self.root_page = new_root;

            debug!("  → New root is page {}.", new_root);

            return Ok(());
        }

        if parent_page == 0 {
            // This can also happen if old_page is a non-root leaf whose parent is page 0
            // and page 0 is currently a leaf. But that “leaf” scenario was handled in split_leaf.
            // So here parent_page=0 means page 0 is internal.
        }

        debug!(
            "insert_in_parent: inserting into existing internal parent {}.",
            parent_page
        );

        // Otherwise, parent_page > 0, and we know it’s an internal node.
        let mut parent = self.pager.get_page(parent_page)?;
        let node_type = get_node_type(&parent.data);
        if node_type != NODE_INTERNAL {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "insert_in_parent: parent is not internal",
            ));
        }

        // Read all existing keys and child pointers from parent
        let (mut keys, mut children) = self.read_all_from_internal(parent_page)?;

        // Find where separator_key belongs in `keys`, and insert new_page accordingly
        // `children.len() = keys.len() + 1`. For insertion, find index i such that keys[i] > separator_key,
        // then new children list is children[0..i+1] + [new_page] + children[i+1..].
        let mut insert_idx = 0;
        while insert_idx < keys.len() && separator_key > keys[insert_idx] {
            insert_idx += 1;
        }
        // Now insert at insert_idx:
        children.insert(insert_idx + 1, new_page);
        keys.insert(insert_idx, separator_key);

        debug!(
            "  → Parent {} before write has {} keys and {} children.",
            parent_page,
            keys.len(),
            children.len()
        );

        // Try writing back to this internal node
        match self.write_all_to_internal(parent_page, &keys, &children) {
            Ok(()) => {
                debug!(
                    "  → Inserted separator {} into parent {} without overflow.",
                    separator_key, parent_page
                );
                {
                    let c1 = self.pager.get_page(old_page)?;
                    set_parent(&mut c1.data, parent_page);
                }
                {
                    let c2 = self.pager.get_page(new_page)?;
                    set_parent(&mut c2.data, parent_page);
                }
                return Ok(());
            }
            Err(e) => {
                if e.to_string().starts_with("Internal overflow") {
                    debug!(
                        "  → Internal overflow at page {}! Splitting internal node.",
                        parent_page
                    );
                    return self.split_internal(parent_page, keys, children);
                } else {
                    return Err(e);
                }
            }
        }
    }

    /// Split an internal node at `page_num`. `keys` and `children` are the full lists post-insertion.
    ///
    /// We divide them roughly in half:
    ///   left_keys = keys[0..mid], right_keys = keys[mid+1..]
    ///   left_children = children[0..mid+1], right_children = children[mid+1..]
    ///   The middle key (keys[mid]) is “pushed up” to the parent via insert_in_parent.
    fn split_internal(
        &mut self,
        page_num: u32,
        keys: Vec<i32>,
        children: Vec<u32>,
    ) -> io::Result<()> {
        debug!(
            "split_internal: splitting internal page {} with {} keys.",
            page_num,
            keys.len()
        );

        let total_keys = keys.len();
        let mid_index = total_keys / 2;

        // The separator to push up
        let separator_key = keys[mid_index];

        // Left half:
        let left_keys = &keys[..mid_index];
        let left_children = &children[..(mid_index + 1)];

        // Right half:
        let right_keys = &keys[(mid_index + 1)..];
        let right_children = &children[(mid_index + 1)..];

        // Rewrite the current page (page_num) as an internal node containing left_keys/left_children
        self.write_all_to_internal(page_num, left_keys, left_children)?;

        debug!(
            "  → Wrote {} keys to left internal {}.",
            left_keys.len(),
            page_num
        );

        // Allocate a new internal page for the right half
        let new_internal = self.pager.allocate_page()?;
        {
            let ni = self.pager.get_page(new_internal)?;
            set_node_type(&mut ni.data, NODE_INTERNAL);
            set_is_root(&mut ni.data, false);
            set_parent(&mut ni.data, 0); // will fix below
            set_cell_count(&mut ni.data, 0);
        }
        self.write_all_to_internal(new_internal, right_keys, right_children)?;

        debug!(
            "  → Allocated new internal {} and wrote {} keys.",
            new_internal,
            right_keys.len()
        );

        // Update parent pointers for all children of new_internal
        for &child in right_children {
            let c = self.pager.get_page(child)?;
            set_parent(&mut c.data, new_internal);
        }

        // Now we must insert (separator_key, new_internal) into the parent of `page_num`.
        // But if `page_num` was the root, we need to create a new root first.
        if page_num == self.root_page {
            debug!(
                "  → Splitting root internal {}. Creating new root.",
                page_num
            );

            // Create a brand-new root (internal) at a fresh page
            let new_root = self.pager.allocate_page()?;
            {
                let nr = self.pager.get_page(new_root)?;
                set_node_type(&mut nr.data, NODE_INTERNAL);
                set_is_root(&mut nr.data, true);
                set_parent(&mut nr.data, 0); // root’s parent = 0
                set_cell_count(&mut nr.data, 1);

                // Leftmost child = old root (page_num)
                nr.data[HEADER_SIZE..HEADER_SIZE + 4]
                    .copy_from_slice(&page_num.to_le_bytes());

                // One separator: [separator_key][new_internal]
                let off = HEADER_SIZE + 4;
                nr.data[off..off + 4].copy_from_slice(&separator_key.to_le_bytes());
                nr.data[off + 4..off + 8].copy_from_slice(&new_internal.to_le_bytes());
            }
            // Update parent pointers of the two children
            {
                let p_old = self.pager.get_page(page_num)?;
                set_parent(&mut p_old.data, new_root);
            }
            {
                let p_new = self.pager.get_page(new_internal)?;
                set_parent(&mut p_new.data, new_root);
            }
            // Flush pages
            self.pager.flush_page(page_num)?;
            self.pager.flush_page(new_internal)?;
            self.pager.flush_page(new_root)?;
            // Update root_page
            self.root_page = new_root;

            debug!("  → New root page is {}.", new_root);

            return Ok(());
        }

        // Otherwise, normal case: insert separator into parent of page_num
        let parent_page = get_parent(&self.pager.get_page(page_num)?.data);

        debug!(
            "  → Inserting separator {} into parent {}.",
            separator_key, parent_page
        );

        // Recursively insert into parent
        self.insert_in_parent(page_num, separator_key, new_internal)
    }

    /// Read all rows from a leaf page into a Vec<Row>.
    fn read_all_rows_from_leaf(&mut self, page_num: u32) -> io::Result<Vec<Row>> {
        let page = self.pager.get_page(page_num)?;
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
            // 4B key
            let key_bytes = &page.data[offset..offset + 4];
            let key = i32::from_le_bytes(key_bytes.try_into().unwrap());

            // 4B payload length
            let len_bytes = &page.data[offset + 4..offset + 8];
            let payload_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

            // payload bytes
            let start = offset + 8;
            let end = start + payload_len;
            if end > PAGE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "read_all_rows_from_leaf: corrupt payload length",
                ));
            }
            let payload_bytes = &page.data[start..end];
            // Copy into a Vec<u8>
            let payload = payload_bytes.to_vec();

            rows.push(Row { key, payload });
            offset = end;
        }
        Ok(rows)
    }

    /// Write a complete sorted list of rows into a leaf page.
    fn write_all_rows_to_leaf(&mut self, page_num: u32, rows: &[Row]) -> io::Result<()> {
        let page = self.pager.get_page(page_num)?;

        // Compute total size of all (key + length + payload_bytes)
        let mut total_size = 0;
        for row in rows {
            total_size += 4;                // 4 bytes for key
            total_size += 4;                // 4 bytes for payload_len
            total_size += row.payload.len(); // payload_bytes.len()
        }

        if HEADER_SIZE + total_size > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Leaf overflow: not enough space",
            ));
        }

        // Zero out page body
        for idx in HEADER_SIZE..PAGE_SIZE {
            page.data[idx] = 0;
        }

        // Write each [ key (4B) | payload_len (4B) | payload_bytes ]
        let mut offset = HEADER_SIZE;
        for row in rows {
            // 4B key (little‐endian)
            let key_bytes = row.key.to_le_bytes();
            page.data[offset..offset + 4].copy_from_slice(&key_bytes);

            // 4B payload length
            let payload_len = row.payload.len() as u32;
            let len_bytes = payload_len.to_le_bytes();
            page.data[offset + 4..offset + 8].copy_from_slice(&len_bytes);

            // payload bytes themselves
            let start = offset + 8;
            let end = start + row.payload.len();
            page.data[start..end].copy_from_slice(&row.payload);

            offset = end;
        }

        // Update cell count
        set_cell_count(&mut page.data, rows.len() as u16);
        self.pager.flush_page(page_num)?;
        Ok(())
    }

    /// Read all keys and children from an internal node into (keys, children).
    ///
    /// Returns:
    ///   keys: Vec<i32> of length = cell_count
    ///   children: Vec<u32> of length = cell_count + 1
    fn read_all_from_internal(&mut self, page_num: u32) -> io::Result<(Vec<i32>, Vec<u32>)> {
        let page = self.pager.get_page(page_num)?;
        let node_type = get_node_type(&page.data);
        if node_type != NODE_INTERNAL {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "read_all_from_internal: not internal",
            ));
        }

        let cell_count = get_cell_count(&page.data) as usize;
        let mut keys = Vec::with_capacity(cell_count);
        let mut children = Vec::with_capacity(cell_count + 1);

        // Read leftmost child
        let mut offset = HEADER_SIZE;
        let leftmost_bytes = &page.data[offset..offset + 4];
        let leftmost = u32::from_le_bytes(leftmost_bytes.try_into().unwrap());
        children.push(leftmost);
        offset += 4;

        for _ in 0..cell_count {
            let key_bytes = &page.data[offset..offset + 4];
            let key = i32::from_le_bytes(key_bytes.try_into().unwrap());
            offset += 4;
            let child_bytes = &page.data[offset..offset + 4];
            let child = u32::from_le_bytes(child_bytes.try_into().unwrap());
            offset += 4;

            keys.push(key);
            children.push(child);
        }

        Ok((keys, children))
    }

    /// Write a complete internal node given `keys` and `children`.
    ///
    /// children.len() must equal keys.len() + 1.
    fn write_all_to_internal(
        &mut self,
        page_num: u32,
        keys: &[i32],
        children: &[u32],
    ) -> io::Result<()> {
        if children.len() != keys.len() + 1 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "write_all_to_internal: children length must be keys length + 1",
            ));
        }

        // Compute required size:
        // HEADER_SIZE + 4 (leftmost child) + keys.len()*(4+4)
        let required = HEADER_SIZE + 4 + keys.len() * 8;
        if required > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Internal overflow: not enough space",
            ));
        }

        let page = self.pager.get_page(page_num)?;
        // Zero out body:
        for idx in HEADER_SIZE..PAGE_SIZE {
            page.data[idx] = 0;
        }

        // Write leftmost child
        let mut offset = HEADER_SIZE;
        let left_child_bytes = children[0].to_le_bytes();
        page.data[offset..offset + 4].copy_from_slice(&left_child_bytes);
        offset += 4;

        // Write each [key][child] pair
        for i in 0..keys.len() {
            let key_bytes = keys[i].to_le_bytes();
            page.data[offset..offset + 4].copy_from_slice(&key_bytes);
            offset += 4;
            let child_bytes = children[i + 1].to_le_bytes();
            page.data[offset..offset + 4].copy_from_slice(&child_bytes);
            offset += 4;
        }

        // Update cell_count
        set_cell_count(&mut page.data, keys.len() as u16);
        self.pager.flush_page(page_num)?;
        Ok(())
    }

    pub fn open_root(pager: &'a mut Pager, root_page: u32) -> io::Result<BTree<'a>> {
        Ok(BTree { root_page, pager })
    }

    pub fn scan_all_rows(&'a mut self) -> RowCursor<'a> {
        // Find the leftmost leaf: start at root, follow leftmost child until a leaf.
        let mut page_num = self.root_page;
        loop {
            let page = self.pager.get_page(page_num).unwrap();
            let node_type = get_node_type(&page.data);
            if node_type == NODE_LEAF {
                break;
            }
            // internal: read leftmost child pointer
            let left_child_bytes = &page.data[HEADER_SIZE..HEADER_SIZE + 4];
            page_num = u32::from_le_bytes(left_child_bytes.try_into().unwrap());
        }
        RowCursor {
            btree: self,
            current_page: page_num,
            offset: HEADER_SIZE,
            rows_in_page: 0,
        }
    }

    /// Flush all cached pages to disk (for final cleanup).
    pub fn flush_all(&mut self) -> io::Result<()> {
        for i in 0..self.pager.num_pages() {
            self.pager.flush_page(i)?;
        }
        Ok(())
    }
}

pub struct RowCursor<'b> {
    btree: &'b mut BTree<'b>,
    current_page: u32,
    offset: usize,
    rows_in_page: usize,
}

impl<'a> Iterator for RowCursor<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        loop {
            let page = self.btree.pager.get_page(self.current_page).ok()?;
            let cell_count = get_cell_count(&page.data) as usize;

            if self.rows_in_page < cell_count {
                // Deserialize the next row at `offset`
                let key_bytes = &page.data[self.offset..self.offset + 4];
                let key = i32::from_le_bytes(key_bytes.try_into().unwrap());
                let len_bytes = &page.data[self.offset + 4..self.offset + 8];
                let payload_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
                let start = self.offset + 8;
                let end = start + payload_len;
                if end > PAGE_SIZE {
                    return None;
                }
                let payload_bytes = &page.data[start..end];
                let payload = payload_bytes.to_vec(); // Vec<u8>
                let row = Row { key, payload };

                // Advance cursor
                self.offset = end;
                self.rows_in_page += 1;
                return Some(row);
            }

            // If we’ve yielded all rows in this page, move to the “next” leaf.
            // We assume a simple linked‐list: the rightmost 4 bytes of header at offset (PAGE_SIZE-4)
            // store the “next_leaf_page” (u32), or 0 if none. We didn’t set that earlier, so for now
            // we’ll just stop here. (Implementing sibling pointers is the next step.)
            // For simplicity, let’s assume no sibling pointers; just stop iteration.
            return None;
        }
    }
}