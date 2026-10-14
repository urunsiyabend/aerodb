use crate::storage::page::{
    HEADER_SIZE, NODE_INTERNAL, NODE_LEAF, PAGE_SIZE, get_cell_count, get_next_leaf, get_node_type,
    get_parent, set_cell_count, set_is_root, set_next_leaf, set_node_type, set_parent,
};
use crate::storage::pager::{PageData, Pager};
use crate::storage::row::{COMMITTED_BOOTSTRAP_TX, Row, RowData};
use crate::storage::vacuum::{
    aborted_creator_is_removable, aborted_deleter_is_clearable, deleted_version_is_removable,
};
use crate::transaction::{
    Snapshot, TransactionId, TransactionStatus, TransactionTable, is_visible,
};
use log::debug;
use std::io;

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

/// A B-Tree over one root page of the shared buffer pool.
///
/// The pager is borrowed **shared** (`&Pager`): page access goes through
/// short-lived per-page latches ([`Pager::with_page`] / [`Pager::with_page_mut`])
/// rather than an exclusive borrow of the whole storage layer, so two trees — or
/// two transactions over the same tree — can be open at once and only contend on
/// the individual pages they touch.
///
/// The access discipline every method below follows: latch a page, copy out the
/// bytes needed, release the latch, *then* descend or ascend. No latch is ever
/// held across a recursive call, which is what keeps the ascending latch order
/// (and therefore deadlock freedom) trivially satisfied.
pub struct BTree<'a> {
    root_page: u32,
    pager: &'a Pager,
}

/// Where one physical row version sits: its leaf page and position in it.
#[derive(Debug, Clone, Copy)]
struct LeafSlot {
    page: u32,
    index: usize,
}

impl<'a> BTree<'a> {
    /// Open or create a B-Tree. If the file is empty, initialize page 0 as a leaf root.
    /// Otherwise, simply reuse page 0 as the existing root (leaf or internal).
    pub fn new(pager: &'a Pager) -> io::Result<Self> {
        let root_page: u32;
        if pager.file_length_pages() == 0 {
            debug!("Initializing new database: allocating page 0 as a leaf root.");
            let new_root = pager.allocate_page()?;
            pager.with_page_mut(new_root, |data| {
                set_node_type(data, NODE_LEAF);
                set_is_root(data, true);
                set_parent(data, 0);
                set_cell_count(data, 0);
                set_next_leaf(data, 0);
            })?;
            pager.flush_page(new_root)?;
            root_page = new_root;
        } else {
            debug!("Opening existing database: using page 0 as root.");
            root_page = 0;
        }
        Ok(BTree { root_page, pager })
    }

    /// Public find: legacy wrapper that returns the newest non-deleted version for a logical key.
    pub fn find(&mut self, key: i32) -> io::Result<Option<Row>> {
        debug!(
            "find() → starting at root page {} for logical key={}",
            self.root_page, key
        );

        self.find_latest_logical(key)
    }

    /// Return the newest version of `key` visible to `snapshot`.
    pub fn find_visible(&mut self, key: i32, snapshot: &Snapshot) -> io::Result<Option<Row>> {
        let versions = self.key_versions(key)?;
        Ok(self.pager.with_transaction_table(|tx_table| {
            versions
                .into_iter()
                .map(|(_, row)| row)
                .filter(|row| Self::row_visible(row, snapshot, tx_table))
                .max_by_key(|row| row.created_tx)
        }))
    }

    /// Return true when the logical key visible in `snapshot` has been changed
    /// by another transaction that was not visible to that snapshot. UPDATE and
    /// DELETE use this to provide snapshot-isolation write/write conflict
    /// detection before they mark the visible version as deleted.
    pub fn has_write_conflict(
        &mut self,
        key: i32,
        visible_created_tx: TransactionId,
        snapshot: &Snapshot,
    ) -> io::Result<bool> {
        let current_tx = snapshot.current_tx_id;
        for (_, row) in self.key_versions(key)? {
            if row.created_tx != visible_created_tx
                && Self::changed_after_snapshot(row.created_tx, snapshot)
                && Some(row.created_tx) != current_tx
            {
                return Ok(true);
            }

            if row.created_tx == visible_created_tx {
                if let Some(deleted_tx) = row.deleted_tx {
                    if Self::changed_after_snapshot(deleted_tx, snapshot)
                        && Some(deleted_tx) != current_tx
                    {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    fn changed_after_snapshot(tx_id: TransactionId, snapshot: &Snapshot) -> bool {
        tx_id >= snapshot.xmax || snapshot.active_tx_ids.binary_search(&tx_id).is_ok()
    }

    fn find_latest_logical(&mut self, key: i32) -> io::Result<Option<Row>> {
        // Physical "newest live version" lookup used by non-snapshot callers such
        // as index probes. Anchor it to the active transaction (if any) so the
        // transaction's own in-flight versions are recognised as own writes
        // instead of being hidden as uncommitted by the real transaction table.
        let snapshot = match self.pager.transaction_id() {
            Some(current) => {
                Snapshot::new_for_transaction(current, TransactionId::MAX, Vec::new())
            }
            None => Snapshot::new(TransactionId::MAX, Vec::new()),
        };
        self.find_visible(key, &snapshot)
    }

    /// Visibility wrapper over [`is_visible`] that consults the authoritative,
    /// WAL-recovered transaction table (cloned from the pager by callers) instead
    /// of fabricating commit status from snapshot boundaries. Transactions absent
    /// from the table are treated as committed/frozen by `committed_before_snapshot`,
    /// so long-committed versions stay visible after the WAL is truncated, while
    /// explicitly aborted or still-active transactions are correctly excluded.
    fn row_visible(row: &Row, snapshot: &Snapshot, tx_table: &TransactionTable) -> bool {
        is_visible(row, snapshot, tx_table)
    }

    /// Public insert: adds (key, data) into the tree.
    pub fn insert(&mut self, key: i32, data: RowData) -> io::Result<()> {
        debug!(
            "insert() → starting at root {} for key={}",
            self.root_page, key
        );

        let res = self.insert_into_page(self.root_page, key, data);
        debug!("insert() complete for key={}", key);
        res
    }

    /// Insert an already-versioned row without applying logical-key uniqueness checks.
    /// Used by MVCC update paths that append a new version for an existing key.
    pub fn insert_version(&mut self, row: Row) -> io::Result<()> {
        debug!(
            "insert_version() → starting at root {} for key={} created_tx={}",
            self.root_page, row.key, row.created_tx
        );
        self.insert_row_version_into_page(self.root_page, row)
    }

    /// Delete a logical key by marking its newest visible version as deleted.
    pub fn delete(&mut self, key: i32) -> io::Result<bool> {
        let deleted_tx = self
            .pager
            .transaction_id()
            .unwrap_or(COMMITTED_BOOTSTRAP_TX);
        self.mark_deleted(key, deleted_tx)
    }

    /// Mark the newest visible version of `key` as deleted without physically rebuilding the tree.
    pub fn mark_deleted(&mut self, key: i32, deleted_tx: TransactionId) -> io::Result<bool> {
        let snapshot = Snapshot::new(TransactionId::MAX, Vec::new());
        self.mark_deleted_visible(key, &snapshot, deleted_tx)
    }

    /// Mark the version of `key` visible to `snapshot` as deleted.
    pub fn mark_deleted_visible(
        &mut self,
        key: i32,
        snapshot: &Snapshot,
        deleted_tx: TransactionId,
    ) -> io::Result<bool> {
        let versions = self.key_versions(key)?;
        let target = self.pager.with_transaction_table(|tx_table| {
            versions
                .into_iter()
                .filter(|(_, row)| Self::row_visible(row, snapshot, tx_table))
                .max_by_key(|(_, row)| row.created_tx)
        });
        let Some((slot, target)) = target else {
            return Ok(false);
        };
        // Stamp the exact slot holding the visible version. A key's version chain
        // can span several leaves, so the leaf `key` routes to for insertion is
        // not necessarily where an older visible version lives; and versions
        // written outside a transaction all carry the bootstrap `created_tx`, so
        // (key, created_tx) alone does not identify one version. The slot stays
        // valid between `key_versions` and this re-read only because DML on one
        // table is serialized (the `Database` storage mutex); the key/created_tx
        // re-check below is a guard, not a substitute for that.
        let mut rows = self.read_all_rows_from_leaf(slot.page)?;
        let Some(row) = rows
            .get_mut(slot.index)
            .filter(|r| r.key == key && r.created_tx == target.created_tx)
        else {
            return Ok(false);
        };
        row.deleted_tx = Some(deleted_tx);
        let leaf_page = slot.page;
        self.write_all_rows_to_leaf(leaf_page, &rows)?;
        Ok(true)
    }

    /// Every physical version of `key`, paired with the slot holding it.
    ///
    /// Leaf splits cut by row count, not by key, so one key's versions can sit
    /// on both sides of a separator equal to that key. The descent therefore
    /// routes `key <= separator` to the left to reach the *first* leaf that may
    /// hold the key, then follows the leaf chain until it sees a larger key.
    /// This keeps a point lookup at O(height + versions of key) instead of a
    /// scan of the whole tree.
    fn key_versions(&self, key: i32) -> io::Result<Vec<(LeafSlot, Row)>> {
        let mut page_num = self.root_page;
        let mut data = loop {
            let data = self.pager.read_page(page_num)?;
            if get_node_type(&data) == NODE_LEAF {
                break data;
            }
            page_num = Self::leftmost_child_for_key(&data, key);
        };

        let mut versions = Vec::new();
        loop {
            let mut past_key = false;
            for (index, row) in Self::parse_leaf(&data)?.into_iter().enumerate() {
                if row.key == key {
                    versions.push((
                        LeafSlot {
                            page: page_num,
                            index,
                        },
                        row,
                    ));
                } else if row.key > key {
                    past_key = true;
                }
            }
            let next = get_next_leaf(&data);
            if past_key || next == 0 {
                return Ok(versions);
            }
            page_num = next;
            data = self.pager.read_page(page_num)?;
        }
    }

    /// Descend from `page_num` to the leaf that owns `key`.
    ///
    /// Iterative rather than recursive: each level's bytes are copied out under a
    /// latch that is released before stepping to the child, so only one page is
    /// latched at a time.
    #[cfg(test)]
    fn find_leaf_page(&self, page_num: u32, key: i32) -> io::Result<u32> {
        let mut page_num = page_num;
        loop {
            let data = self.pager.read_page(page_num)?;
            if get_node_type(&data) == NODE_LEAF {
                return Ok(page_num);
            }
            page_num = Self::child_for_key(&data, key);
        }
    }

    /// The child pointer an internal node's separators route `key` to.
    fn child_for_key(data: &PageData, key: i32) -> u32 {
        Self::route(data, |separator| key < separator)
    }

    /// Like [`Self::child_for_key`], but sends a key equal to a separator to the
    /// left child: the first leaf that may hold `key`, rather than the last.
    fn leftmost_child_for_key(data: &PageData, key: i32) -> u32 {
        Self::route(data, |separator| key <= separator)
    }

    /// Walk an internal node's separators and return the child left of the
    /// first separator for which `goes_left` holds (or the rightmost child).
    fn route(data: &PageData, goes_left: impl Fn(i32) -> bool) -> u32 {
        let cell_count = get_cell_count(data) as usize;
        let mut offset = HEADER_SIZE;
        let mut child_page = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        for _ in 0..cell_count {
            let key_i = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let right_child = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            if goes_left(key_i) {
                return child_page;
            }
            child_page = right_child;
        }
        child_page
    }

    fn leftmost_leaf_page(&self) -> io::Result<u32> {
        let mut page_num = self.root_page;
        loop {
            let data = self.pager.read_page(page_num)?;
            if get_node_type(&data) == NODE_LEAF {
                return Ok(page_num);
            }
            page_num = u32::from_le_bytes(data[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap());
        }
    }

    fn collect_all_rows(&self) -> io::Result<Vec<Row>> {
        let mut rows = Vec::new();
        let mut page_num = self.leftmost_leaf_page()?;
        loop {
            let data = self.pager.read_page(page_num)?;
            rows.extend(Self::parse_leaf(&data)?);
            let next = get_next_leaf(&data);
            if next == 0 {
                break;
            }
            page_num = next;
        }
        Ok(rows)
    }

    /// Return every physical row version, including versions that have been
    /// logically deleted. Commit-time constraint rechecks use this to validate
    /// rows written or deleted by the committing transaction.
    pub fn all_versions(&mut self) -> io::Result<Vec<Row>> {
        self.collect_all_rows()
    }

    /// Physically remove deleted row versions that are older than every active
    /// snapshot boundary. This is an explicit maintenance API; it is never
    /// called automatically by DML paths.
    pub fn vacuum_deleted_versions(
        &mut self,
        global_xmin: TransactionId,
        tx_table: &TransactionTable,
    ) -> io::Result<usize> {
        let mut removed = 0;
        let mut page_num = self.leftmost_leaf_page()?;
        loop {
            let data = self.pager.read_page(page_num)?;
            let rows = Self::parse_leaf(&data)?;
            let before = rows.len();
            let mut cleared_stamp = false;
            let kept: Vec<Row> = rows
                .into_iter()
                .filter(|row| {
                    !deleted_version_is_removable(row.deleted_tx, global_xmin, tx_table)
                        && !aborted_creator_is_removable(row.created_tx, tx_table)
                })
                .map(|mut row| {
                    if aborted_deleter_is_clearable(row.deleted_tx, tx_table) {
                        row.deleted_tx = None;
                        cleared_stamp = true;
                    }
                    row
                })
                .collect();
            removed += before - kept.len();
            if kept.len() != before || cleared_stamp {
                self.write_all_rows_to_leaf(page_num, &kept)?;
            }

            let next = get_next_leaf(&data);
            if next == 0 {
                break;
            }
            page_num = next;
        }
        Ok(removed)
    }

    fn insert_row_version_into_page(&mut self, page_num: u32, row: Row) -> io::Result<()> {
        // Walk down to the owning leaf first, one latch at a time.
        let mut page_num = page_num;
        let leaf_data = loop {
            let data = self.pager.read_page(page_num)?;
            if get_node_type(&data) == NODE_LEAF {
                break data;
            }
            page_num = Self::child_for_key(&data, row.key);
        };

        let mut rows = Self::parse_leaf(&leaf_data)?;
        rows.push(row);
        rows.sort_by_key(|r| (r.key, r.created_tx));
        match self.write_all_rows_to_leaf(page_num, &rows) {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().starts_with("Leaf overflow") => {
                self.split_leaf(page_num, rows)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Insert into the leaf that owns `key`, descending from `page_num` and
    /// splitting on overflow.
    fn insert_into_page(&mut self, page_num: u32, key: i32, data: RowData) -> io::Result<()> {
        let mut page_num = page_num;
        let leaf_data = loop {
            let page = self.pager.read_page(page_num)?;
            if get_node_type(&page) == NODE_LEAF {
                break page;
            }
            debug!("insert_into_page: at internal page {}.", page_num);
            page_num = Self::child_for_key(&page, key);
        };

        debug!("insert_into_page: at leaf page {}.", page_num);
        let mut rows = Self::parse_leaf(&leaf_data)?;

        // Keep the legacy insert semantics: a logical key may not have a currently visible row.
        if self.find_latest_logical(key)?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Duplicate key {} not allowed", key),
            ));
        }

        // Insert new logical row version and sort by logical key, then newest version.
        let mut row = Row::new(key, data);
        row.created_tx = self
            .pager
            .transaction_id()
            .unwrap_or(COMMITTED_BOOTSTRAP_TX);
        rows.push(row);
        rows.sort_by_key(|r| (r.key, r.created_tx));

        match self.write_all_rows_to_leaf(page_num, &rows) {
            Ok(()) => {
                debug!(
                    "  → Wrote {} rows to leaf {} without overflow.",
                    rows.len(),
                    page_num
                );
                Ok(())
            }
            Err(e) if e.to_string().starts_with("Leaf overflow") => {
                debug!("  → Leaf overflow at page {}! Splitting...", page_num);
                self.split_leaf(page_num, rows)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Split a leaf page that has overflowed. `all_rows` is the full (sorted) list of rows.
    ///
    /// - Redistribute half to original page (`leaf_page_num`), half to newly allocated leaf.
    /// - Let `separator_key` = first key of right half.
    /// - Call `insert_in_parent(leaf_page_num, separator_key, new_leaf_page)`.
    fn split_leaf(&mut self, leaf_page_num: u32, all_rows: Vec<Row>) -> io::Result<()> {
        let total = all_rows.len();
        let split_index = total / 2;
        let left_rows = &all_rows[..split_index];
        let right_rows = &all_rows[split_index..];

        // —————————————————————————
        // 1) Read the old next_leaf *after* the left page has been rewritten.
        //    (This ensures we fetch the correct successor in the chain.)
        // —————————————————————————
        let old_next = get_next_leaf(&self.pager.read_page(leaf_page_num)?);

        // —————————————————————————
        // 2) Allocate a new leaf page. Initialize its header properly:
        //    NODE_TYPE = LEAF, IS_ROOT = false, PARENT = 0 (temporarily),
        //    CELL_COUNT = 0 (will set shortly), next_leaf = 0.
        // —————————————————————————
        let new_leaf = self.pager.allocate_page()?;
        self.pager.with_page_mut(new_leaf, |data| {
            set_node_type(data, NODE_LEAF);
            set_is_root(data, false);
            set_parent(data, 0);
            set_cell_count(data, 0);
            // __Ensure next_leaf starts out at 0 so we know we have a blank slate__
            set_next_leaf(data, 0);
        })?;
        self.pager.flush_page(new_leaf)?;

        // —————————————————————————
        // 3) Rewrite left half back into leaf_page_num
        // —————————————————————————
        self.write_all_rows_to_leaf(leaf_page_num, left_rows)?;

        // —————————————————————————
        // 4) Write the right half into new_leaf, then flush.
        //    This preserves new_leaf.next_leaf == 0 (for now).
        // —————————————————————————
        self.write_all_rows_to_leaf(new_leaf, right_rows)?;

        // —————————————————————————
        // 5) Now fix up the leaf chain in exactly this order:
        //    (a) left_page.next_leaf = new_leaf
        //    (b) new_leaf.next_leaf = old_next
        //    and flush each change immediately, so there’s never a broken chain.
        // —————————————————————————
        // Link original leaf to new leaf
        self.pager
            .with_page_mut(leaf_page_num, |data| set_next_leaf(data, new_leaf))?;
        self.pager.flush_page(leaf_page_num)?;
        // Link new leaf to the old successor
        self.pager
            .with_page_mut(new_leaf, |data| set_next_leaf(data, old_next))?;
        self.pager.flush_page(new_leaf)?;

        // —————————————————————————
        // 6) Finally, take the first key of “right_rows” and insert it into the parent:
        // —————————————————————————
        let separator_key = right_rows[0].key;
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
        let parent_page = get_parent(&self.pager.read_page(old_page)?);

        if parent_page == 0 && old_page == self.root_page {
            debug!(
                "insert_in_parent: {} was root. Creating new internal root.",
                old_page
            );

            // old_page was the root. Create a new root internal node.
            // Allocate a fresh page to be the new root (we’ll keep old_page and new_page as children).
            let new_root = self.pager.allocate_page()?;
            self.pager.with_page_mut(new_root, |data| {
                set_node_type(data, NODE_INTERNAL);
                set_is_root(data, true);
                set_parent(data, 0); // root’s parent = 0

                // Leftmost child pointer = old_page
                data[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&old_page.to_le_bytes());

                // One separator: [separator_key][new_page]
                let offset = HEADER_SIZE + 4;
                data[offset..offset + 4].copy_from_slice(&separator_key.to_le_bytes());
                data[offset + 4..offset + 8].copy_from_slice(&new_page.to_le_bytes());

                // Set cell_count = 1
                set_cell_count(data, 1);
            })?;

            // Update parent pointers of both children (one latch at a time)
            self.pager
                .with_page_mut(old_page, |data| set_parent(data, new_root))?;
            self.pager
                .with_page_mut(new_page, |data| set_parent(data, new_root))?;

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
        let node_type = get_node_type(&self.pager.read_page(parent_page)?);
        if node_type != NODE_INTERNAL {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "insert_in_parent: parent is not internal",
            ));
        }

        // Read all existing keys and child pointers from parent
        let (mut keys, mut children) = self.read_all_from_internal(parent_page)?;

        // `new_page` is the right sibling split off `old_page`, so it goes directly
        // after `old_page` with `separator_key` between them. Positioning by key
        // comparison instead is wrong under MVCC: every UPDATE adds another
        // version of the same logical key, so separators repeat, and a key search
        // would land the new sibling *before* `old_page` among equal separators,
        // misrouting later descents away from the leaf chain's order.
        let Some(old_idx) = children.iter().position(|&child| child == old_page) else {
            return Err(io::Error::other(format!(
                "insert_in_parent: page {} is not a child of {}",
                old_page, parent_page
            )));
        };
        children.insert(old_idx + 1, new_page);
        keys.insert(old_idx, separator_key);

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
                self.pager
                    .with_page_mut(old_page, |data| set_parent(data, parent_page))?;
                self.pager
                    .with_page_mut(new_page, |data| set_parent(data, parent_page))?;
                return Ok(());
            }
            Err(e) => {
                if e.to_string().starts_with("Internal overflow") {
                    debug!(
                        "  → Internal overflow at page {}! Splitting internal node.",
                        parent_page
                    );
                    // `split_internal` re-parents only the children that move to
                    // the new right node. If `new_page` stays on the left it must
                    // already point at `parent_page`, or its own next split will
                    // not find its parent.
                    self.pager
                        .with_page_mut(new_page, |data| set_parent(data, parent_page))?;
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
        self.pager.with_page_mut(new_internal, |data| {
            set_node_type(data, NODE_INTERNAL);
            set_is_root(data, false);
            set_parent(data, 0); // will fix below
            set_cell_count(data, 0);
        })?;
        self.write_all_to_internal(new_internal, right_keys, right_children)?;

        debug!(
            "  → Allocated new internal {} and wrote {} keys.",
            new_internal,
            right_keys.len()
        );

        // Update parent pointers for all children of new_internal
        for &child in right_children {
            self.pager
                .with_page_mut(child, |data| set_parent(data, new_internal))?;
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
            self.pager.with_page_mut(new_root, |data| {
                set_node_type(data, NODE_INTERNAL);
                set_is_root(data, true);
                set_parent(data, 0); // root’s parent = 0
                set_cell_count(data, 1);

                // Leftmost child = old root (page_num)
                data[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&page_num.to_le_bytes());

                // One separator: [separator_key][new_internal]
                let off = HEADER_SIZE + 4;
                data[off..off + 4].copy_from_slice(&separator_key.to_le_bytes());
                data[off + 4..off + 8].copy_from_slice(&new_internal.to_le_bytes());
            })?;
            // Update parent pointers of the two children (one latch at a time)
            self.pager
                .with_page_mut(page_num, |data| set_parent(data, new_root))?;
            self.pager
                .with_page_mut(new_internal, |data| set_parent(data, new_root))?;
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
        let parent_page = get_parent(&self.pager.read_page(page_num)?);

        debug!(
            "  → Inserting separator {} into parent {}.",
            separator_key, parent_page
        );

        // Recursively insert into parent
        self.insert_in_parent(page_num, separator_key, new_internal)
    }

    /// Read back every row from a leaf page.  We assume `HEADER_SIZE=12`.
    fn read_all_rows_from_leaf(&self, page_num: u32) -> io::Result<Vec<Row>> {
        Self::parse_leaf(&self.pager.read_page(page_num)?)
    }

    /// Decode a leaf page image. Takes bytes rather than a page number so the
    /// caller can latch once, copy, and then parse with the latch released.
    fn parse_leaf(data: &PageData) -> io::Result<Vec<Row>> {
        if get_node_type(data) != NODE_LEAF {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "read_all_rows_from_leaf: not a leaf",
            ));
        }

        let cell_count = get_cell_count(data) as usize;
        let mut rows = Vec::with_capacity(cell_count);
        let mut offset = HEADER_SIZE; // 12

        for _ in 0..cell_count {
            // 1) 4 bytes key
            let key = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            // 2) 4 bytes payload length
            let payload_len =
                u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as usize;
            // 3) payload bytes
            let start = offset + 8;
            let end = start + payload_len;
            if end > PAGE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "read_all_rows_from_leaf: corrupt payload length",
                ));
            }
            let row = Row::deserialize_mvcc_payload(key, &data[start..end])?;

            rows.push(row);
            offset = end;
        }

        Ok(rows)
    }

    /// Write a complete sorted list of rows into a leaf page.
    ///
    /// Serialization and the overflow check happen *before* the page is latched,
    /// so an overflow (which triggers a split) never leaves a latch held, and the
    /// exclusive latch covers only the memcpy.
    fn write_all_rows_to_leaf(&mut self, page_num: u32, rows: &[Row]) -> io::Result<()> {
        // 1) Serialize once and compute the total body size (4B key + 4B length + payload).
        let payloads: Vec<Vec<u8>> = rows.iter().map(Row::serialize_mvcc_payload).collect();
        let total_size: usize = payloads.iter().map(|bytes| 8 + bytes.len()).sum();

        // 2) Check overflow: if HEADER_SIZE + total_size > PAGE_SIZE, it truly won't fit
        if HEADER_SIZE + total_size > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Leaf overflow: not enough space",
            ));
        }

        self.pager.with_page_mut(page_num, |data| {
            // 3) Zero‐out everything AFTER the 12B header (we do NOT touch offsets [0..12))
            data[HEADER_SIZE..PAGE_SIZE].fill(0);

            // 4) Pack each row at offset = HEADER_SIZE
            let mut offset = HEADER_SIZE;
            for (row, bytes) in rows.iter().zip(&payloads) {
                // 4A: 4 bytes for key
                data[offset..offset + 4].copy_from_slice(&row.key.to_le_bytes());

                // 4B: 4 bytes for payload length
                data[offset + 4..offset + 8]
                    .copy_from_slice(&(bytes.len() as u32).to_le_bytes());

                // 4C: payload bytes
                let start = offset + 8;
                let end = start + bytes.len();
                data[start..end].copy_from_slice(bytes);

                offset = end;
            }

            // 5) Update the cell_count (u16) at offsets 6..8
            set_cell_count(data, rows.len() as u16);
        })?;

        // 6) Flush so that the “next_leaf” field (bytes 8..12) is preserved
        self.pager.flush_page(page_num)?;
        Ok(())
    }

    /// Read all keys and children from an internal node into (keys, children).
    ///
    /// Returns:
    ///   keys: Vec<i32> of length = cell_count
    ///   children: Vec<u32> of length = cell_count + 1
    fn read_all_from_internal(&self, page_num: u32) -> io::Result<(Vec<i32>, Vec<u32>)> {
        let data = self.pager.read_page(page_num)?;
        if get_node_type(&data) != NODE_INTERNAL {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "read_all_from_internal: not internal",
            ));
        }

        let cell_count = get_cell_count(&data) as usize;
        let mut keys = Vec::with_capacity(cell_count);
        let mut children = Vec::with_capacity(cell_count + 1);

        // Read leftmost child
        let mut offset = HEADER_SIZE;
        children.push(u32::from_le_bytes(
            data[offset..offset + 4].try_into().unwrap(),
        ));
        offset += 4;

        for _ in 0..cell_count {
            keys.push(i32::from_le_bytes(
                data[offset..offset + 4].try_into().unwrap(),
            ));
            offset += 4;
            children.push(u32::from_le_bytes(
                data[offset..offset + 4].try_into().unwrap(),
            ));
            offset += 4;
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

        self.pager.with_page_mut(page_num, |data| {
            // Zero out body:
            data[HEADER_SIZE..PAGE_SIZE].fill(0);

            // Write leftmost child
            let mut offset = HEADER_SIZE;
            data[offset..offset + 4].copy_from_slice(&children[0].to_le_bytes());
            offset += 4;

            // Write each [key][child] pair
            for i in 0..keys.len() {
                data[offset..offset + 4].copy_from_slice(&keys[i].to_le_bytes());
                offset += 4;
                data[offset..offset + 4].copy_from_slice(&children[i + 1].to_le_bytes());
                offset += 4;
            }

            // Update cell_count
            set_cell_count(data, keys.len() as u16);
        })?;
        self.pager.flush_page(page_num)?;
        Ok(())
    }

    pub fn open_root(pager: &'a Pager, root_page: u32) -> io::Result<Self> {
        Ok(BTree { root_page, pager })
    }

    /// Return the page number of the current root node. Callers can use this
    /// after insertions to detect if the root has split.
    pub fn root_page(&self) -> u32 {
        self.root_page
    }

    pub fn scan_all_rows(&self) -> RowCursor<'a> {
        self.scan_rows_with_bounds(0, None)
    }

    /// Return one visible version per logical key for `snapshot`.
    pub fn scan_visible(&self, snapshot: &Snapshot) -> io::Result<Vec<Row>> {
        let mut rows = self.collect_all_rows()?;
        let tx_table = self.pager.transaction_table();
        rows.retain(|row| Self::row_visible(row, snapshot, &tx_table));
        rows.sort_by_key(|row| (row.key, std::cmp::Reverse(row.created_tx)));

        let mut visible = Vec::new();
        let mut last_key = None;
        for row in rows {
            if last_key == Some(row.key) {
                continue;
            }
            last_key = Some(row.key);
            visible.push(row);
        }
        Ok(visible)
    }

    pub fn scan_rows_with_bounds(&self, skip: usize, limit: Option<usize>) -> RowCursor<'a> {
        let page_num = self
            .leftmost_leaf_page()
            .expect("leftmost leaf must be reachable");
        RowCursor {
            pager: self.pager,
            current_page: page_num,
            page: self.pager.read_page(page_num).expect("leaf page readable"),
            offset: HEADER_SIZE,
            rows_in_page: 0,
            skip,
            limit,
            returned: 0,
        }
    }

    pub fn scan_rows_desc_with_bounds(&self, skip: usize, limit: Option<usize>) -> Vec<Row> {
        let mut rows: Vec<Row> = self.scan_rows_with_bounds(0, None).collect();
        rows.reverse();
        let start = skip.min(rows.len());
        let end = match limit {
            Some(l) => start + l.min(rows.len() - start),
            None => rows.len(),
        };
        rows[start..end].to_vec()
    }

    /// Flush all resident pages to disk (for final cleanup).
    pub fn flush_all(&mut self) -> io::Result<()> {
        for i in 0..self.pager.num_pages() {
            self.pager.flush_page(i)?;
        }
        Ok(())
    }
}

/// A forward scan over the leaf chain.
///
/// The cursor holds a *copy* of the leaf page it is currently walking rather
/// than a borrow of the frame, so it never keeps a page latched between calls to
/// `next()` — a scan cannot block a concurrent writer on the page it happens to
/// be sitting on.
pub struct RowCursor<'b> {
    pager: &'b Pager,
    current_page: u32,
    page: PageData,
    offset: usize,
    rows_in_page: usize,
    skip: usize,
    limit: Option<usize>,
    returned: usize,
}

impl<'b> Iterator for RowCursor<'b> {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        loop {
            let cell_count = get_cell_count(&self.page) as usize;

            if self.rows_in_page < cell_count {
                // Deserialize one row from this page
                let key =
                    i32::from_le_bytes(self.page[self.offset..self.offset + 4].try_into().unwrap());
                let payload_len = u32::from_le_bytes(
                    self.page[self.offset + 4..self.offset + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let start = self.offset + 8;
                let end = start + payload_len;
                if end > PAGE_SIZE {
                    return None;
                }
                let row = Row::deserialize_mvcc_payload(key, &self.page[start..end]).ok()?;

                // Advance offsets
                self.offset = end;
                self.rows_in_page += 1;

                // Legacy scan wrapper hides versions that have been logically deleted.
                if row.deleted_tx.is_some() {
                    continue;
                }

                if self.skip > 0 {
                    self.skip -= 1;
                    continue;
                }
                if let Some(lim) = self.limit {
                    if self.returned >= lim {
                        return None;
                    }
                }
                self.returned += 1;
                return Some(row);
            }

            // If we’re here, this leaf is exhausted. Jump to the next leaf page.
            let next_leaf = get_next_leaf(&self.page);
            if next_leaf == 0 {
                // No more leaves
                return None;
            }
            // Move into the next leaf, taking a fresh copy of its image.
            self.current_page = next_leaf;
            self.page = self.pager.read_page(next_leaf).ok()?;
            self.offset = HEADER_SIZE;
            self.rows_in_page = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn row_with_tx(key: i32, value: &str, created_tx: TransactionId) -> Row {
        let mut row = Row::new(
            key,
            RowData(vec![crate::storage::row::ColumnValue::Text(value.into())]),
        );
        row.created_tx = created_tx;
        row
    }

    #[test]
    fn write_conflict_detects_version_created_after_snapshot() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = BTree::new(&pager).unwrap();
        btree.insert_version(row_with_tx(1, "before", 1)).unwrap();
        let snapshot = Snapshot::new_for_transaction(2, 3, vec![]);
        let visible = btree.find_visible(1, &snapshot).unwrap().unwrap();

        btree.insert_version(row_with_tx(1, "after", 4)).unwrap();

        assert!(
            btree
                .has_write_conflict(1, visible.created_tx, &snapshot)
                .unwrap()
        );
    }

    #[test]
    fn write_conflict_detects_delete_after_snapshot() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = BTree::new(&pager).unwrap();
        btree.insert_version(row_with_tx(1, "before", 1)).unwrap();
        let snapshot = Snapshot::new_for_transaction(2, 3, vec![]);
        let visible = btree.find_visible(1, &snapshot).unwrap().unwrap();

        assert!(btree.mark_deleted_visible(1, &snapshot, 4).unwrap());

        assert!(
            btree
                .has_write_conflict(1, visible.created_tx, &snapshot)
                .unwrap()
        );
    }

    #[test]
    fn write_conflict_absent_for_unchanged_key() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = BTree::new(&pager).unwrap();
        btree.insert_version(row_with_tx(1, "before", 1)).unwrap();
        let snapshot = Snapshot::new_for_transaction(2, 3, vec![]);
        let visible = btree.find_visible(1, &snapshot).unwrap().unwrap();

        assert!(
            !btree
                .has_write_conflict(1, visible.created_tx, &snapshot)
                .unwrap(),
            "a key with no concurrent change must not conflict"
        );
    }

    #[test]
    fn write_conflict_detects_writer_live_at_snapshot() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = BTree::new(&pager).unwrap();
        btree.insert_version(row_with_tx(1, "before", 1)).unwrap();
        // Transaction 4 was still live when the snapshot was captured.
        let snapshot = Snapshot::new_for_transaction(2, 10, vec![4]);
        btree.insert_version(row_with_tx(1, "concurrent", 4)).unwrap();

        assert!(
            btree.has_write_conflict(1, 1, &snapshot).unwrap(),
            "a version from a concurrently-live transaction must conflict"
        );
    }

    #[test]
    fn write_conflict_ignores_own_write() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = BTree::new(&pager).unwrap();
        // The current transaction (id 3) wrote the only version; its own write
        // sits at/after the snapshot bound but must not be flagged as a conflict.
        let snapshot = Snapshot::new_for_transaction(3, 3, vec![]);
        btree.insert_version(row_with_tx(1, "self", 3)).unwrap();

        assert!(
            !btree
                .has_write_conflict(1, COMMITTED_BOOTSTRAP_TX, &snapshot)
                .unwrap(),
            "a transaction's own write must not count as a conflict"
        );
    }

    /// Keys 1..=9 with one version each, except key 5 which gets versions
    /// created by transactions 1..=60. The payload is large enough that key 5's
    /// version chain spans several leaves.
    fn tree_with_hot_key(pager: &Pager) -> BTree<'_> {
        let mut btree = BTree::new(pager).unwrap();
        let pad = "p".repeat(300);
        for key in (1..=9).filter(|&k| k != 5) {
            btree.insert_version(row_with_tx(key, &pad, 1)).unwrap();
        }
        for tx in 1..=60 {
            btree
                .insert_version(row_with_tx(5, &format!("{tx}{pad}"), tx))
                .unwrap();
        }
        btree
    }

    #[test]
    fn old_snapshot_finds_version_outside_routed_leaf() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = tree_with_hot_key(&pager);
        let routed = btree.find_leaf_page(btree.root_page, 5).unwrap();
        assert!(
            btree
                .read_all_rows_from_leaf(routed)
                .unwrap()
                .iter()
                .all(|row| row.created_tx != 2),
            "precondition: the version an old snapshot sees lives in an earlier leaf"
        );

        let snapshot = Snapshot::new_for_transaction(100, 3, vec![]);
        let visible = btree.find_visible(5, &snapshot).unwrap().unwrap();
        assert_eq!(visible.created_tx, 2);

        let latest = Snapshot::new_for_transaction(100, 101, vec![]);
        assert_eq!(
            btree.find_visible(5, &latest).unwrap().unwrap().created_tx,
            60
        );
        for key in (1..=9).filter(|&k| k != 5) {
            assert_eq!(btree.find_visible(key, &latest).unwrap().unwrap().key, key);
        }
        assert!(btree.find_visible(10, &latest).unwrap().is_none());
        assert!(btree.find_visible(0, &latest).unwrap().is_none());
    }

    #[test]
    fn mark_deleted_visible_reaches_version_outside_routed_leaf() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = tree_with_hot_key(&pager);
        let snapshot = Snapshot::new_for_transaction(100, 3, vec![]);

        assert!(btree.mark_deleted_visible(5, &snapshot, 100).unwrap());
        let stamped: Vec<_> = btree
            .collect_all_rows()
            .unwrap()
            .into_iter()
            .filter(|row| row.deleted_tx == Some(100))
            .map(|row| (row.key, row.created_tx))
            .collect();
        assert_eq!(stamped, vec![(5, 2)]);
    }

    #[test]
    fn random_order_inserts_survive_internal_splits() {
        // Random-order keys split interior children, so after an internal-node
        // split the freshly split-off sibling can land in the *left* half. Its
        // parent pointer must still be set, or its own next split cannot find
        // its parent. Large payloads keep the tree three levels deep quickly.
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = BTree::new(&pager).unwrap();
        let pad = "r".repeat(1000);
        let n: i32 = 3_000;
        // Deterministic pseudo-random order: walk a full-period LCG over 0..2^14
        // and keep the values below `n`.
        let mut keys = Vec::with_capacity(n as usize);
        let mut x: u32 = 1;
        while keys.len() < n as usize {
            x = x.wrapping_mul(1_103_515_245).wrapping_add(12_345) & 0x3FFF;
            if (x as i32) < n {
                keys.push(x as i32);
            }
        }
        let mut seen = std::collections::HashSet::new();
        keys.retain(|k| seen.insert(*k));
        for &key in &keys {
            btree.insert_version(row_with_tx(key, &pad, 1)).unwrap();
        }
        let latest = Snapshot::new_for_transaction(100, 101, vec![]);
        for &key in &keys {
            assert!(
                btree.find_visible(key, &latest).unwrap().is_some(),
                "key {key} lost"
            );
        }
        assert_eq!(btree.collect_all_rows().unwrap().len(), keys.len());
    }

    #[test]
    fn repeated_delete_insert_outside_transaction_targets_live_version() {
        // Catalog/sequence rows are rewritten outside any transaction, so every
        // version shares the bootstrap created_tx. Each delete must stamp the
        // live version, not an already-deleted twin.
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = BTree::new(&pager).unwrap();
        let value = |n: i32| RowData(vec![crate::storage::row::ColumnValue::Integer(n)]);
        btree.insert(1, value(0)).unwrap();
        for n in 1..=5 {
            assert!(btree.delete(1).unwrap());
            btree.insert(1, value(n)).unwrap();
        }
        let live = btree.find(1).unwrap().unwrap();
        assert_eq!(live.data, value(5));
    }

    #[test]
    fn vacuum_prunes_only_committed_deletes_below_global_xmin() {
        let file = NamedTempFile::new().unwrap();
        let pager = Pager::new(file.path().to_str().unwrap()).unwrap();
        let mut btree = BTree::new(&pager).unwrap();
        let mut removable = row_with_tx(1, "old", 1);
        removable.deleted_tx = Some(3);
        let mut active_delete = row_with_tx(2, "active-delete", 1);
        active_delete.deleted_tx = Some(4);
        let mut protected = row_with_tx(3, "protected", 1);
        protected.deleted_tx = Some(7);
        btree.insert_version(removable).unwrap();
        btree.insert_version(active_delete).unwrap();
        btree.insert_version(protected).unwrap();

        let tx_table = TransactionTable::from([
            (3, TransactionStatus::Committed(1)),
            (4, TransactionStatus::Active),
            (7, TransactionStatus::Committed(2)),
        ]);

        assert_eq!(btree.vacuum_deleted_versions(6, &tx_table).unwrap(), 1);
        let rows = btree.collect_all_rows().unwrap();
        assert!(rows.iter().all(|row| row.key != 1));
        assert!(rows.iter().any(|row| row.key == 2));
        assert!(rows.iter().any(|row| row.key == 3));
    }
}
