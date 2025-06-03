// ┌─────────────────────────────────────────────────────────────────────────┐
// │ Offset │ Length │ Description                                           │
// │────────┼────────┼───────────────────────────────────────────────────────│
// │   0    │   1    │ NODE_TYPE (0 = internal, 1 = leaf)                    │
// │   1    │   1    │ IS_ROOT   (0 = false, 1 = true)                       │
// │   2    │   4    │ PARENT_PAGE (u32): page number of parent (0 if none)  │
// │   6    │   2    │ CELL_COUNT:  number of cells in this node (u16)       │
// │────────┼────────┼───────────────────────────────────────────────────────│
// │   8    │  (PAGE_SIZE - 8)  ┆ Cells/rows                                 │
// └─────────────────────────────────────────────────────────────────────────│

// ┌───────────────────────────────────────────────────────────────────────┐
// │ Offset (from start of page)  │ Length │ Description                   │
// │──────────────────────────────┼────────┼───────────────────────────────│
// │        8 (HEADER_SIZE)       │   4    │ KEY   (i32, little‐endian)    │
// │        12                    │   4    │ PAYLOAD_LEN (u32)             │
// │        16                    │   N    │ PAYLOAD_BYTES (UTF-8 string)  │
// └───────────────────────────────────────────────────────────────────────┘

pub const PAGE_SIZE: usize = 4096;

pub const NODE_TYPE_OFFSET: usize   = 0;          // 1 byte
pub const IS_ROOT_OFFSET: usize     = 1;          // 1 byte
pub const PARENT_PAGE_OFFSET: usize = 2;          // 4 bytes (u32)
pub const CELL_COUNT_OFFSET: usize  = 6;          // 2 bytes (u16)
pub const HEADER_SIZE: usize        = 8;          // total header length

pub const NODE_INTERNAL: u8 = 0;
pub const NODE_LEAF: u8     = 1;

/// Given a raw page buffer, read its node type (internal vs. leaf).
pub fn get_node_type(page: &[u8; PAGE_SIZE]) -> u8 {
    page[NODE_TYPE_OFFSET]
}

/// Set the node type (internal=0, leaf=1).
pub fn set_node_type(page: &mut [u8; PAGE_SIZE], node_type: u8) {
    page[NODE_TYPE_OFFSET] = node_type;
}

/// Read the “is_root” flag (0 = false, 1 = true).
pub fn get_is_root(page: &[u8; PAGE_SIZE]) -> u8 {
    page[IS_ROOT_OFFSET]
}

/// Set or clear the “is_root” flag.
pub fn set_is_root(page: &mut [u8; PAGE_SIZE], is_root: bool) {
    page[IS_ROOT_OFFSET] = if is_root { 1 } else { 0 };
}

/// Read the parent page number (u32).
pub fn get_parent(page: &[u8; PAGE_SIZE]) -> u32 {
    let bytes = &page[PARENT_PAGE_OFFSET..PARENT_PAGE_OFFSET + 4];
    u32::from_le_bytes(bytes.try_into().unwrap())
}

/// Set the parent page number (u32).
pub fn set_parent(page: &mut [u8; PAGE_SIZE], parent: u32) {
    page[PARENT_PAGE_OFFSET..PARENT_PAGE_OFFSET + 4]
        .copy_from_slice(&parent.to_le_bytes());
}

/// Read the number of cells in this node (u16).
pub fn get_cell_count(page: &[u8; PAGE_SIZE]) -> u16 {
    let bytes = &page[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2];
    u16::from_le_bytes(bytes.try_into().unwrap())
}

/// Set the number of cells (u16).
pub fn set_cell_count(page: &mut [u8; PAGE_SIZE], count: u16) {
    page[CELL_COUNT_OFFSET..CELL_COUNT_OFFSET + 2]
        .copy_from_slice(&count.to_le_bytes());
}
