use crate::storage::page::PAGE_SIZE;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub(super) struct DirtyPageSet {
    pages: HashMap<u32, [u8; PAGE_SIZE]>,
}

impl DirtyPageSet {
    pub(super) fn mark(&mut self, page_num: u32, data: [u8; PAGE_SIZE]) {
        self.pages.insert(page_num, data);
    }

    pub(super) fn snapshot(&self) -> Vec<(u32, [u8; PAGE_SIZE])> {
        self.pages
            .iter()
            .map(|(page_num, data)| (*page_num, *data))
            .collect()
    }

    pub(super) fn page_numbers(&self) -> Vec<u32> {
        self.pages.keys().copied().collect()
    }

    pub(super) fn clear(&mut self) {
        self.pages.clear();
    }
}
