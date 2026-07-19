use std::collections::BTreeSet;

use super::{IsolationLevel, Snapshot, TransactionId};

/// A write this transaction performed against a logical key, recorded so commit
/// can re-validate first-committer-wins: if another transaction committed a
/// conflicting change to the same key after this transaction's snapshot, the
/// committer loses and aborts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteIntent {
    pub table_root: u32,
    pub key: i32,
    /// The `created_tx` of the version this write was based on (the one visible
    /// to this transaction's snapshot when it wrote).
    pub visible_created_tx: TransactionId,
}

/// One live transaction's private session state.
///
/// Under version-in-place (Postgres-style) MVCC, a transaction's new row
/// versions are written directly into the shared B-Tree pages, stamped with this
/// transaction's id; visibility hides them from everyone else until commit. So
/// the transaction does not buffer private page images — it only remembers which
/// pages it touched (to flush their current image at commit) and which logical
/// keys it wrote (to re-validate write/write conflicts at commit).
#[derive(Debug)]
pub struct Transaction {
    id: TransactionId,
    snapshot: Snapshot,
    name: Option<String>,
    isolation_level: IsolationLevel,
    /// Pages this transaction has written; their current cache image is flushed
    /// (durably) at commit.
    touched_pages: BTreeSet<u32>,
    /// Logical keys this transaction updated/deleted, for commit-time conflict
    /// re-validation.
    write_set: Vec<WriteIntent>,
}

impl Transaction {
    pub fn new(
        id: TransactionId,
        snapshot: Snapshot,
        name: Option<String>,
        isolation_level: IsolationLevel,
    ) -> Self {
        Self {
            id,
            snapshot,
            name,
            isolation_level,
            touched_pages: BTreeSet::new(),
            write_set: Vec::new(),
        }
    }

    pub fn id(&self) -> TransactionId {
        self.id
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Note that this transaction wrote `page_num` (in place, in the cache); its
    /// current image is flushed at commit.
    pub fn mark_touched(&mut self, page_num: u32) {
        self.touched_pages.insert(page_num);
    }

    /// The pages this transaction wrote, ascending (a stable order also keeps
    /// flush deterministic).
    pub fn touched_pages(&self) -> impl Iterator<Item = u32> + '_ {
        self.touched_pages.iter().copied()
    }

    /// Record a write intent for commit-time conflict re-validation.
    pub fn record_write(&mut self, intent: WriteIntent) {
        self.write_set.push(intent);
    }

    pub fn write_set(&self) -> &[WriteIntent] {
        &self.write_set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_exposes_identity_and_tracks_touched_pages() {
        let snapshot = Snapshot::new(2, vec![1]);
        let mut tx = Transaction::new(
            2,
            snapshot.clone(),
            Some("tx".to_string()),
            IsolationLevel::Snapshot,
        );

        assert_eq!(tx.id(), 2);
        assert_eq!(tx.snapshot(), &snapshot);
        assert_eq!(tx.name(), Some("tx"));
        assert_eq!(tx.isolation_level(), IsolationLevel::Snapshot);
        assert!(tx.touched_pages().next().is_none());

        tx.mark_touched(5);
        tx.mark_touched(5);
        tx.mark_touched(3);
        assert_eq!(tx.touched_pages().collect::<Vec<_>>(), vec![3, 5]);

        tx.record_write(WriteIntent {
            table_root: 4,
            key: 1,
            visible_created_tx: 2,
        });
        assert_eq!(tx.write_set().len(), 1);
    }
}
