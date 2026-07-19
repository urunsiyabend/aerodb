use crate::transaction::{TransactionId, TransactionStatus, TransactionTable};

/// Summary of one explicit vacuum pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VacuumReport {
    /// Physical row versions removed from table B-Tree leaves.
    pub versions_removed: usize,
    /// Index B-Trees rebuilt to drop stale logical row-key candidates.
    pub indexes_cleaned: usize,
}

/// Returns true when an MVCC row version whose `deleted_tx` is present can be
/// physically removed without becoming visible to any active snapshot.
pub fn deleted_version_is_removable(
    deleted_tx: Option<TransactionId>,
    global_xmin: TransactionId,
    tx_table: &TransactionTable,
) -> bool {
    let Some(deleted_tx) = deleted_tx else {
        return false;
    };

    deleted_tx < global_xmin
        && matches!(
            tx_table.get(&deleted_tx),
            Some(TransactionStatus::Committed(_))
        )
}

/// Returns true when a row version was created by a transaction that aborted.
///
/// With logical abort, an aborted transaction's versions are left in place and
/// hidden by MVCC visibility (they are never visible to any snapshot, present or
/// future). They can therefore be reclaimed immediately, without waiting for
/// `global_xmin`.
pub fn aborted_creator_is_removable(
    created_tx: TransactionId,
    tx_table: &TransactionTable,
) -> bool {
    matches!(tx_table.get(&created_tx), Some(TransactionStatus::Aborted))
}
