use crate::storage::row::{Row, COMMITTED_BOOTSTRAP_TX};

use super::{Snapshot, TransactionId, TransactionStatus, TransactionTable};

pub fn is_visible(row_version: &Row, snapshot: &Snapshot, tx_table: &TransactionTable) -> bool {
    let current_tx = snapshot.current_tx_id;

    if tx_table.get(&row_version.created_tx) == Some(&TransactionStatus::Aborted) {
        return false;
    }

    if current_tx == Some(row_version.created_tx) {
        return row_version.deleted_tx != current_tx;
    }

    if !created_tx_visible(row_version.created_tx, snapshot, tx_table) {
        return false;
    }

    match row_version.deleted_tx {
        None => true,
        Some(deleted_tx) if Some(deleted_tx) == current_tx => false,
        Some(deleted_tx) => !committed_before_snapshot(deleted_tx, snapshot, tx_table),
    }
}

fn created_tx_visible(
    created_tx: TransactionId,
    snapshot: &Snapshot,
    tx_table: &TransactionTable,
) -> bool {
    committed_before_snapshot(created_tx, snapshot, tx_table)
}

fn committed_before_snapshot(
    tx_id: TransactionId,
    snapshot: &Snapshot,
    tx_table: &TransactionTable,
) -> bool {
    // A transaction counts as committed-before-snapshot when it sits below the
    // snapshot boundary, is not one of the transactions that were live when the
    // snapshot was taken, and is not explicitly recorded as aborted or still
    // active. A tx id that is *absent* from the table is treated as committed
    // (frozen): the WAL is truncated on commit, so long-committed transactions
    // leave no status record behind, yet their versions must stay visible.
    tx_id == COMMITTED_BOOTSTRAP_TX
        || (tx_id < snapshot.xmax
            && snapshot.active_tx_ids.binary_search(&tx_id).is_err()
            && !matches!(
                tx_table.get(&tx_id),
                Some(TransactionStatus::Aborted) | Some(TransactionStatus::Active)
            ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::row::RowData;

    fn row(created_tx: TransactionId, deleted_tx: Option<TransactionId>) -> Row {
        let mut row = Row::new(1, RowData(Vec::new()));
        row.created_tx = created_tx;
        row.deleted_tx = deleted_tx;
        row
    }

    fn snapshot() -> Snapshot {
        Snapshot::new_for_transaction(10, 20, vec![15])
    }

    #[test]
    fn own_write_visible_unless_self_deleted() {
        let tx_table = TransactionTable::new();
        assert!(is_visible(&row(10, None), &snapshot(), &tx_table));
        assert!(!is_visible(&row(10, Some(10)), &snapshot(), &tx_table));
    }

    #[test]
    fn aborted_creator_is_invisible_even_to_same_snapshot_tx() {
        let mut tx_table = TransactionTable::new();
        tx_table.insert(10, TransactionStatus::Aborted);
        assert!(!is_visible(&row(10, None), &snapshot(), &tx_table));
    }

    #[test]
    fn committed_creator_before_snapshot_is_visible() {
        let mut tx_table = TransactionTable::new();
        tx_table.insert(8, TransactionStatus::Committed(1));
        assert!(is_visible(&row(8, None), &snapshot(), &tx_table));
    }

    #[test]
    fn active_at_snapshot_creator_is_invisible() {
        let mut tx_table = TransactionTable::new();
        tx_table.insert(15, TransactionStatus::Committed(1));
        assert!(!is_visible(&row(15, None), &snapshot(), &tx_table));
    }

    #[test]
    fn uncommitted_delete_keeps_old_version_visible() {
        let mut tx_table = TransactionTable::new();
        tx_table.insert(8, TransactionStatus::Committed(1));
        tx_table.insert(12, TransactionStatus::Active);
        assert!(is_visible(&row(8, Some(12)), &snapshot(), &tx_table));
    }

    #[test]
    fn committed_delete_before_snapshot_hides_old_version() {
        let mut tx_table = TransactionTable::new();
        tx_table.insert(8, TransactionStatus::Committed(1));
        tx_table.insert(12, TransactionStatus::Committed(1));
        assert!(!is_visible(&row(8, Some(12)), &snapshot(), &tx_table));
    }
}
