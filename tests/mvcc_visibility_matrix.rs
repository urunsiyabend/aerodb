//! Exhaustive MVCC visibility matrix over [`aerodb::transaction::is_visible`].
//!
//! These are unit-level tests: they construct `Row` versions, a
//! `TransactionTable`, and a `Snapshot` directly, then assert visibility for
//! every meaningful combination of creator and deleter transaction status.
//!
//! Testing-model note: the engine runs a single active transaction per process,
//! so true multi-transaction concurrency cannot be exercised end-to-end. The
//! full cross-product of transaction states is therefore validated here against
//! the visibility predicate, which is the single source of truth every read
//! path (`find_visible` / `scan_visible`) funnels through.

use aerodb::storage::row::{Row, RowData, COMMITTED_BOOTSTRAP_TX};
use aerodb::transaction::{is_visible, Snapshot, TransactionId, TransactionStatus, TransactionTable};

/// Snapshot taken by transaction 100: everything below 100 is "before" the
/// snapshot, transaction 50 was still live when the snapshot was captured.
fn snapshot() -> Snapshot {
    Snapshot::new_for_transaction(100, 100, vec![50])
}

fn row(created_tx: TransactionId, deleted_tx: Option<TransactionId>) -> Row {
    let mut row = Row::new(1, RowData(Vec::new()));
    row.created_tx = created_tx;
    row.deleted_tx = deleted_tx;
    row
}

fn table(entries: &[(TransactionId, TransactionStatus)]) -> TransactionTable {
    let mut table = TransactionTable::new();
    for (id, status) in entries {
        table.insert(*id, *status);
    }
    table
}

// ---------------------------------------------------------------------------
// Creator axis (row not deleted)
// ---------------------------------------------------------------------------

#[test]
fn creator_bootstrap_is_visible() {
    let tx_table = table(&[]);
    assert!(is_visible(
        &row(COMMITTED_BOOTSTRAP_TX, None),
        &snapshot(),
        &tx_table
    ));
}

#[test]
fn creator_committed_before_snapshot_is_visible() {
    let tx_table = table(&[(30, TransactionStatus::Committed(1))]);
    assert!(is_visible(&row(30, None), &snapshot(), &tx_table));
}

#[test]
fn creator_absent_below_bound_is_frozen_visible() {
    // No status record (WAL truncated on commit) but below the snapshot bound
    // and not live at snapshot time -> treated as committed/frozen.
    let tx_table = table(&[]);
    assert!(is_visible(&row(30, None), &snapshot(), &tx_table));
}

#[test]
fn creator_live_at_snapshot_is_invisible() {
    // 50 was in active_tx_ids when the snapshot was captured -> concurrent.
    let tx_table = table(&[(50, TransactionStatus::Committed(1))]);
    assert!(!is_visible(&row(50, None), &snapshot(), &tx_table));
}

#[test]
fn creator_aborted_is_invisible() {
    let tx_table = table(&[(30, TransactionStatus::Aborted)]);
    assert!(!is_visible(&row(30, None), &snapshot(), &tx_table));
}

#[test]
fn creator_active_is_invisible() {
    let tx_table = table(&[(30, TransactionStatus::Active)]);
    assert!(!is_visible(&row(30, None), &snapshot(), &tx_table));
}

#[test]
fn creator_self_is_visible() {
    let tx_table = table(&[(100, TransactionStatus::Active)]);
    assert!(is_visible(&row(100, None), &snapshot(), &tx_table));
}

#[test]
fn creator_after_snapshot_is_invisible() {
    let tx_table = table(&[(150, TransactionStatus::Committed(9))]);
    assert!(!is_visible(&row(150, None), &snapshot(), &tx_table));
}

#[test]
fn creator_self_but_aborted_is_invisible() {
    // Defense in depth: a self-created version whose transaction is recorded as
    // aborted must never be visible, even to its own snapshot.
    let tx_table = table(&[(100, TransactionStatus::Aborted)]);
    assert!(!is_visible(&row(100, None), &snapshot(), &tx_table));
}

// ---------------------------------------------------------------------------
// Deleter axis (creator = 30, committed & visible)
// ---------------------------------------------------------------------------

fn visible_creator_table(extra: &[(TransactionId, TransactionStatus)]) -> TransactionTable {
    let mut entries = vec![(30, TransactionStatus::Committed(1))];
    entries.extend_from_slice(extra);
    table(&entries)
}

#[test]
fn deleter_none_keeps_row_visible() {
    let tx_table = visible_creator_table(&[]);
    assert!(is_visible(&row(30, None), &snapshot(), &tx_table));
}

#[test]
fn deleter_committed_before_snapshot_hides_row() {
    let tx_table = visible_creator_table(&[(40, TransactionStatus::Committed(2))]);
    assert!(!is_visible(&row(30, Some(40)), &snapshot(), &tx_table));
}

#[test]
fn deleter_absent_below_bound_hides_row() {
    // Frozen deleter (no status record) still counts as a committed deletion.
    let tx_table = visible_creator_table(&[]);
    assert!(!is_visible(&row(30, Some(40)), &snapshot(), &tx_table));
}

#[test]
fn deleter_live_at_snapshot_keeps_row_visible() {
    let tx_table = visible_creator_table(&[(50, TransactionStatus::Committed(2))]);
    assert!(is_visible(&row(30, Some(50)), &snapshot(), &tx_table));
}

#[test]
fn deleter_active_keeps_row_visible() {
    let tx_table = visible_creator_table(&[(40, TransactionStatus::Active)]);
    assert!(is_visible(&row(30, Some(40)), &snapshot(), &tx_table));
}

#[test]
fn deleter_aborted_keeps_row_visible() {
    let tx_table = visible_creator_table(&[(40, TransactionStatus::Aborted)]);
    assert!(is_visible(&row(30, Some(40)), &snapshot(), &tx_table));
}

#[test]
fn deleter_after_snapshot_keeps_row_visible() {
    let tx_table = visible_creator_table(&[(150, TransactionStatus::Committed(9))]);
    assert!(is_visible(&row(30, Some(150)), &snapshot(), &tx_table));
}

#[test]
fn deleter_self_hides_row() {
    let tx_table = visible_creator_table(&[(100, TransactionStatus::Active)]);
    assert!(!is_visible(&row(30, Some(100)), &snapshot(), &tx_table));
}

#[test]
fn self_created_then_self_deleted_is_invisible() {
    let tx_table = table(&[(100, TransactionStatus::Active)]);
    assert!(!is_visible(&row(100, Some(100)), &snapshot(), &tx_table));
}

// ---------------------------------------------------------------------------
// Reader whose snapshot predates the row's creation
// ---------------------------------------------------------------------------

#[test]
fn reader_before_creation_does_not_see_row() {
    // Read-only snapshot with xmax = 10: a row created by tx 30 is in the future.
    let early = Snapshot::new(10, Vec::new());
    let tx_table = table(&[(30, TransactionStatus::Committed(1))]);
    assert!(!is_visible(&row(30, None), &early, &tx_table));
}

#[test]
fn reader_after_creation_sees_row() {
    let later = Snapshot::new(50, Vec::new());
    let tx_table = table(&[(30, TransactionStatus::Committed(1))]);
    assert!(is_visible(&row(30, None), &later, &tx_table));
}
