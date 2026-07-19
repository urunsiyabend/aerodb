/// Tracks whether the engine is currently outside a transaction, running an
/// auto-commit transaction, or inside a user-managed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    None,
    Implicit,
    Explicit,
}

impl TransactionMode {
    pub fn is_implicit(self) -> bool {
        matches!(self, TransactionMode::Implicit)
    }
}

impl Default for TransactionMode {
    fn default() -> Self {
        Self::None
    }
}

/// Process-local transaction identifier. Persistence for this counter will be
/// added later through WAL/metastore integration.
pub type TransactionId = u64;

/// SQL transaction isolation levels. The parser does not expose `SET TRANSACTION
/// ISOLATION LEVEL` yet, but keeping the isolation decision explicit here lets
/// that syntax select a policy without reshaping transaction state later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// One snapshot is captured at BEGIN and reused for the whole transaction.
    Snapshot,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::Snapshot
    }
}

/// MVCC snapshot captured when a transaction starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub active_tx_ids: Vec<TransactionId>,
    pub current_tx_id: Option<TransactionId>,
}

impl Snapshot {
    pub fn new(xmax: TransactionId, active_tx_ids: Vec<TransactionId>) -> Self {
        Self::new_with_current(xmax, active_tx_ids, None)
    }

    pub fn new_for_transaction(
        current_tx_id: TransactionId,
        xmax: TransactionId,
        active_tx_ids: Vec<TransactionId>,
    ) -> Self {
        Self::new_with_current(xmax, active_tx_ids, Some(current_tx_id))
    }

    fn new_with_current(
        xmax: TransactionId,
        mut active_tx_ids: Vec<TransactionId>,
        current_tx_id: Option<TransactionId>,
    ) -> Self {
        active_tx_ids.sort_unstable();
        active_tx_ids.dedup();
        let xmin = active_tx_ids.first().copied().unwrap_or(xmax);

        Self {
            xmin,
            xmax,
            active_tx_ids,
            current_tx_id,
        }
    }
}

// Per-session transaction state now lives in `super::session::Transaction`,
// which bundles the id/snapshot/isolation with the transaction's private write
// set. The pure snapshot/isolation types above remain shared building blocks.
