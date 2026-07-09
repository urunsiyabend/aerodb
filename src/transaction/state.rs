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

/// Pager-level transaction bookkeeping. Keeping this state behind a single type
/// gives MVCC work one place for transaction ids, snapshots, and page version
/// metadata without spreading those fields through the pager.
#[derive(Debug, Default)]
pub struct TransactionState {
    active: bool,
    id: Option<TransactionId>,
    snapshot: Option<Snapshot>,
    name: Option<String>,
}

impl TransactionState {
    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn begin(&mut self, id: TransactionId, snapshot: Snapshot, name: Option<String>) {
        self.active = true;
        self.id = Some(id);
        self.snapshot = Some(snapshot);
        self.name = name;
    }

    pub fn finish(&mut self) {
        self.active = false;
        self.id = None;
        self.snapshot = None;
        self.name = None;
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn id(&self) -> Option<TransactionId> {
        self.id
    }

    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_state_tracks_lifecycle() {
        let mut state = TransactionState::default();
        assert!(!state.is_active());

        let snapshot = Snapshot::new(2, vec![1]);
        state.begin(2, snapshot.clone(), Some("tx".to_string()));
        assert!(state.is_active());
        assert_eq!(state.id(), Some(2));
        assert_eq!(state.snapshot(), Some(&snapshot));
        assert_eq!(state.name(), Some("tx"));

        state.finish();
        assert!(!state.is_active());
        assert_eq!(state.id(), None);
        assert_eq!(state.snapshot(), None);
        assert_eq!(state.name(), None);
    }
}
