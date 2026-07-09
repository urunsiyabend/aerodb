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

/// Pager-level transaction bookkeeping. Keeping this state behind a single type
/// gives future MVCC work one place to add transaction ids, snapshots, and page
/// version metadata without spreading those fields through the pager.
#[derive(Debug, Default)]
pub struct TransactionState {
    active: bool,
    name: Option<String>,
}

impl TransactionState {
    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn begin(&mut self, name: Option<String>) {
        self.active = true;
        self.name = name;
    }

    pub fn finish(&mut self) {
        self.active = false;
        self.name = None;
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_state_tracks_lifecycle() {
        let mut state = TransactionState::default();
        assert!(!state.is_active());

        state.begin(Some("tx".to_string()));
        assert!(state.is_active());
        assert_eq!(state.name(), Some("tx"));

        state.finish();
        assert!(!state.is_active());
        assert_eq!(state.name(), None);
    }
}
