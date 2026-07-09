use std::collections::HashMap;

use super::TransactionId;

/// Monotonic timestamp assigned when a transaction commits.
pub type CommitTimestamp = u64;

/// Durable transaction outcome used by MVCC visibility and WAL recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Active,
    Committed(CommitTimestamp),
    Aborted,
}

pub type TransactionTable = HashMap<TransactionId, TransactionStatus>;
