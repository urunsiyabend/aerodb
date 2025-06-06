use crate::storage::row::Row;

pub trait Executor {
    /// Returns the next row of results, or None if done.
    fn next(&mut self) -> Option<Row>;
}
