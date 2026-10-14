use super::Constraint;
use super::keys::{Pending, check_primary_key};
use crate::catalog::{Catalog, TableInfo};
use crate::error::DbResult;
use crate::storage::row::RowData;
use crate::transaction::Snapshot;

/// PRIMARY KEY on `columns` (declared names): NOT NULL and logically unique
/// among the visible rows. See [`super::keys::check_primary_key`].
pub struct PrimaryKeyConstraint<'a> {
    pub columns: &'a [String],
}

impl<'a> Constraint for PrimaryKeyConstraint<'a> {
    fn validate_insert(
        &self,
        catalog: &mut Catalog,
        table: &TableInfo,
        row: &mut RowData,
        snapshot: &Snapshot,
    ) -> DbResult<()> {
        let mut view = table.clone();
        view.primary_key = Some(self.columns.to_vec());
        check_primary_key(catalog, &view, snapshot, std::slice::from_ref(row), &Pending::new())
    }
}
