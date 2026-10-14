use super::Constraint;
use super::keys::{
    Pending, Removal, check_foreign_keys, check_removed_keys, check_restrict, visible_rows,
};
use crate::catalog::{Catalog, TableInfo};
use crate::error::DbResult;
use crate::sql::ast::ForeignKey;
use crate::storage::row::RowData;
use crate::transaction::Snapshot;

/// FOREIGN KEY constraints `fks` of a child table (MATCH SIMPLE, logical
/// comparison on all columns). See [`super::keys`].
pub struct ForeignKeyConstraint<'a> {
    pub fks: &'a [ForeignKey],
}

impl<'a> Constraint for ForeignKeyConstraint<'a> {
    /// The row image `row` of `table` must reference existing parent keys.
    fn validate_insert(
        &self,
        catalog: &mut Catalog,
        table: &TableInfo,
        row: &mut RowData,
        snapshot: &Snapshot,
    ) -> DbResult<()> {
        let mut view = table.clone();
        view.fks = self.fks.to_vec();
        let mut pending = Pending::new();
        pending.add(&table.name, row.clone());
        check_foreign_keys(catalog, &view, row, snapshot, &pending)
    }

    /// Deleting the visible row(s) of `table` equal to `row` must not hit an
    /// ON DELETE RESTRICT reference or orphan a NO ACTION one. Pure check: ON DELETE CASCADE
    /// references are allowed here and carried out by the DELETE executor.
    fn validate_delete(
        &self,
        catalog: &mut Catalog,
        table: &TableInfo,
        row: &RowData,
        snapshot: &Snapshot,
    ) -> DbResult<()> {
        let mut pending = Pending::new();
        for r in visible_rows(catalog, table, snapshot, &Pending::new())? {
            if r.data == *row {
                pending.remove(&table.name, &r);
            }
        }
        check_restrict(catalog, table, &[(row.clone(), None)], snapshot, &pending)?;
        check_removed_keys(
            catalog,
            table,
            std::slice::from_ref(row),
            snapshot,
            &pending,
            Removal::Delete,
        )
    }
}
