use std::io;
use crate::catalog::{Catalog, TableInfo};
use crate::sql::ast::ForeignKey;
use crate::storage::row::{RowData, ColumnValue};
use crate::storage::btree::BTree;
use super::Constraint;

pub struct ForeignKeyConstraint<'a> {
    pub fks: &'a [ForeignKey],
}

impl<'a> Constraint for ForeignKeyConstraint<'a> {
    fn validate_insert(&self, catalog: &mut Catalog, table: &TableInfo, row: &mut RowData) -> io::Result<()> {
        for fk in self.fks {
            if fk.columns.is_empty() || fk.parent_columns.is_empty() {
                continue;
            }
            let col_idx = table
                .columns
                .iter()
                .position(|(c, _)| c == &fk.columns[0])
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "FK column not found"))?;
            let child_val = match row.0[col_idx] {
                ColumnValue::Integer(i) => i,
                _ => return Err(io::Error::new(io::ErrorKind::Other, "FK column must be INTEGER")),
            };
            let parent_root = catalog.get_table(&fk.parent_table)?.root_page;
            let mut parent_btree = BTree::open_root(&mut catalog.pager, parent_root)?;
            if parent_btree.find(child_val)?.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("FK violation: no {}.{} = {}", fk.parent_table, fk.parent_columns[0], child_val),
                ));
            }
        }
        Ok(())
    }

    fn validate_delete(&self, catalog: &mut Catalog, table: &TableInfo, row: &RowData) -> io::Result<()> {
        let columns = &table.columns;
        let child_tables: Vec<_> = catalog.all_tables();
        for child in &child_tables {
            for fk in &child.fks {
                if fk.parent_table == table.name {
                    let parent_idx = columns.iter().position(|(c, _)| c == &fk.parent_columns[0]).unwrap();
                    let parent_val = match row.0[parent_idx] {
                        ColumnValue::Integer(i) => i,
                        _ => continue,
                    };
                    let child_idx = child.columns.iter().position(|(c, _)| c == &fk.columns[0]).unwrap();
                    let mut matches: Vec<(i32, RowData)> = Vec::new();
                    {
                        let mut scan_tree = BTree::open_root(&mut catalog.pager, child.root_page)?;
                        let mut cursor = scan_tree.scan_all_rows();
                        while let Some(crow) = cursor.next() {
                            if let ColumnValue::Integer(v) = crow.data.0[child_idx] {
                                if v == parent_val {
                                    matches.push((crow.key, crow.data.clone()));
                                }
                            }
                        }
                    }
                    if !matches.is_empty() {
                        if fk.on_delete == Some(crate::sql::ast::Action::Cascade) {
                            let mut del_tree = BTree::open_root(&mut catalog.pager, child.root_page)?;
                            for (k, _) in &matches {
                                del_tree.delete(*k)?;
                            }
                            let new_root_c = del_tree.root_page();
                            drop(del_tree);
                            for (k, data) in &matches {
                                catalog.remove_from_indexes(&child.name, data, *k)?;
                            }
                            if new_root_c != child.root_page {
                                catalog.get_table_mut(&child.name)?.root_page = new_root_c;
                                catalog.update_catalog_root(&child.name, new_root_c)?;
                            }
                        } else {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("Cannot delete {}: referenced by {}.{}", table.name, child.name, fk.columns[0]),
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
