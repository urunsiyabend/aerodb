use std::io;

use crate::catalog::Catalog;
use crate::sql::ast::{Statement, Expr};
use crate::storage::btree::BTree;
use crate::storage::row::{Row, RowData, ColumnValue, build_row_data};
use std::collections::HashMap;

pub fn execute_delete(catalog: &mut Catalog, table_name: &str, selection: Option<Expr>) -> io::Result<()> {
    if let Ok(table_info) = catalog.get_table(table_name) {
        let root_page = table_info.root_page;
        let columns = table_info.columns.clone();
        let rows_to_delete = {
            let mut scan_tree = BTree::open_root(&mut catalog.pager, root_page)?;
            let mut cursor = scan_tree.scan_all_rows();
            let mut collected = Vec::new();
            while let Some(row) = cursor.next() {
                if let Some(ref expr) = selection {
                    let mut values = HashMap::new();
                    for ((col, _), val) in columns.iter().zip(row.data.0.iter()) {
                        let v = match val {
                            ColumnValue::Integer(i) => i.to_string(),
                            ColumnValue::Text(s) => s.clone(),
                            ColumnValue::Boolean(b) => b.to_string(),
                        };
                        values.insert(col.clone(), v);
                    }
                    if crate::sql::ast::evaluate_expression(expr, &values) {
                        collected.push(row);
                    }
                } else {
                    collected.push(row);
                }
            }
            drop(cursor);
            collected
        };

        if !rows_to_delete.is_empty() {
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
            for r in &rows_to_delete {
                table_btree.delete(r.key)?;
            }
            let new_root = table_btree.root_page();
            drop(table_btree);
            if new_root != root_page {
                if let Ok(t) = catalog.get_table_mut(table_name) {
                    t.root_page = new_root;
                }
            }

            for r in rows_to_delete {
                catalog.remove_from_indexes(table_name, &r.data, r.key)?;
            }
        }
    }
    Ok(())
}

pub fn execute_select_with_indexes(
    catalog: &mut Catalog,
    table_name: &str,
    selection: Option<Expr>,
    out: &mut Vec<Row>,
) -> io::Result<bool> {
    let table_info = catalog.get_table(table_name)?;
    let root_page = table_info.root_page;
    let columns = table_info.columns.clone();

    if let Some(Expr::Equals { left, right }) = selection.clone() {
        let (col_name, value) = if columns.iter().any(|(c, _)| c == &left) {
            (left, right)
        } else if columns.iter().any(|(c, _)| c == &right) {
            (right, left)
        } else {
            ("".into(), String::new())
        };
        if !col_name.is_empty() {
            if let Some(index) = catalog.find_index(table_name, &col_name).cloned() {
                let mut index_tree = BTree::open_root(&mut catalog.pager, index.root_page)?;
                let val_cv = ColumnValue::Text(value.clone());
                let hash = Catalog::hash_value(&val_cv);
                if let Some(row) = index_tree.find(hash)? {
                    if let ColumnValue::Text(ref stored) = row.data.0[0] {
                        if stored == &value {
                            for val in row.data.0.iter().skip(1) {
                                if let ColumnValue::Integer(k) = val {
                                    let mut table_tree = BTree::open_root(&mut catalog.pager, root_page)?;
                                    if let Some(r) = table_tree.find(*k)? {
                                        out.push(r);
                                    }
                                }
                            }
                            return Ok(true);
                        }
                    }
                }
                return Ok(true);
            }
        }
    }

    let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
    let mut cursor = table_btree.scan_all_rows();
    while let Some(row) = cursor.next() {
        if let Some(ref expr) = selection {
            let mut values = HashMap::new();
            for ((col, _), val) in columns.iter().zip(row.data.0.iter()) {
                let v = match val {
                    ColumnValue::Integer(i) => i.to_string(),
                    ColumnValue::Text(s) => s.clone(),
                    ColumnValue::Boolean(b) => b.to_string(),
                };
                values.insert(col.clone(), v);
            }
            if crate::sql::ast::evaluate_expression(expr, &values) {
                out.push(row);
            }
        } else {
            out.push(row);
        }
    }
    Ok(false)
}

pub fn handle_statement(catalog: &mut Catalog, stmt: Statement) -> io::Result<()> {
    match stmt {
        Statement::CreateTable { table_name, columns, if_not_exists } => {
            match catalog.create_table(&table_name, columns) {
                Ok(()) => {}
                Err(e) => {
                    if !(if_not_exists && e.to_string().contains("already exists")) {
                        return Err(e);
                    }
                }
            }
        }
        Statement::CreateIndex { index_name, table_name, column_name } => {
            catalog.create_index(&index_name, &table_name, &column_name)?;
        }
        Statement::Insert { table_name, values } => {
            let table_info = catalog.get_table(&table_name)?;
            let root_page = table_info.root_page;
            let columns = table_info.columns.clone();
            let row_data = build_row_data(&values, &columns)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let key = match row_data.0.get(0) {
                Some(ColumnValue::Integer(i)) => *i,
                _ => {
                    return Err(io::Error::new(io::ErrorKind::Other, "First column must be an INTEGER key"));
                }
            };
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
            table_btree.insert(key, row_data.clone())?;
            let new_root = table_btree.root_page();
            drop(table_btree);
            if new_root != root_page {
                catalog.get_table_mut(&table_name)?.root_page = new_root;
            }
            catalog.insert_into_indexes(&table_name, &row_data)?;
        }
        Statement::Select { table_name, selection, limit: _, offset: _, order_by: _ } => {
            let mut results = Vec::new();
            execute_select_with_indexes(catalog, &table_name, selection, &mut results)?;
        }
        Statement::DropTable { table_name, .. } => {
            catalog.drop_table(&table_name)?;
        }
        Statement::Delete { table_name, selection } => {
            execute_delete(catalog, &table_name, selection)?;
        }
        Statement::Exit => {}
    }
    Ok(())
}
