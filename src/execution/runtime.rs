use std::io;

use crate::catalog::Catalog;
use crate::sql::ast::{Statement, Expr};
use crate::storage::btree::BTree;
use crate::storage::row::{Row, RowData, ColumnValue, ColumnType, build_row_data};
use std::collections::HashMap;

pub fn execute_delete(catalog: &mut Catalog, table_name: &str, selection: Option<Expr>) -> io::Result<usize> {
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
            let count = rows_to_delete.len();
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
                catalog.update_catalog_root(table_name, new_root)?;
            }

            for r in rows_to_delete {
                catalog.remove_from_indexes(table_name, &r.data, r.key)?;
            }
            return Ok(count);
        }
    }
    Ok(0)
}

pub fn execute_update(
    catalog: &mut Catalog,
    table_name: &str,
    assignments: Vec<(String, String)>,
    selection: Option<Expr>,
) -> io::Result<usize> {
    if let Ok(table_info) = catalog.get_table(table_name) {
        let root_page = table_info.root_page;
        let columns = table_info.columns.clone();
        let mut col_pos = HashMap::new();
        for (i, (c, _)) in columns.iter().enumerate() {
            col_pos.insert(c.clone(), i);
        }
        let mut parsed = Vec::new();
        for (col, val) in assignments {
            let idx = *col_pos
                .get(&col)
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Column not found"))?;
            let ty = columns[idx].1;
            let cv = match ty {
                ColumnType::Integer => ColumnValue::Integer(
                    val.parse::<i32>()
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid INTEGER"))?,
                ),
                ColumnType::Text => ColumnValue::Text(val.clone()),
                ColumnType::Boolean => match val.to_ascii_lowercase().as_str() {
                    "true" => ColumnValue::Boolean(true),
                    "false" => ColumnValue::Boolean(false),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Invalid BOOLEAN",
                        ))
                    }
                },
            };
            parsed.push((idx, cv));
        }

        let rows_to_update = {
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

        if !rows_to_update.is_empty() {
            let count = rows_to_update.len();
            struct UpdateOp {
                old_key: i32,
                new_key: i32,
                old_data: RowData,
                new_data: RowData,
            }
            let mut ops = Vec::new();
            for row in rows_to_update {
                let mut new_data = row.data.clone();
                for (idx, val) in &parsed {
                    new_data.0[*idx] = val.clone();
                }
                let new_key = match new_data.0[0] {
                    ColumnValue::Integer(i) => i,
                    _ => row.key,
                };
                ops.push(UpdateOp {
                    old_key: row.key,
                    new_key,
                    old_data: row.data,
                    new_data,
                });
            }

            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
            for op in &ops {
                table_btree.delete(op.old_key)?;
                table_btree.insert(op.new_key, op.new_data.clone())?;
            }
            let new_root = table_btree.root_page();
            drop(table_btree);
            if new_root != root_page {
                if let Ok(t) = catalog.get_table_mut(table_name) {
                    t.root_page = new_root;
                }
                catalog.update_catalog_root(table_name, new_root)?;
            }

            for op in ops {
                catalog.remove_from_indexes(table_name, &op.old_data, op.old_key)?;
                catalog.insert_into_indexes(table_name, &op.new_data)?;
            }
            return Ok(count);
        }
    }
    Ok(0)
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
                Ok(()) => println!("Table {} created", table_name),
                Err(e) => {
                    if if_not_exists && e.to_string().contains("already exists") {
                        println!("Table {} already exists", table_name);
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        Statement::CreateIndex { index_name, table_name, column_name } => {
            catalog.create_index(&index_name, &table_name, &column_name)?;
            println!("Index {} created", index_name);
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
            println!("1 row inserted");
        }
        Statement::Select { table_name, selection, limit: _, offset: _, order_by: _ } => {
            let table_info = catalog.get_table(&table_name)?;
            println!("{}", format_header(&table_info.columns));
            let mut results = Vec::new();
            execute_select_with_indexes(catalog, &table_name, selection, &mut results)?;
            for row in results {
                println!("{}", format_row(&row));
            }
        }
        Statement::DropTable { table_name, .. } => {
            if catalog.drop_table(&table_name)? {
                println!("Table {} dropped", table_name);
            }
        }
        Statement::Delete { table_name, selection } => {
            let count = execute_delete(catalog, &table_name, selection)?;
            println!("{} row(s) deleted", count);
        }
        Statement::Update { table_name, assignments, selection } => {
            let count = execute_update(catalog, &table_name, assignments, selection)?;
            println!("{} row(s) updated", count);
        }
        Statement::BeginTransaction { name } => {
            catalog.begin_transaction(name)?;
        }
        Statement::Commit => {
            catalog.commit_transaction()?;
        }
        Statement::Rollback => {
            catalog.rollback_transaction()?;
        }
        Statement::Exit => {}
    }
    Ok(())
}

pub fn format_row(row: &Row) -> String {
    row.data.0
        .iter()
        .map(|v| match v {
            ColumnValue::Integer(i) => i.to_string(),
            ColumnValue::Text(s) => s.clone(),
            ColumnValue::Boolean(b) => b.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

pub fn format_header(columns: &[(String, ColumnType)]) -> String {
    columns
        .iter()
        .map(|(name, ty)| format!("{} {}", name, ty.as_str()))
        .collect::<Vec<_>>()
        .join(" | ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_row_simple() {
        let row = Row {
            key: 1,
            data: RowData(vec![ColumnValue::Integer(1), ColumnValue::Text("bob".into())]),
        };
        assert_eq!(format_row(&row), "1 | bob");
    }

    #[test]
    fn format_header_simple() {
        let cols = vec![
            ("id".into(), ColumnType::Integer),
            ("name".into(), ColumnType::Text),
        ];
        assert_eq!(format_header(&cols), "id INTEGER | name TEXT");
    }
}
