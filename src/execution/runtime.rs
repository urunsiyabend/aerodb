use std::io;

use crate::catalog::Catalog;
use crate::sql::ast::{Statement, Expr, expr_to_string};
use crate::constraints::{Constraint, not_null::NotNullConstraint, foreign_key::ForeignKeyConstraint, default::DefaultConstraint};
use crate::storage::btree::BTree;
use crate::storage::row::{Row, RowData, ColumnValue, ColumnType, build_row_data};
use std::collections::HashMap;

pub fn execute_delete(catalog: &mut Catalog, table_name: &str, selection: Option<Expr>) -> io::Result<usize> {
    if let Ok(table_info) = catalog.get_table(table_name).map(Clone::clone) {
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
                        let v = val.to_string_value();
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
            let fk_cons = ForeignKeyConstraint { fks: &table_info.fks };
            for row in &rows_to_delete {
                fk_cons.validate_delete(catalog, &table_info, &row.data)?;
            }

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
                ColumnType::Char(len) => {
                    if val.len() > len {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "Value '{}' for column '{}' exceeds length {}",
                                val, col, len
                            ),
                        ));
                    }
                    let mut s = val.clone();
                    if s.len() < len {
                        s.push_str(&" ".repeat(len - s.len()));
                    }
                    ColumnValue::Char(s)
                }
                ColumnType::SmallInt { unsigned, .. } => {
                    let i = val.parse::<i32>().map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid SMALLINT"))?;
                    if unsigned {
                        if !(0..=65535).contains(&i) {
                            return Err(io::Error::new(io::ErrorKind::Other, "Value out of range"));
                        }
                    } else if !(-32768..=32767).contains(&i) {
                        return Err(io::Error::new(io::ErrorKind::Other, "Value out of range"));
                    }
                    ColumnValue::Integer(i)
                }
                ColumnType::MediumInt { unsigned, .. } => {
                    let i = val.parse::<i32>().map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid MEDIUMINT"))?;
                    if unsigned {
                        if !(0..=16_777_215).contains(&i) {
                            return Err(io::Error::new(io::ErrorKind::Other, "Value out of range"));
                        }
                    } else if !(-8_388_608..=8_388_607).contains(&i) {
                        return Err(io::Error::new(io::ErrorKind::Other, "Value out of range"));
                    }
                    ColumnValue::Integer(i)
                }
                ColumnType::Double { unsigned, .. } => {
                    let f = val.parse::<f64>().map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid DOUBLE"))?;
                    if unsigned && f < 0.0 {
                        return Err(io::Error::new(io::ErrorKind::Other, "Value out of range"));
                    }
                    ColumnValue::Double(f)
                }
                ColumnType::Date => {
                    match crate::storage::row::parse_date(&val) {
                        Some(d) => ColumnValue::Date(d),
                        None => {
                            return Err(io::Error::new(io::ErrorKind::Other, "Invalid DATE"));
                        }
                    }
                }
                ColumnType::DateTime => {
                    match crate::storage::row::parse_datetime(&val) {
                        Some(ts) => ColumnValue::DateTime(ts),
                        None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid DATETIME")),
                    }
                }
                ColumnType::Timestamp => {
                    match crate::storage::row::parse_datetime(&val) {
                        Some(ts) => ColumnValue::Timestamp(ts),
                        None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid TIMESTAMP")),
                    }
                }
                ColumnType::Time => {
                    match crate::storage::row::parse_time(&val) {
                        Some(t) => ColumnValue::Time(t),
                        None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid TIME")),
                    }
                }
                ColumnType::Year => {
                    match crate::storage::row::parse_year(&val) {
                        Some(y) => ColumnValue::Year(y),
                        None => return Err(io::Error::new(io::ErrorKind::Other, "Invalid YEAR")),
                    }
                }
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
                        let v = val.to_string_value();
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
    let table_info = catalog.get_table(table_name)?.clone();
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
                let v = val.to_string_value();
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

pub fn execute_multi_join(
    plan: &crate::execution::plan::MultiJoinPlan,
    catalog: &mut Catalog,
    out: &mut Vec<Vec<String>>,
) -> io::Result<()> {
    use crate::sql::ast::evaluate_expression;
    let mut result_rows: Vec<std::collections::HashMap<String, ColumnValue>> = Vec::new();

    // base table scan
    {
        let base_info = catalog.get_table(&plan.base_table)?.clone();
        let mut tree = BTree::open_root(&mut catalog.pager, base_info.root_page)?;
        let mut cursor = tree.scan_all_rows();
        while let Some(row) = cursor.next() {
            let mut map = std::collections::HashMap::new();
            for ((c, _), v) in base_info.columns.iter().zip(row.data.0.iter()) {
                map.insert(format!("{}.{c}", plan.base_table), v.clone());
            }
            result_rows.push(map);
        }
    }

    for jc in &plan.joins {
        let alias = jc.alias.as_ref().unwrap_or(&jc.table);
        let info = catalog.get_table(&jc.table)?.clone();
        let mut tree = BTree::open_root(&mut catalog.pager, info.root_page)?;
        let rows: Vec<_> = {
            let mut curs = tree.scan_all_rows();
            let mut tmp = Vec::new();
            while let Some(r) = curs.next() {
                let mut m = std::collections::HashMap::new();
                for ((c, _), v) in info.columns.iter().zip(r.data.0.iter()) {
                    m.insert(format!("{alias}.{c}"), v.clone());
                }
                tmp.push(m);
            }
            tmp
        };

        let mut new_rows = Vec::new();
        for left in &result_rows {
            let key = left.get(&format!("{}.{}", jc.left_table, jc.left_column));
            if let Some(key_val) = key {
                for r in &rows {
                    if let Some(rv) = r.get(&format!("{alias}.{}", jc.right_column)) {
                        if rv == key_val {
                            let mut merged = left.clone();
                            for (k, v) in r {
                                merged.insert(k.clone(), v.clone());
                            }
                            new_rows.push(merged);
                        }
                    }
                }
            }
        }
        result_rows = new_rows;
    }

    let projections = expand_join_projections(plan, catalog)?;
    for row in result_rows {
        let mut str_map = std::collections::HashMap::new();
        for (k, v) in &row {
            let s = v.to_string_value();
            str_map.insert(k.clone(), s);
        }
        if let Some(ref pred) = plan.where_predicate {
            if !evaluate_expression(pred, &str_map) {
                continue;
            }
        }
        let mut projected = Vec::new();
        for p in &projections {
            if let Some(v) = str_map.get(p) {
                projected.push(v.clone());
            }
        }
        out.push(projected);
    }
    Ok(())
}

pub fn execute_group_query(
    catalog: &mut Catalog,
    table_name: &str,
    projections: &[crate::sql::ast::SelectExpr],
    group_by: Option<&[String]>,
    having: Option<Expr>,
    selection: Option<Expr>,
    out: &mut Vec<Vec<String>>,
    context: Option<&std::collections::HashMap<String, String>>,
) -> io::Result<Vec<(String, ColumnType)>> {
    let mut rows = Vec::new();
    execute_select_with_indexes(catalog, table_name, None, &mut rows)?;
    let table_info = catalog.get_table(table_name)?.clone();

    let mut groups: std::collections::HashMap<Vec<String>, Vec<crate::storage::row::Row>> = std::collections::HashMap::new();
    let mut col_pos = std::collections::HashMap::new();
    for (i, (c, _)) in table_info.columns.iter().enumerate() {
        col_pos.insert(c.clone(), i);
    }
    for row in rows {
        let mut values = std::collections::HashMap::new();
        if let Some(ctx) = context {
            for (k, v) in ctx {
                values.insert(k.clone(), v.clone());
            }
        }
        for ((c, _), val) in table_info.columns.iter().zip(row.data.0.iter()) {
            let s = val.to_string_value();
            values.insert(c.clone(), s.clone());
            let qual = format!("{}.{}", table_name, c);
            values.insert(qual, s);
        }
        if let Some(ref sel) = selection {
            if !evaluate_with_catalog(sel, &values, catalog)? {
                continue;
            }
        }
        let key = if let Some(gb) = group_by {
            gb.iter()
                .map(|c| {
                    let idx = col_pos[c];
                    row.data.0[idx].to_string_value()
                })
                .collect()
        } else {
            Vec::new()
        };
        groups.entry(key).or_default().push(row);
    }

    let mut header = Vec::new();
    for expr in projections {
        match expr {
            crate::sql::ast::SelectExpr::Column(c) => {
                let idx = col_pos[c];
                header.push((c.clone(), table_info.columns[idx].1));
            }
            crate::sql::ast::SelectExpr::Aggregate { func, column } => {
                header.push((format!("{}({})", func.as_str(), column.clone().unwrap_or("*".into())), ColumnType::Integer));
            }
            crate::sql::ast::SelectExpr::All => {
                for (c, ty) in &table_info.columns {
                    header.push((c.clone(), *ty));
                }
            }
            crate::sql::ast::SelectExpr::Subquery(_) => {
                header.push(("SUBQUERY".into(), ColumnType::Text));
            }
            crate::sql::ast::SelectExpr::Literal(val) => {
                let ty = if val.parse::<i32>().is_ok() { ColumnType::Integer } else { ColumnType::Text };
                header.push((val.clone(), ty));
            }
        }
    }

    for (_key, grows) in groups {
        let mut result_row = Vec::new();
        let mut value_map = std::collections::HashMap::new();
        for expr in projections {
            match expr {
                crate::sql::ast::SelectExpr::Column(c) => {
                    let idx = col_pos[c];
                    let val = &grows[0].data.0[idx];
                    let s = val.to_string_value();
                    value_map.insert(c.clone(), s.clone());
                    result_row.push(s);
                }
                crate::sql::ast::SelectExpr::Aggregate { func, column } => {
                    let val = match func {
                        crate::sql::ast::AggFunc::Count => grows.len().to_string(),
                        crate::sql::ast::AggFunc::Sum => {
                            let idx = col_pos[column.as_ref().unwrap()];
                            let mut sum = 0i64;
                            for r in &grows {
                                if let ColumnValue::Integer(i) = r.data.0[idx] {
                                    sum += i as i64;
                                }
                            }
                            sum.to_string()
                        }
                        crate::sql::ast::AggFunc::Min => {
                            let idx = col_pos[column.as_ref().unwrap()];
                            let mut min_val: Option<i32> = None;
                            for r in &grows {
                                if let ColumnValue::Integer(i) = r.data.0[idx] {
                                    min_val = Some(min_val.map_or(i, |m| m.min(i)));
                                }
                            }
                            min_val.unwrap_or(0).to_string()
                        }
                        crate::sql::ast::AggFunc::Max => {
                            let idx = col_pos[column.as_ref().unwrap()];
                            let mut max_val: Option<i32> = None;
                            for r in &grows {
                                if let ColumnValue::Integer(i) = r.data.0[idx] {
                                    max_val = Some(max_val.map_or(i, |m| m.max(i)));
                                }
                            }
                            max_val.unwrap_or(0).to_string()
                        }
                        crate::sql::ast::AggFunc::Avg => {
                            let idx = col_pos[column.as_ref().unwrap()];
                            let mut sum = 0i64;
                            for r in &grows {
                                if let ColumnValue::Integer(i) = r.data.0[idx] {
                                    sum += i as i64;
                                }
                            }
                            let avg = sum as f64 / grows.len() as f64;
                            avg.to_string()
                        }
                    };
                    let name = format!("{}({})", func.as_str(), column.clone().unwrap_or("*".into()));
                    value_map.insert(name, val.clone());
                    result_row.push(val);
                }
                crate::sql::ast::SelectExpr::All => {
                    for (i, _) in &table_info.columns {
                        let idx = col_pos[i];
                        let v = &grows[0].data.0[idx];
                        let s = v.to_string_value();
                        value_map.insert(i.clone(), s.clone());
                        result_row.push(s);
                    }
                }
                crate::sql::ast::SelectExpr::Subquery(sub) => {
                    let mut inner_rows = Vec::new();
                    let mut ctx = std::collections::HashMap::new();
                    for ((c, _), v) in table_info.columns.iter().zip(grows[0].data.0.iter()) {
                        let val = v.to_string_value();
                        ctx.insert(c.clone(), val);
                    }
                    execute_select_statement(catalog, sub, &mut inner_rows, Some(&ctx))?;
                    let val = inner_rows.get(0).and_then(|r| r.get(0)).cloned().unwrap_or_default();
                    // subqueries are not referenced by HAVING expressions
                    result_row.push(val);
                }
                crate::sql::ast::SelectExpr::Literal(val) => {
                    result_row.push(val.clone());
                }
            }
        }
        if let Some(ref pred) = having {
            if !crate::sql::ast::evaluate_expression(pred, &value_map) {
                continue;
            }
        }
        out.push(result_row);
    }
    Ok(header)
}

pub fn handle_statement(catalog: &mut Catalog, stmt: Statement) -> io::Result<()> {
    match stmt {
        Statement::CreateTable { table_name, columns, fks, if_not_exists } => {
            let auto_cols: Vec<_> = columns.iter().filter(|c| c.auto_increment).collect();
            if auto_cols.len() > 1 {
                return Err(io::Error::new(io::ErrorKind::Other, "Only one AUTO_INCREMENT column allowed per table"));
            }
            let cols: Vec<_> = columns
                .into_iter()
                .map(|c| (c.name.clone(), c.col_type, c.not_null, c.default_value, c.auto_increment))
                .collect();
            match catalog.create_table_with_fks(&table_name, cols.clone(), fks) {
                Ok(()) => println!("Table {} created", table_name),
                Err(e) => {
                    if if_not_exists && e.to_string().contains("already exists") {
                        println!("Table {} already exists", table_name);
                    } else {
                        return Err(e);
                    }
                }
            }
            for (name, _, _, _, ai) in cols {
                if ai {
                    let seq_name = format!("{}_{}", table_name, name);
                    catalog.create_sequence(&seq_name, 1, 1)?;
                }
            }
        }
        Statement::CreateIndex { index_name, table_name, column_name } => {
            catalog.create_index(&index_name, &table_name, &column_name)?;
            println!("Index {} created", index_name);
        }
        Statement::Insert { table_name, columns: col_list, values } => {
            let table_info = catalog.get_table(&table_name)?.clone();
            let root_page = table_info.root_page;
            let columns = table_info.columns.clone();
            let fks = table_info.fks.clone();

            let mut vals = Vec::new();
            if let Some(cols) = col_list {
                if values.len() != cols.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "INSERT column/value count mismatch: expected {} columns, got {} values",
                            cols.len(),
                            values.len()
                        ),
                    ));
                }
                for c in &cols {
                    if !columns.iter().any(|(n, _)| n == c) {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Column '{}' does not exist in table '{}'", c, table_name),
                        ));
                    }
                }
                for (idx, (col_name, _)) in columns.iter().enumerate() {
                    let auto = table_info.auto_increment.get(idx).copied().unwrap_or(false);
                    if let Some(pos) = cols.iter().position(|c| c == col_name) {
                        let expr = &values[pos];
                        if auto {
                            if matches!(expr, Expr::DefaultValue) {
                                let seq = format!("{}_{}", table_name, col_name);
                                let next = catalog.next_sequence_value(&seq)?;
                                vals.push(next.to_string());
                            } else {
                                let s = expr_to_string(expr);
                                if let Ok(v) = s.parse::<i64>() {
                                    catalog.update_sequence_current(&format!("{}_{}", table_name, col_name), v)?;
                                }
                                vals.push(s);
                            }
                        } else if matches!(expr, Expr::DefaultValue) {
                            if let Some(def) = table_info.default_values.get(idx).and_then(|o| o.as_ref()) {
                                vals.push(DefaultConstraint::evaluate(def)?);
                            } else if !table_info.not_null[idx] {
                                vals.push("NULL".into());
                            } else {
                                return Err(io::Error::new(io::ErrorKind::Other, format!("Cannot use DEFAULT for column '{}' - no default value defined", col_name)));
                            }
                        } else {
                            vals.push(expr_to_string(expr));
                        }
                    } else {
                        if auto {
                            let seq = format!("{}_{}", table_name, col_name);
                            let next = catalog.next_sequence_value(&seq)?;
                            vals.push(next.to_string());
                        } else if let Some(def) = table_info.default_values.get(idx).and_then(|o| o.as_ref()) {
                            vals.push(DefaultConstraint::evaluate(def)?);
                        } else if !table_info.not_null[idx] {
                            vals.push("NULL".into());
                        } else {
                            return Err(io::Error::new(io::ErrorKind::Other, format!("Column '{}' requires a value or DEFAULT", col_name)));
                        }
                    }
                }
            } else {
                if values.len() != columns.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "INSERT has wrong number of values: expected {}, got {}",
                            columns.len(),
                            values.len()
                        ),
                    ));
                }
                for (idx, expr) in values.iter().enumerate() {
                    let auto = table_info.auto_increment.get(idx).copied().unwrap_or(false);
                    if auto {
                        if matches!(expr, Expr::DefaultValue) {
                            let seq = format!("{}_{}", table_name, columns[idx].0);
                            let next = catalog.next_sequence_value(&seq)?;
                            vals.push(next.to_string());
                        } else {
                            let s = expr_to_string(expr);
                            if let Ok(v) = s.parse::<i64>() {
                                catalog.update_sequence_current(&format!("{}_{}", table_name, columns[idx].0), v)?;
                            }
                            vals.push(s);
                        }
                    } else if matches!(expr, Expr::DefaultValue) {
                        if let Some(def) = table_info.default_values.get(idx).and_then(|o| o.as_ref()) {
                            vals.push(DefaultConstraint::evaluate(def)?);
                        } else if !table_info.not_null[idx] {
                            vals.push("NULL".into());
                        } else {
                            return Err(io::Error::new(io::ErrorKind::Other, format!(
                                    "Cannot use DEFAULT for column '{}' - no default value defined",
                                    columns[idx].0)));
                        }
                    } else {
                        vals.push(expr_to_string(expr));
                    }
                }
            }

            let mut row_data = build_row_data(&vals, &columns)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let nn = NotNullConstraint;
            nn.validate_insert(catalog, &table_info, &mut row_data)?;

            let fk_cons = ForeignKeyConstraint { fks: &fks };
            fk_cons.validate_insert(catalog, &table_info, &mut row_data)?;
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
        Statement::Select { columns, from, joins, where_predicate, group_by, having } => {
            let has_subquery = from.iter().any(|t| matches!(t, crate::sql::ast::TableRef::Subquery { .. }))
                || columns.iter().any(|c| matches!(c, crate::sql::ast::SelectExpr::Subquery(_)))
                || where_predicate.as_ref().map_or(false, |e| expr_has_subquery(e));
            if has_subquery {
                let stmt = crate::sql::ast::Statement::Select {
                    columns: columns.clone(),
                    from: from.clone(),
                    joins: joins.clone(),
                    where_predicate: where_predicate.clone(),
                    group_by: group_by.clone(),
                    having: having.clone(),
                };
                let mut results = Vec::new();
                let header = execute_select_statement(catalog, &stmt, &mut results, None)?;
                println!("{}", format_header(&header));
                for row in results {
                    println!("{}", format_values(&row));
                }
                return Ok(());
            }
            let from_table = match from.first().unwrap() {
                crate::sql::ast::TableRef::Named { name, .. } => name.clone(),
                _ => unreachable!(),
            };
            if joins.is_empty() {
                if group_by.is_some() || columns.iter().any(|c| matches!(c, crate::sql::ast::SelectExpr::Aggregate { .. })) {
                    let mut results = Vec::new();
                    let header = execute_group_query(catalog, &from_table, &columns, group_by.as_deref(), having.clone(), where_predicate, &mut results, None)?;
                    println!("{}", format_header(&header));
                    for row in results {
                        println!("{}", format_values(&row));
                    }
                } else {
                    let table_info = catalog.get_table(&from_table)?;
                    let (idxs, meta) = select_projection_indices(&table_info.columns, &columns)?;
                    println!("{}", format_header(&meta));
                    let mut results = Vec::new();
                    execute_select_with_indexes(catalog, &from_table, where_predicate, &mut results)?;
                    for row in results {
                        let vals = row_to_strings(&row);
                        let projected: Vec<_> = idxs
                            .iter()
                            .map(|p| match p {
                                Projection::Index(i) => vals[*i].clone(),
                                Projection::Literal(s) => s.clone(),
                                Projection::Subquery(_) => String::new(),
                            })
                            .collect();
                        println!("{}", format_values(&projected));
                    }
                }
            } else {
                let plan = crate::execution::plan::MultiJoinPlan { base_table: from_table, joins, projections: columns.clone(), where_predicate };
                let projections = expand_join_projections(&plan, catalog)?;
                let header_meta = join_header(&plan, catalog, &projections)?;
                println!("{}", format_header(&header_meta));
                let mut results = Vec::new();
                execute_multi_join(&plan, catalog, &mut results)?;
                for row in results {
                    println!("{}", format_values(&row));
                }
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
        Statement::CreateSequence(seq) => {
            catalog.create_sequence(&seq.name, seq.start, seq.increment)?;
            println!("Sequence '{}' created successfully", seq.name);
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

pub fn row_to_strings(row: &Row) -> Vec<String> {
    row
        .data
        .0
        .iter()
        .map(|v| v.to_string_value())
        .collect()
}

pub enum Projection {
    Index(usize),
    Literal(String),
    Subquery(Box<crate::sql::ast::Statement>),
}

pub fn select_projection_indices(
    columns: &[(String, ColumnType)],
    projections: &[crate::sql::ast::SelectExpr],
) -> io::Result<(Vec<Projection>, Vec<(String, ColumnType)>)> {
    let use_all = projections.len() == 1 && matches!(projections[0], crate::sql::ast::SelectExpr::All);
    let mut idxs = Vec::new();
    let mut meta = Vec::new();
    if use_all {
        for (i, (n, ty)) in columns.iter().enumerate() {
            idxs.push(Projection::Index(i));
            meta.push((n.clone(), *ty));
        }
    } else {
        for p in projections {
            match p {
                crate::sql::ast::SelectExpr::Column(col) => {
                    let c = col.split('.').last().unwrap_or(col).to_string();
                    if let Some((i, (_, ty))) = columns.iter().enumerate().find(|(_, (name, _))| name == &c) {
                        idxs.push(Projection::Index(i));
                        meta.push((c, *ty));
                    } else {
                        return Err(io::Error::new(io::ErrorKind::Other, format!("Unknown column {c}")));
                    }
                }
                crate::sql::ast::SelectExpr::Aggregate { func, column } => {
                    meta.push((format!("{}({})", func.as_str(), column.clone().unwrap_or("*".into())), ColumnType::Integer));
                }
                crate::sql::ast::SelectExpr::All => {
                    for (i, (n, ty)) in columns.iter().enumerate() {
                        idxs.push(Projection::Index(i));
                        meta.push((n.clone(), *ty));
                    }
                }
                crate::sql::ast::SelectExpr::Subquery(q) => {
                    meta.push(("SUBQUERY".into(), ColumnType::Text));
                    idxs.push(Projection::Subquery(q.clone()));
                }
                crate::sql::ast::SelectExpr::Literal(val) => {
                    let ty = if val.parse::<i32>().is_ok() { ColumnType::Integer } else { ColumnType::Text };
                    meta.push((val.clone(), ty));
                    idxs.push(Projection::Literal(val.clone()));
                }
            }
        }
    }
    Ok((idxs, meta))
}

pub fn expand_join_projections(
    plan: &crate::execution::plan::MultiJoinPlan,
    catalog: &Catalog,
) -> io::Result<Vec<String>> {
    use crate::sql::ast::SelectExpr;
    if plan.projections.len() == 1 && matches!(plan.projections[0], SelectExpr::All) {
        let mut list = Vec::new();
        let base_info = catalog.get_table(&plan.base_table)?;
        for (c, _) in &base_info.columns {
            list.push(format!("{}.{}", plan.base_table, c));
        }
        for jc in &plan.joins {
            let alias = jc.alias.as_ref().unwrap_or(&jc.table);
            let info = catalog.get_table(&jc.table)?;
            for (c, _) in &info.columns {
                list.push(format!("{alias}.{}", c));
            }
        }
        Ok(list)
    } else {
        let mut out = Vec::new();
        for p in &plan.projections {
            if let SelectExpr::Column(c) = p {
                out.push(c.clone());
            }
        }
        Ok(out)
    }
}

pub fn join_header(
    plan: &crate::execution::plan::MultiJoinPlan,
    catalog: &Catalog,
    projections: &[String],
) -> io::Result<Vec<(String, ColumnType)>> {
    use std::collections::HashMap;
    let mut alias_map = HashMap::new();
    alias_map.insert(plan.base_table.clone(), plan.base_table.clone());
    for jc in &plan.joins {
        alias_map.insert(jc.alias.clone().unwrap_or_else(|| jc.table.clone()), jc.table.clone());
    }

    let mut out = Vec::new();
    for p in projections {
        let mut parts = p.split('.');
        let alias = parts.next().ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Bad column"))?;
        let col = parts.next().ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Bad column"))?;
        let table = alias_map.get(alias).ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Bad alias"))?;
        let info = catalog.get_table(table)?;
        let ty = info
            .columns
            .iter()
            .find(|(c, _)| c == col)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Column not found"))?
            .1;
        out.push((p.clone(), ty));
    }
    Ok(out)
}

fn evaluate_with_catalog(
    expr: &crate::sql::ast::Expr,
    values: &std::collections::HashMap<String, String>,
    catalog: &mut Catalog,
) -> io::Result<bool> {
    use crate::sql::ast::Expr;
    match expr {
        Expr::Equals { left, right } => {
            Ok(values.get(left).map(String::as_str).unwrap_or(left)
                == values.get(right).map(String::as_str).unwrap_or(right))
        }
        Expr::NotEquals { left, right } => {
            Ok(values.get(left).map(String::as_str).unwrap_or(left)
                != values.get(right).map(String::as_str).unwrap_or(right))
        }
        Expr::GreaterThan { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(0.0);
            Ok(l > r)
        }
        Expr::GreaterOrEquals { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(0.0);
            Ok(l >= r)
        }
        Expr::LessThan { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(0.0);
            Ok(l < r)
        }
        Expr::LessOrEquals { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(0.0);
            Ok(l <= r)
        }
        Expr::And(a, b) => {
            Ok(evaluate_with_catalog(a, values, catalog)?
                && evaluate_with_catalog(b, values, catalog)?)
        }
        Expr::Or(a, b) => {
            Ok(evaluate_with_catalog(a, values, catalog)?
                || evaluate_with_catalog(b, values, catalog)?)
        }
        Expr::InSubquery { left, query } => {
            let mut rows = Vec::new();
            let header = execute_select_statement(catalog, query, &mut rows, Some(values))?;
            if header.len() != 1 {
                return Err(io::Error::new(io::ErrorKind::Other, "Subquery must return one column"));
            }
            let val = values.get(left).map(String::as_str).unwrap_or(left);
            for r in rows {
                if r.get(0).map(|s| s.as_str()) == Some(val) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::ExistsSubquery { query } => {
            let mut rows = Vec::new();
            let _ = execute_select_statement(catalog, query, &mut rows, Some(values))?;
            Ok(!rows.is_empty())
        }
        Expr::Subquery(_) | Expr::Literal(_) | Expr::FunctionCall { .. } | Expr::DefaultValue => Ok(false),
    }
}

fn expr_has_subquery(expr: &crate::sql::ast::Expr) -> bool {
    use crate::sql::ast::Expr;
    match expr {
        Expr::InSubquery { .. } | Expr::ExistsSubquery { .. } | Expr::Subquery(_) => true,
        Expr::And(a, b) | Expr::Or(a, b) => expr_has_subquery(a) || expr_has_subquery(b),
        _ => false,
    }
}

pub fn execute_select_statement(
    catalog: &mut Catalog,
    stmt: &crate::sql::ast::Statement,
    out: &mut Vec<Vec<String>>,
    context: Option<&std::collections::HashMap<String, String>>,
) -> io::Result<Vec<(String, ColumnType)>> {
    use crate::sql::ast::{SelectExpr, TableRef};
    match stmt {
        crate::sql::ast::Statement::Select { columns, from, joins, where_predicate, group_by, having } => {
            if !joins.is_empty() {
                return Err(io::Error::new(io::ErrorKind::Other, "Unsupported query"));
            }
            let source = from.first().ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Missing FROM"))?;
            match source {
                TableRef::Named { name, alias } => {
                    if group_by.is_some() || columns.iter().any(|c| matches!(c, SelectExpr::Aggregate { .. })) {
                        return execute_group_query(catalog, name, columns, group_by.as_deref(), having.clone(), where_predicate.clone(), out, context);
                    }
                    let info = catalog.get_table(name)?.clone();
                    let (idxs, header) = select_projection_indices(&info.columns, columns)?;
                    let mut rows = Vec::new();
                    execute_select_with_indexes(catalog, name, None, &mut rows)?;
                    for row in rows {
                        let vals = row_to_strings(&row);
                        let mut map = std::collections::HashMap::new();
                        if let Some(ctx) = context {
                            for (k, v) in ctx {
                                map.insert(k.clone(), v.clone());
                            }
                        }
                        for ((c, _), v) in info.columns.iter().zip(vals.iter()) {
                            map.insert(c.clone(), v.clone());
                            let qual = format!("{}.{}", alias.as_deref().unwrap_or(name), c);
                            map.insert(qual, v.clone());
                        }
                        if let Some(pred) = where_predicate {
                            if !evaluate_with_catalog(pred, &map, catalog)? {
                                continue;
                            }
                        }
                        let mut projected = Vec::new();
                        for p in idxs.iter() {
                            match p {
                                Projection::Index(i) => projected.push(vals[*i].clone()),
                                Projection::Literal(s) => projected.push(s.clone()),
                                Projection::Subquery(q) => {
                                    let mut inner_rows = Vec::new();
                                    execute_select_statement(catalog, q, &mut inner_rows, Some(&map))?;
                                    let val = inner_rows.get(0).and_then(|r| r.get(0)).cloned().unwrap_or_default();
                                    projected.push(val);
                                }
                            }
                        }
                        
                        out.push(projected);
                    }
                    Ok(header)
                }
                TableRef::Subquery { query, .. } => {
                    let mut inner_rows = Vec::new();
                    let inner_header = execute_select_statement(catalog, query, &mut inner_rows, context)?;
                    let mut filtered = Vec::new();
                    for row in inner_rows {
                        let mut values = std::collections::HashMap::new();
                        if let Some(ctx) = context {
                            for (k, v) in ctx {
                                values.insert(k.clone(), v.clone());
                            }
                        }
                        for ((col, _), val) in inner_header.iter().zip(row.iter()) {
                            values.insert(col.clone(), val.clone());
                        }
                        if let Some(pred) = where_predicate {
                            if !evaluate_with_catalog(pred, &values, catalog)? {
                                continue;
                            }
                        }
                        filtered.push(row);
                    }
                    if columns.len() == 1 && matches!(columns[0], SelectExpr::All) {
                        out.extend(filtered.clone());
                        Ok(inner_header)
                    } else {
                        let mut header = Vec::new();
                        let mut idxs = Vec::new();
                        for expr in columns {
                            match expr {
                                SelectExpr::Column(c) => {
                                    if let Some((i, (_, ty))) = inner_header.iter().enumerate().find(|(_, (n, _))| n == c) {
                                        idxs.push(i);
                                        header.push((c.clone(), *ty));
                                    } else {
                                        return Err(io::Error::new(io::ErrorKind::Other, format!("Unknown column {c}")));
                                    }
                                }
                                _ => return Err(io::Error::new(io::ErrorKind::Other, "Unsupported projection")),
                            }
                        }
                        for row in filtered {
                            let projected: Vec<_> = idxs.iter().map(|&i| row[i].clone()).collect();
                            out.push(projected);
                        }
                        Ok(header)
                    }
                }
            }
        }
        _ => Err(io::Error::new(io::ErrorKind::Other, "Not a SELECT")),
    }
}

pub fn format_values(vals: &[String]) -> String {
    vals.join(" | ")
}


pub fn format_row(row: &Row) -> String {
    row.data
        .0
        .iter()
        .map(|v| v.to_string_value())
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
