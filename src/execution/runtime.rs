use crate::catalog::Catalog;
use crate::sql::ast::{Statement, Expr, expr_to_string};
use crate::constraints::{Constraint, not_null::NotNullConstraint, foreign_key::ForeignKeyConstraint, default::DefaultConstraint, primary_key::PrimaryKeyConstraint};
use crate::storage::btree::BTree;
use crate::storage::row::{Row, RowData, ColumnValue, ColumnType, build_row_data};
use std::collections::HashMap;
use crate::error::{DbError, DbResult};
use crate::planner::aggregate;

pub fn execute_delete(catalog: &mut Catalog, table_name: &str, selection: Option<Expr>) -> DbResult<usize> {
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
                    if matches!(crate::sql::ast::evaluate_expression(expr, &values), ColumnValue::Boolean(true)) {
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
) -> DbResult<usize> {
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
                .ok_or_else(|| DbError::ColumnNotFound(col.clone()))?;
            let ty = columns[idx].1;
            let cv = match ty {
                ColumnType::Integer => ColumnValue::Integer(
                    val.parse::<i32>()
                        .map_err(|_| DbError::InvalidValue("Invalid INTEGER".into()))?,
                ),
                ColumnType::Text => ColumnValue::Text(val.clone()),
                ColumnType::Boolean => match val.to_ascii_lowercase().as_str() {
                    "true" => ColumnValue::Boolean(true),
                    "false" => ColumnValue::Boolean(false),
                    _ => {
                        return Err(DbError::InvalidValue("Invalid BOOLEAN".into()))
                    }
                },
                ColumnType::Char(len) => {
                    if val.len() > len {
                        return Err(DbError::InvalidValue(
                            format!("Value '{}' for column '{}' exceeds length {}", val, col, len)
                        ));
                    }
                    let mut s = val.clone();
                    if s.len() < len {
                        s.push_str(&" ".repeat(len - s.len()));
                    }
                    ColumnValue::Char(s)
                }
                ColumnType::SmallInt { unsigned, .. } => {
                    let i = val.parse::<i32>().map_err(|_| DbError::ParseError("Invalid SMALLINT".into()))?;
                    if unsigned {
                        if !(0..=65535).contains(&i) {
                            return Err(DbError::Overflow);
                        }
                    } else if !(-32768..=32767).contains(&i) {
                        return Err(DbError::Overflow);
                    }
                    ColumnValue::Integer(i)
                }
                ColumnType::MediumInt { unsigned, .. } => {
                    let i = val.parse::<i32>().map_err(|_| DbError::ParseError("Invalid MEDIUMINT".into()))?;
                    if unsigned {
                        if !(0..=16_777_215).contains(&i) {
                            return Err(DbError::Overflow);
                        }
                    } else if !(-8_388_608..=8_388_607).contains(&i) {
                        return Err(DbError::Overflow);
                    }
                    ColumnValue::Integer(i)
                }
                ColumnType::Double { unsigned, .. } => {
                    let f = val.parse::<f64>().map_err(|_| DbError::ParseError("Invalid DOUBLE".into()))?;
                    if unsigned && f < 0.0 {
                        return Err(DbError::Overflow);
                    }
                    ColumnValue::Double(f)
                }
                ColumnType::Date => {
                    match crate::storage::row::parse_date(&val) {
                        Some(d) => ColumnValue::Date(d),
                        None => {
                            return Err(DbError::ParseError("Invalid DATE".into()));
                        }
                    }
                }
                ColumnType::DateTime => {
                    match crate::storage::row::parse_datetime(&val) {
                        Some(ts) => ColumnValue::DateTime(ts),
                        None => return Err(DbError::ParseError("Invalid DATETIME".into())),
                    }
                }
                ColumnType::Timestamp => {
                    match crate::storage::row::parse_datetime(&val) {
                        Some(ts) => ColumnValue::Timestamp(ts),
                        None => return Err(DbError::ParseError("Invalid TIMESTAMP".into())),
                    }
                }
                ColumnType::Time => {
                    match crate::storage::row::parse_time(&val) {
                        Some(t) => ColumnValue::Time(t),
                        None => return Err(DbError::ParseError("Invalid TIME".into())),
                    }
                }
                ColumnType::Year => {
                    match crate::storage::row::parse_year(&val) {
                        Some(y) => ColumnValue::Year(y),
                        None => return Err(DbError::ParseError("Invalid YEAR".into())),
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
                    if matches!(crate::sql::ast::evaluate_expression(expr, &values), ColumnValue::Boolean(true)) {
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

pub fn execute_insert(
    catalog: &mut Catalog,
    table_name: &str,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Expr>>,
) -> DbResult<usize> {
    let table_info = catalog.get_table(table_name)?.clone();
    let root_page = table_info.root_page;
    let columns_meta = table_info.columns.clone();
    let fks = table_info.fks.clone();

    let mut inserted = 0usize;
    let mut result: DbResult<()> = Ok(());

    for row_vals in rows {
        if let Err(e) = (|| {
            let mut vals = Vec::new();
            if let Some(ref cols) = columns {
                if row_vals.len() != cols.len() {
                    return Err(DbError::InvalidValue(format!(
                        "INSERT column/value count mismatch: expected {} columns, got {} values",
                        cols.len(),
                        row_vals.len()
                    )));
                }
                for c in cols {
                    if !columns_meta.iter().any(|(n, _)| n == c) {
                        return Err(DbError::ColumnNotFound(c.clone()));
                    }
                }
                for (idx, (col_name, _)) in columns_meta.iter().enumerate() {
                    let auto = table_info.auto_increment.get(idx).copied().unwrap_or(false);
                    if let Some(pos) = cols.iter().position(|c| c == col_name) {
                        let expr = &row_vals[pos];
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
                                return Err(DbError::InvalidValue(format!(
                                    "Cannot use DEFAULT for column '{}' - no default value defined",
                                    col_name
                                )));
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
                            return Err(DbError::InvalidValue(format!("Column '{}' requires a value or DEFAULT", col_name)));
                        }
                    }
                }
            } else {
                if row_vals.len() != columns_meta.len() {
                    return Err(DbError::InvalidValue(format!(
                        "INSERT has wrong number of values: expected {}, got {}",
                        columns_meta.len(),
                        row_vals.len()
                    )));
                }
                for (idx, expr) in row_vals.iter().enumerate() {
                    let auto = table_info.auto_increment.get(idx).copied().unwrap_or(false);
                    if auto {
                        if matches!(expr, Expr::DefaultValue) {
                            let seq = format!("{}_{}", table_name, columns_meta[idx].0);
                            let next = catalog.next_sequence_value(&seq)?;
                            vals.push(next.to_string());
                        } else {
                            let s = expr_to_string(expr);
                            if let Ok(v) = s.parse::<i64>() {
                                catalog.update_sequence_current(&format!("{}_{}", table_name, columns_meta[idx].0), v)?;
                            }
                            vals.push(s);
                        }
                    } else if matches!(expr, Expr::DefaultValue) {
                        if let Some(def) = table_info.default_values.get(idx).and_then(|o| o.as_ref()) {
                            vals.push(DefaultConstraint::evaluate(def)?);
                        } else if !table_info.not_null[idx] {
                            vals.push("NULL".into());
                        } else {
                            return Err(DbError::InvalidValue(format!(
                                "Cannot use DEFAULT for column '{}' - no default value defined",
                                columns_meta[idx].0
                            )));
                        }
                    } else {
                        vals.push(expr_to_string(expr));
                    }
                }
            }

            let mut row_data = build_row_data(&vals, &columns_meta)
                .map_err(|e| DbError::InvalidValue(e))?;

            let nn = NotNullConstraint;
            nn.validate_insert(catalog, &table_info, &mut row_data)?;

            let fk_cons = ForeignKeyConstraint { fks: &fks };
            fk_cons.validate_insert(catalog, &table_info, &mut row_data)?;
            if let Some(ref pk_cols) = table_info.primary_key {
                let pk_cons = PrimaryKeyConstraint { columns: pk_cols };
                pk_cons.validate_insert(catalog, &table_info, &mut row_data)?;
            }
            let key = match row_data.0.get(0) {
                Some(ColumnValue::Integer(i)) => *i,
                _ => {
                    return Err(DbError::InvalidValue("First column must be an INTEGER key".into()));
                }
            };
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
            table_btree.insert(key, row_data.clone())?;
            let new_root = table_btree.root_page();
            drop(table_btree);
            if new_root != root_page {
                catalog.get_table_mut(table_name)?.root_page = new_root;
            }
            catalog.insert_into_indexes(table_name, &row_data)?;
            inserted += 1;
            Ok(())
        })() {
            result = Err(e);
            break;
        }
    }

    if result.is_ok() {
        Ok(inserted)
    } else {
        Err(result.unwrap_err())
    }
}

pub fn execute_select_with_indexes(
    catalog: &mut Catalog,
    table_name: &str,
    selection: Option<Expr>,
    out: &mut Vec<Row>,
) -> DbResult<bool> {
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
            if matches!(crate::sql::ast::evaluate_expression(expr, &values), ColumnValue::Boolean(true)) {
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
) -> DbResult<()> {
    use crate::sql::ast::evaluate_expression;
    let mut result_rows: Vec<std::collections::HashMap<String, ColumnValue>> = Vec::new();

    // base table scan
    {
        let base_info = catalog.get_table(&plan.base_table)?.clone();
        let mut tree = BTree::open_root(&mut catalog.pager, base_info.root_page)?;
        let mut cursor = tree.scan_all_rows();
        while let Some(row) = cursor.next() {
            let mut map = std::collections::HashMap::new();
            let alias = plan.base_alias.as_deref().unwrap_or(&plan.base_table);
            for ((c, _), v) in base_info.columns.iter().zip(row.data.0.iter()) {
                map.insert(format!("{alias}.{c}"), v.clone());
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
            if !matches!(evaluate_expression(pred, &str_map), ColumnValue::Boolean(true)) {
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
) -> DbResult<Vec<(String, ColumnType)>> {
    let mut rows = Vec::new();
    execute_select_with_indexes(catalog, table_name, None, &mut rows)?;
    let table_info = catalog.get_table(table_name)?.clone();
    aggregate::validate_group_by(projections, group_by, having.as_ref(), &table_info.columns)?;

    let mut groups: std::collections::HashMap<Vec<String>, Vec<crate::storage::row::Row>> = std::collections::HashMap::new();
    let mut col_pos = std::collections::HashMap::new();
    for (i, (c, _)) in table_info.columns.iter().enumerate() {
        col_pos.insert(c.clone(), i);
        col_pos.insert(c.to_uppercase(), i);
    }
    let mut get_idx = |name: &str| -> DbResult<usize> {
        col_pos
            .get(name)
            .copied()
            .or_else(|| {
                name.rsplit('.').next().and_then(|n| col_pos.get(n).copied())
            })
            .ok_or_else(|| DbError::ColumnNotFound(name.to_string()))
    };
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
            let mut parts = Vec::new();
            for c in gb {
                let idx = get_idx(c)?;
                parts.push(row.data.0[idx].to_string_value());
            }
            parts
        } else {
            Vec::new()
        };
        groups.entry(key).or_default().push(row);
    }

    let mut header = Vec::new();
    use crate::sql::ast::SelectItem;
    for expr in projections {
        match &expr.expr {
            SelectItem::Column(c) => {
                let idx = get_idx(c)?;
                header.push((expr.alias.clone().unwrap_or(c.clone()), table_info.columns[idx].1));
            }
            SelectItem::Aggregate { func, column } => {
                let name = format!("{}({})", func.as_str(), column.clone().unwrap_or("*".into()));
                header.push((expr.alias.clone().unwrap_or(name), ColumnType::Integer));
            }
            SelectItem::All => {
                for (c, ty) in &table_info.columns {
                    header.push((c.clone(), *ty));
                }
            }
            SelectItem::Subquery(_) => {
                header.push((expr.alias.clone().unwrap_or("SUBQUERY".into()), ColumnType::Text));
            }
            SelectItem::Literal(val) => {
                let ty = if val.parse::<i32>().is_ok() { ColumnType::Integer } else { ColumnType::Text };
                header.push((expr.alias.clone().unwrap_or_else(|| val.clone()), ty));
            }
            SelectItem::Expr(_) => {
                header.push((expr.alias.clone().unwrap_or("EXPR".into()), ColumnType::Integer));
            }
        }
    }

    for (_key, grows) in groups {
        let mut result_row = Vec::new();
        let mut value_map = std::collections::HashMap::new();
        for expr in projections {
            match &expr.expr {
                SelectItem::Column(c) => {
                    let idx = get_idx(c)?;
                    let val = &grows[0].data.0[idx];
                    let s = val.to_string_value();
                    value_map.insert(c.clone(), s.clone());
                    result_row.push(s);
                }
                SelectItem::Aggregate { func, column } => {
                    let val = match func {
                        crate::sql::ast::AggFunc::Count => grows.len().to_string(),
                        crate::sql::ast::AggFunc::Sum => {
                            let idx = get_idx(column.as_ref().unwrap())?;
                            match table_info.columns[idx].1 {
                                ColumnType::Double { .. } => {
                                    let mut sum = 0.0;
                                    for r in &grows {
                                        if let ColumnValue::Double(f) = r.data.0[idx] {
                                            sum += f;
                                        }
                                    }
                                    sum.to_string()
                                }
                                _ => {
                                    let mut sum = 0i64;
                                    for r in &grows {
                                        if let ColumnValue::Integer(i) = r.data.0[idx] {
                                            sum += i as i64;
                                        }
                                    }
                                    sum.to_string()
                                }
                            }
                        }
                        crate::sql::ast::AggFunc::Min => {
                            let idx = get_idx(column.as_ref().unwrap())?;
                            let mut min_val: Option<i32> = None;
                            for r in &grows {
                                if let ColumnValue::Integer(i) = r.data.0[idx] {
                                    min_val = Some(min_val.map_or(i, |m| m.min(i)));
                                }
                            }
                            min_val.unwrap_or(0).to_string()
                        }
                        crate::sql::ast::AggFunc::Max => {
                            let idx = get_idx(column.as_ref().unwrap())?;
                            let mut max_val: Option<i32> = None;
                            for r in &grows {
                                if let ColumnValue::Integer(i) = r.data.0[idx] {
                                    max_val = Some(max_val.map_or(i, |m| m.max(i)));
                                }
                            }
                            max_val.unwrap_or(0).to_string()
                        }
                        crate::sql::ast::AggFunc::Avg => {
                            let idx = get_idx(column.as_ref().unwrap())?;
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
                    let key = expr.alias.clone().unwrap_or(name.clone());
                    value_map.insert(key, val.clone());
                    if expr.alias.is_some() {
                        value_map.insert(name, val.clone());
                    }
                    result_row.push(val);
                }
                SelectItem::All => {
                    for (i, _) in &table_info.columns {
                        let idx = get_idx(i)?;
                        let v = &grows[0].data.0[idx];
                        let s = v.to_string_value();
                        value_map.insert(i.clone(), s.clone());
                        result_row.push(s);
                    }
                }
                SelectItem::Subquery(sub) => {
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
                SelectItem::Literal(val) => {
                    result_row.push(val.clone());
                }
                SelectItem::Expr(expr) => {
                    let map = table_info
                        .columns
                        .iter()
                        .zip(grows[0].data.0.iter())
                        .map(|((c, _), v)| (c.clone(), v.to_string_value()))
                        .collect::<std::collections::HashMap<_, _>>();
                    let val = crate::sql::ast::evaluate_expression(expr, &map).to_string_value();
                    result_row.push(val);
                }
            }
        }
        if let Some(ref pred) = having {
            if !matches!(crate::sql::ast::evaluate_expression(pred, &value_map), ColumnValue::Boolean(true)) {
                continue;
            }
        }
        out.push(result_row);
    }
    Ok(header)
}

pub fn handle_statement(catalog: &mut Catalog, stmt: Statement) -> DbResult<()> {
    match stmt {
        Statement::CreateTable { table_name, columns, fks, primary_key, if_not_exists } => {
            let auto_cols: Vec<_> = columns.iter().filter(|c| c.auto_increment).collect();
            if auto_cols.len() > 1 {
                return Err(DbError::InvalidValue("Only one AUTO_INCREMENT column allowed per table".into()));
            }
            let cols: Vec<_> = columns
                .into_iter()
                .map(|c| (c.name.clone(), c.col_type, c.not_null, c.default_value, c.auto_increment))
                .collect();
            match catalog.create_table_with_fks(&table_name, cols.clone(), fks, primary_key.clone()) {
                Ok(()) => println!("Table {} created", table_name),
                Err(e) => {
                    if if_not_exists && e.to_string().contains("already exists") {
                        println!("Table {} already exists", table_name);
                    } else {
                        return Err(DbError::from(e));
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
        Statement::DropIndex { name } => {
            if catalog.drop_index(&name)? {
                println!("Index {} dropped", name);
            } else {
                return Err(DbError::NotFound(format!("index '{}' not found", name)));
            }
        }
        Statement::Insert { table_name, columns: col_list, rows } => {
            execute_insert(catalog, &table_name, col_list, rows)?;
        }
        Statement::Select { columns, from, joins, where_predicate, group_by, having } => {
            let has_subquery = from.iter().any(|t| matches!(t, crate::sql::ast::TableRef::Subquery { .. }))
                || columns.iter().any(|c| matches!(c.expr, crate::sql::ast::SelectItem::Subquery(_)))
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
            if from.is_empty() {
                let stmt = crate::sql::ast::Statement::Select {
                    columns: columns.clone(),
                    from: Vec::new(),
                    joins: Vec::new(),
                    where_predicate: None,
                    group_by: None,
                    having: None,
                };
                let mut results = Vec::new();
                let header = execute_select_statement(catalog, &stmt, &mut results, None)?;
                println!("{}", format_header(&header));
                for row in results {
                    println!("{}", format_values(&row));
                }
                return Ok(());
            }
            let (from_table, base_alias) = match from.first().unwrap() {
                crate::sql::ast::TableRef::Named { name, alias } => {
                    (name.clone(), alias.clone())
                }
                _ => unreachable!(),
            };
            if joins.is_empty() {
                if group_by.is_some() || columns.iter().any(|c| matches!(c.expr, crate::sql::ast::SelectItem::Aggregate { .. })) {
                    let mut results = Vec::new();
                    let header = execute_group_query(catalog, &from_table, &columns, group_by.as_deref(), having.clone(), where_predicate, &mut results, None)?;
                    println!("{}", format_header(&header));
                    for row in results {
                        println!("{}", format_values(&row));
                    }
                } else {
                    let table_info = catalog.get_table(&from_table)?.clone();
                    let (idxs, meta) = select_projection_indices(&table_info.columns, &columns)?;
                    println!("{}", format_header(&meta));
                    let mut results = Vec::new();
                    execute_select_with_indexes(catalog, &from_table, where_predicate, &mut results)?;
                    for row in results {
                        let vals = row_to_strings(&row);
                        let mut val_map = std::collections::HashMap::new();
                        for ((c, _), v) in table_info.columns.iter().zip(vals.iter()) {
                            val_map.insert(c.clone(), v.clone());
                        }
                        let projected: Vec<_> = idxs
                            .iter()
                            .map(|p| match p {
                                Projection::Index(i) => vals[*i].clone(),
                                Projection::Literal(s) => s.clone(),
                                Projection::Subquery(_) => String::new(),
                                Projection::Expr(expr) => crate::sql::ast::evaluate_expression(expr, &val_map).to_string_value(),
                            })
                            .collect();
                        println!("{}", format_values(&projected));
                    }
                }
            } else {
                let plan = crate::execution::plan::MultiJoinPlan { base_table: from_table, base_alias, joins, projections: columns.clone(), where_predicate };
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
    Expr(Box<crate::sql::ast::Expr>),
}

pub fn select_projection_indices(
    columns: &[(String, ColumnType)],
    projections: &[crate::sql::ast::SelectExpr],
) -> DbResult<(Vec<Projection>, Vec<(String, ColumnType)>)> {
    use crate::sql::ast::SelectItem;
    let use_all = projections.len() == 1 && matches!(projections[0].expr, SelectItem::All);
    let mut idxs = Vec::new();
    let mut meta = Vec::new();
    if use_all {
        for (i, (n, ty)) in columns.iter().enumerate() {
            idxs.push(Projection::Index(i));
            meta.push((n.clone(), *ty));
        }
    } else {
        for p in projections {
            match &p.expr {
                SelectItem::Column(col) => {
                    let c = col.split('.').last().unwrap_or(col).to_string();
                    if let Some((i, (_, ty))) = columns.iter().enumerate().find(|(_, (name, _))| name == &c) {
                        idxs.push(Projection::Index(i));
                        let name = p.alias.clone().unwrap_or(c);
                        meta.push((name, *ty));
                    } else {
                        return Err(DbError::ColumnNotFound(c.clone()));
                    }
                }
                SelectItem::Aggregate { func, column } => {
                    let name = format!("{}({})", func.as_str(), column.clone().unwrap_or("*".into()));
                    let header = p.alias.clone().unwrap_or(name);
                    meta.push((header, ColumnType::Integer));
                }
                SelectItem::All => {
                    for (i, (n, ty)) in columns.iter().enumerate() {
                        idxs.push(Projection::Index(i));
                        meta.push((n.clone(), *ty));
                    }
                }
                SelectItem::Subquery(q) => {
                    meta.push((p.alias.clone().unwrap_or("SUBQUERY".into()), ColumnType::Text));
                    idxs.push(Projection::Subquery(q.clone()));
                }
                SelectItem::Literal(val) => {
                    let ty = if val.parse::<i32>().is_ok() { ColumnType::Integer } else { ColumnType::Text };
                    meta.push((p.alias.clone().unwrap_or_else(|| val.clone()), ty));
                    idxs.push(Projection::Literal(val.clone()));
                }
                SelectItem::Expr(expr) => {
                    meta.push((p.alias.clone().unwrap_or("EXPR".into()), ColumnType::Double { precision: 8, scale: 2, unsigned: false }));
                    idxs.push(Projection::Expr(expr.clone()));
                }
            }
        }
    }
    Ok((idxs, meta))
}

pub fn expand_join_projections(
    plan: &crate::execution::plan::MultiJoinPlan,
    catalog: &Catalog,
) -> DbResult<Vec<String>> {
    use crate::sql::ast::{SelectExpr, SelectItem};
    if plan.projections.len() == 1 && matches!(plan.projections[0].expr, SelectItem::All) {
        let mut list = Vec::new();
        let base_info = catalog.get_table(&plan.base_table)?;
        let base_alias = plan.base_alias.as_deref().unwrap_or(&plan.base_table);
        for (c, _) in &base_info.columns {
            list.push(format!("{base_alias}.{}", c));
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
            if let SelectItem::Column(c) = &p.expr {
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
) -> DbResult<Vec<(String, ColumnType)>> {
    use std::collections::HashMap;
    let mut alias_map = HashMap::new();
    let base_alias = plan.base_alias.as_ref().unwrap_or(&plan.base_table);
    alias_map.insert(base_alias.clone(), plan.base_table.clone());
    for jc in &plan.joins {
        alias_map.insert(jc.alias.clone().unwrap_or_else(|| jc.table.clone()), jc.table.clone());
    }

    let mut out = Vec::new();
    for p in projections {
        let mut parts = p.split('.');
        let alias = parts.next().ok_or_else(|| DbError::ParseError("Bad column".into()))?;
        let col = parts.next().ok_or_else(|| DbError::ParseError("Bad column".into()))?;
        let table = alias_map.get(alias).ok_or_else(|| DbError::ParseError("Bad alias".into()))?;
        let info = catalog.get_table(table)?;
        let ty = info
            .columns
            .iter()
            .find(|(c, _)| c == col)
            .ok_or_else(|| DbError::ColumnNotFound(col.to_string()))?
            .1;
        out.push((p.clone(), ty));
    }
    Ok(out)
}

fn evaluate_with_catalog(
    expr: &crate::sql::ast::Expr,
    values: &std::collections::HashMap<String, String>,
    catalog: &mut Catalog,
) -> DbResult<bool> {
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
        Expr::Add { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(0.0);
            Ok((l + r) != 0.0)
        }
        Expr::Subtract { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(0.0);
            Ok((l - r) != 0.0)
        }
        Expr::Multiply { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(0.0);
            Ok((l * r) != 0.0)
        }
        Expr::Divide { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(1.0);
            if r == 0.0 { Ok(false) } else { Ok((l / r) != 0.0) }
        }
        Expr::Modulo { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<f64>().unwrap_or(0.0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<f64>().unwrap_or(1.0);
            if r == 0.0 { Ok(false) } else { Ok((l % r) != 0.0) }
        }
        Expr::BitwiseAnd { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<i64>().unwrap_or(0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<i64>().unwrap_or(0);
            Ok((l & r) != 0)
        }
        Expr::BitwiseOr { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<i64>().unwrap_or(0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<i64>().unwrap_or(0);
            Ok((l | r) != 0)
        }
        Expr::BitwiseXor { left, right } => {
            let l = values.get(left).map(String::as_str).unwrap_or(left).parse::<i64>().unwrap_or(0);
            let r = values.get(right).map(String::as_str).unwrap_or(right).parse::<i64>().unwrap_or(0);
            Ok((l ^ r) != 0)
        }
        Expr::Between { expr, low, high } => {
            let v = values.get(expr).map(String::as_str).unwrap_or(expr).parse::<f64>().unwrap_or(0.0);
            let l = values.get(low).map(String::as_str).unwrap_or(low).parse::<f64>().unwrap_or(0.0);
            let h = values.get(high).map(String::as_str).unwrap_or(high).parse::<f64>().unwrap_or(0.0);
            Ok(v >= l && v <= h)
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
                return Err(DbError::InvalidValue("Subquery must return one column".into()));
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
) -> DbResult<Vec<(String, ColumnType)>> {
    use crate::sql::ast::{SelectExpr, SelectItem, TableRef};
    use crate::storage::row::ColumnType;
    match stmt {
        crate::sql::ast::Statement::Select { columns, from, joins, where_predicate, group_by, having } => {
            if from.is_empty() {
                if !joins.is_empty() || where_predicate.is_some() || group_by.is_some() || having.is_some() {
                    return Err(DbError::InvalidValue("Unsupported query".into()));
                }
                let mut row = Vec::new();
                let mut header = Vec::new();
                for expr in columns {
                    match &expr.expr {
                        SelectItem::Literal(v) => {
                            let name = expr.alias.clone().unwrap_or_else(|| v.clone());
                            let ty = if v.parse::<i32>().is_ok() { ColumnType::Integer } else { ColumnType::Text };
                            header.push((name, ty));
                            row.push(v.clone());
                        }
                        SelectItem::Expr(e) => {
                            let val = crate::sql::ast::evaluate_expression(e, &std::collections::HashMap::new()).to_string_value();
                            header.push((expr.alias.clone().unwrap_or("EXPR".into()), ColumnType::Double { precision: 8, scale: 2, unsigned: false }));
                            row.push(val);
                        }
                        SelectItem::Subquery(q) => {
                            let mut inner = Vec::new();
                            let inner_header = execute_select_statement(catalog, q, &mut inner, context)?;
                            let val = inner.get(0).and_then(|r| r.get(0)).cloned().unwrap_or_default();
                            let ty = inner_header.get(0).map(|(_, t)| *t).unwrap_or(ColumnType::Text);
                            header.push((expr.alias.clone().unwrap_or("SUBQUERY".into()), ty));
                            row.push(val);
                        }
                        _ => return Err(DbError::InvalidValue("Unsupported projection".into())),
                    }
                }
                out.push(row);
                return Ok(header);
            }
            if !joins.is_empty() {
                return Err(DbError::InvalidValue("Unsupported query".into()));
            }
            let source = from.first().ok_or_else(|| DbError::ParseError("Missing FROM".into()))?;
            match source {
                TableRef::Named { name, alias } => {
                    if group_by.is_some() || columns.iter().any(|c| matches!(c.expr, SelectItem::Aggregate { .. })) {
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
                                Projection::Expr(expr) => {
                                    let val = crate::sql::ast::evaluate_expression(expr, &map).to_string_value();
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
                    if columns.len() == 1 && matches!(columns[0].expr, SelectItem::All) {
                        out.extend(filtered.clone());
                        Ok(inner_header)
                    } else {
                        let mut header = Vec::new();
                        let mut idxs = Vec::new();
                        for expr in columns {
                            match &expr.expr {
                                SelectItem::Column(c) => {
                                    let base = c.split('.').last().unwrap_or(c);
                                    if let Some((i, (_, ty))) = inner_header.iter().enumerate().find(|(_, (n, _))| n == base) {
                                        idxs.push(i);
                                        header.push((expr.alias.clone().unwrap_or(base.to_string()), *ty));
                                    } else {
                                        return Err(DbError::ColumnNotFound(c.clone()));
                                    }
                                }
                                _ => return Err(DbError::InvalidValue("Unsupported projection".into())),
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
        _ => Err(DbError::InvalidValue("Not a SELECT".into())),
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
