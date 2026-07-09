use aerodb::{
    engine::Engine,
    execution::runtime::{execute_group_query, execute_multi_join, execute_select_with_indexes},
    sql::{ast::Statement, parser::parse_statement},
};
use std::fs;

fn setup_engine(filename: &str) -> Engine {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Engine::new(filename)
}

fn open_engine(filename: &str) -> Engine {
    Engine::new(filename)
}

#[test]
fn indexed_select_obeys_mvcc_visibility_across_transaction_lifecycle() {
    let filename = "runtime_mvcc_indexed_select.db";
    let mut writer = setup_engine(filename);
    writer
        .execute(parse_statement("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap())
        .unwrap();
    writer
        .execute(parse_statement("CREATE INDEX idx_t_name ON t (name)").unwrap())
        .unwrap();
    writer.execute(parse_statement("BEGIN").unwrap()).unwrap();
    writer
        .execute(parse_statement("INSERT INTO t VALUES (1, 'alice')").unwrap())
        .unwrap();

    let predicate = parse_statement("SELECT * FROM t WHERE name = 'alice'").unwrap();
    let selection = match predicate {
        Statement::Select {
            where_predicate, ..
        } => where_predicate,
        _ => unreachable!(),
    };

    let mut same_tx_rows = Vec::new();
    execute_select_with_indexes(
        &mut writer.catalog,
        "t",
        selection.clone(),
        &mut same_tx_rows,
    )
    .unwrap();
    assert_eq!(same_tx_rows.len(), 1);

    let mut other_snapshot = open_engine(filename);
    other_snapshot
        .execute(parse_statement("BEGIN").unwrap())
        .unwrap();
    let mut other_rows = Vec::new();
    execute_select_with_indexes(
        &mut other_snapshot.catalog,
        "t",
        selection.clone(),
        &mut other_rows,
    )
    .unwrap();
    assert!(other_rows.is_empty());

    writer.execute(parse_statement("COMMIT").unwrap()).unwrap();
    drop(writer);
    let mut after_commit = open_engine(filename);
    let mut committed_rows = Vec::new();
    execute_select_with_indexes(
        &mut after_commit.catalog,
        "t",
        selection,
        &mut committed_rows,
    )
    .unwrap();
    assert_eq!(committed_rows.len(), 1);
}

#[test]
fn joins_and_aggregates_read_visible_rows_inside_transaction() {
    let filename = "runtime_mvcc_join_group.db";
    let mut engine = setup_engine(filename);
    engine
        .execute(parse_statement("CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)").unwrap())
        .unwrap();
    engine
        .execute(parse_statement("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)").unwrap())
        .unwrap();
    engine.execute(parse_statement("BEGIN").unwrap()).unwrap();
    engine
        .execute(parse_statement("INSERT INTO a VALUES (1, 'av1')").unwrap())
        .unwrap();
    engine
        .execute(parse_statement("INSERT INTO b VALUES (1, 1)").unwrap())
        .unwrap();

    let stmt = parse_statement("SELECT a.v, b.id FROM a JOIN b ON a.id = b.a_id").unwrap();
    let mut join_rows = Vec::new();
    if let Statement::Select {
        columns,
        from,
        joins,
        where_predicate,
        ..
    } = stmt
    {
        let base_table = match from.first().unwrap() {
            aerodb::sql::ast::TableRef::Named { name, .. } => name.clone(),
            _ => unreachable!(),
        };
        let plan = aerodb::execution::plan::MultiJoinPlan {
            base_table,
            base_alias: None,
            joins,
            projections: columns,
            where_predicate,
        };
        execute_multi_join(&plan, &mut engine.catalog, &mut join_rows).unwrap();
    }
    assert_eq!(join_rows, vec![vec!["av1".to_string(), "1".to_string()]]);

    let group_stmt = parse_statement("SELECT COUNT(*) FROM a").unwrap();
    let mut aggregate_rows = Vec::new();
    if let Statement::Select {
        columns,
        where_predicate,
        ..
    } = group_stmt
    {
        execute_group_query(
            &mut engine.catalog,
            "a",
            &columns,
            None,
            None,
            where_predicate,
            &mut aggregate_rows,
            None,
        )
        .unwrap();
    }
    assert_eq!(aggregate_rows, vec![vec!["1".to_string()]]);
}
