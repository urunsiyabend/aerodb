use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::{Statement, ColumnDef}}, execution::runtime::{execute_select_statement, format_header}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn column_aliases() {
    let filename = "test_column_aliases.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "first_name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "last_name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO employees VALUES (1, 'John', 'Doe')").unwrap()).unwrap();
    let stmt = parse_statement("SELECT first_name AS fname, last_name lname FROM employees").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "fname TEXT | lname TEXT");
        assert_eq!(out, vec![vec!["John".to_string(), "Doe".to_string()]]);
    } else { panic!("expected select") }
}

#[test]
fn table_aliases() {
    let filename = "test_table_aliases.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "first_name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "department_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "departments".into(),
        columns: vec![
            ColumnDef { name: "department_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "department_name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO employees VALUES (1, 'John', 1)").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO departments VALUES (1, 'Sales')").unwrap()).unwrap();
    let stmt = parse_statement("SELECT e.first_name, d.department_name FROM employees AS e JOIN departments d ON e.department_id = d.department_id").unwrap();
    if let Statement::Select { columns, from, joins, where_predicate, .. } = stmt {
        let (base_table, base_alias) = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, alias } => (name.clone(), alias.clone()), _ => panic!("expected table") };
        let plan = aerodb::execution::plan::MultiJoinPlan { base_table, base_alias, joins, projections: columns, where_predicate };
        let mut out = Vec::new();
        aerodb::execution::runtime::execute_multi_join(&plan, &mut catalog, &mut out).unwrap();
        assert_eq!(out, vec![vec!["John".to_string(), "Sales".to_string()]]);
    } else { panic!("expected select") }
}

#[test]
fn mixed_aliases() {
    let filename = "test_mixed_aliases.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "first_name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "last_name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "department_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "departments".into(),
        columns: vec![
            ColumnDef { name: "department_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "department_name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO employees VALUES (1, 'John', 'Doe', 1)").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO departments VALUES (1, 'Sales')").unwrap()).unwrap();
    let stmt = parse_statement("SELECT e.first_name fname, e.last_name lname, d.department_name dept FROM employees e JOIN departments d ON e.department_id = d.department_id").unwrap();
    if let Statement::Select { columns, from, joins, where_predicate, .. } = stmt {
        let (base_table, base_alias) = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, alias } => (name.clone(), alias.clone()), _ => panic!("expected table") };
        let plan = aerodb::execution::plan::MultiJoinPlan { base_table, base_alias, joins, projections: columns, where_predicate };
        let mut out = Vec::new();
        aerodb::execution::runtime::execute_multi_join(&plan, &mut catalog, &mut out).unwrap();
        assert_eq!(out, vec![vec!["John".to_string(), "Doe".to_string(), "Sales".to_string()]]);
    } else { panic!("expected select") }
}

#[test]
fn subquery_alias() {
    let filename = "test_subquery_alias.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "department_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "salary".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    let data = vec![
        (1, 1, 100),
        (2, 1, 300),
        (3, 2, 200),
    ];
    for (id, dept, sal) in data {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO employees VALUES ({}, {}, {})", id, dept, sal)).unwrap()).unwrap();
    }
    let query = "SELECT avg.dept_id, avg.avg_salary FROM ( SELECT department_id dept_id, AVG(salary) avg_salary FROM employees GROUP BY department_id ) avg";
    let stmt = parse_statement(query).unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        out.sort();
        assert_eq!(format_header(&header), "dept_id INTEGER | avg_salary INTEGER");
        assert_eq!(out, vec![vec!["1".to_string(), "200".to_string()], vec!["2".to_string(), "200".to_string()]]);
    } else { panic!("expected select") }
}

