//! Micro-benchmark for B-Tree point operations under MVCC.
//!
//! Measures the paths that resolve a single logical key: bulk INSERT (each row
//! runs a duplicate-key probe), indexed equality SELECT (index probe + base-table
//! visibility probe), and single-row UPDATE by key. Run with
//! `cargo run --release --example point_lookup_bench -- [rows] [lookups]`.

use aerodb::{
    engine::Engine,
    execution::runtime::execute_select_with_indexes,
    sql::{ast::Expr, parser::parse_statement},
};
use std::{env, fs, time::Instant};

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_statement(sql).unwrap()).unwrap();
}

fn main() {
    let mut args = env::args().skip(1);
    let rows: i32 = args.next().and_then(|a| a.parse().ok()).unwrap_or(4000);
    let lookups: i32 = args.next().and_then(|a| a.parse().ok()).unwrap_or(2000);

    let path = env::temp_dir().join(format!("aerodb-point-bench-{}.db", std::process::id()));
    let filename = path.to_str().unwrap().to_string();
    let cleanup = || {
        for suffix in ["", ".wal", ".clog"] {
            let _ = fs::remove_file(format!("{filename}{suffix}"));
        }
    };
    cleanup();

    let mut engine = Engine::new(&filename);
    exec(
        &mut engine,
        "CREATE TABLE t (id INTEGER, v INTEGER, pad TEXT)",
    );
    exec(&mut engine, "CREATE INDEX idx_t_v ON t (v)");

    let started = Instant::now();
    exec(&mut engine, "BEGIN");
    for id in 0..rows {
        exec(
            &mut engine,
            &format!("INSERT INTO t VALUES ({id}, {}, 'row-{id}')", id * 7),
        );
    }
    exec(&mut engine, "COMMIT");
    let insert = started.elapsed();

    let started = Instant::now();
    let mut found = 0usize;
    for i in 0..lookups {
        let id = (i * 7919) % rows;
        let mut out = Vec::new();
        let selection = Expr::Equals {
            left: "v".into(),
            right: (id * 7).to_string(),
        };
        execute_select_with_indexes(&mut engine.catalog, "t", Some(selection), &mut out).unwrap();
        found += out.len();
    }
    let lookup = started.elapsed();
    assert_eq!(found, lookups as usize, "every probe finds exactly its row");

    let updates = lookups.min(200);
    let started = Instant::now();
    exec(&mut engine, "BEGIN");
    for i in 0..updates {
        let id = (i * 7919) % rows;
        exec(
            &mut engine,
            &format!("UPDATE t SET pad = 'upd-{i}' WHERE id = {id}"),
        );
    }
    exec(&mut engine, "COMMIT");
    let update = started.elapsed();

    println!("rows={rows} lookups={lookups} updates={updates}");
    println!(
        "insert_total_ms={:.1} insert_per_row_us={:.1}",
        insert.as_secs_f64() * 1e3,
        insert.as_secs_f64() * 1e6 / rows as f64
    );
    println!(
        "indexed_lookup_total_ms={:.1} indexed_lookup_per_probe_us={:.1}",
        lookup.as_secs_f64() * 1e3,
        lookup.as_secs_f64() * 1e6 / lookups as f64
    );
    println!(
        "update_total_ms={:.1} update_per_row_us={:.1}",
        update.as_secs_f64() * 1e3,
        update.as_secs_f64() * 1e6 / updates as f64
    );

    drop(engine);
    cleanup();
}
