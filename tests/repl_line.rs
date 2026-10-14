//! The REPL line handler shared by `src/main.rs` (0.6.0 final review I2):
//! parse errors are reported with their SQLSTATE and fail an explicit
//! transaction like any other statement error.
mod common;

use aerodb::engine::Engine;
use aerodb::repl::{LineOutcome, run_line};
use common::{engine, rows, table};

fn repl(e: &mut Engine, line: &str) -> String {
    let mut out = Vec::new();
    let outcome = run_line(e, line, &mut out).expect("write to buffer");
    assert!(matches!(outcome, LineOutcome::Continue), "{line}");
    String::from_utf8(out).unwrap()
}

#[test]
fn repl_reports_parse_errors_with_sqlstate_and_fails_the_transaction() {
    let (_dir, mut e) = engine();
    assert_eq!(repl(&mut e, "CREATE TABLE t(id INTEGER)"), "");
    let out = repl(&mut e, "SELEC nonsense");
    assert!(
        out.starts_with("Error: ") && out.contains("(42601)"),
        "{out}"
    );
    // Outside a transaction a parse error changes nothing.
    assert_eq!(repl(&mut e, "INSERT INTO t VALUES (1)"), "");
    assert_eq!(repl(&mut e, "BEGIN"), "");
    let out = repl(&mut e, "SELEC nonsense");
    assert!(out.contains("(42601)"), "{out}");
    let out = repl(&mut e, "INSERT INTO t VALUES (2)");
    assert!(out.contains("(25P02)"), "{out}");
    let out = repl(&mut e, "COMMIT");
    assert!(out.contains("(25P02)"), "{out}");
    assert_eq!(rows(&mut e, "SELECT id FROM t").unwrap(), table(&[&["1"]]));
    // Execution errors carry their SQLSTATE too.
    let out = repl(&mut e, "SELECT nope FROM t");
    assert!(
        out.starts_with("Error: ") && out.contains("(42703)"),
        "{out}"
    );
    let mut sink = Vec::new();
    assert!(matches!(
        run_line(&mut e, ".exit", &mut sink).unwrap(),
        LineOutcome::Exit
    ));
    assert!(matches!(
        run_line(&mut e, "exit", &mut sink).unwrap(),
        LineOutcome::Exit
    ));
    assert!(matches!(
        run_line(&mut e, "  ", &mut sink).unwrap(),
        LineOutcome::Continue
    ));
    assert!(sink.is_empty());
}
