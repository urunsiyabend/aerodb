//! The line handler of the interactive REPL (`src/main.rs`), kept in the
//! library so it is tested with the rest of the engine.

use std::io::{self, Write};

use crate::engine::Engine;
use crate::error::DbError;
use crate::sql::ast::Statement;

/// What the REPL does after a line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineOutcome {
    Continue,
    Exit,
}

/// How the REPL reports an error: the message and its SQLSTATE,
/// e.g. `Error: no such table: t (42P01)`.
pub fn format_error(e: &DbError) -> String {
    format!("Error: {} ({})", e, e.sqlstate())
}

/// Run one input line. Parse errors and execution errors are written to
/// `out` with their SQLSTATE; a parse error inside an explicit transaction
/// fails the transaction (25P02 until ROLLBACK) exactly like an execution
/// error. Query results and status lines are printed by the executor.
pub fn run_line(engine: &mut Engine, line: &str, out: &mut impl Write) -> io::Result<LineOutcome> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(LineOutcome::Continue);
    }
    if trimmed.eq_ignore_ascii_case(".exit") || trimmed.eq_ignore_ascii_case("exit") {
        return Ok(LineOutcome::Exit);
    }
    let result = match engine.parse(trimmed) {
        Ok(Statement::Exit) => return Ok(LineOutcome::Exit),
        Ok(stmt) => engine.execute(stmt),
        Err(e) => Err(e),
    };
    if let Err(e) = result {
        writeln!(out, "{}", format_error(&e))?;
    }
    Ok(LineOutcome::Continue)
}
