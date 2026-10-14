// src/main.rs
//
// The REPL binary links the `aerodb` library instead of re-compiling its
// modules, so the library is built (and unit-tested) once. Each line goes
// through `aerodb::repl::run_line` (tested in tests/repl_line.rs).

use std::io::{self, Write};
use log::info;

use aerodb::engine::Engine;
use aerodb::repl::{LineOutcome, format_error, run_line};

const DATABASE_FILE: &str = "data.aerodb";

fn main() -> io::Result<()> {
    env_logger::init();
    info!("AeroDB v0.6.0 (SQL core, MVCC with WAL). Type .exit to quit.");

    let mut engine = match Engine::open(DATABASE_FILE) {
        Ok(engine) => engine,
        Err(e) => {
            println!("{}", format_error(&e));
            return Err(io::Error::other(e.to_string()));
        }
    };

    loop {
        print!("aerodb> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            break; // EOF
        }
        if run_line(&mut engine, &input, &mut io::stdout())? == LineOutcome::Exit {
            break;
        }
    }

    info!("Goodbye!");
    Ok(())
}
