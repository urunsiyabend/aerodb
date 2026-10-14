# SQL feature audit harness

The audit is evidence, not a pass/fail certification or production implementation.

From repo root:

```bash
python3 audit/run_probes.py "$PWD" "$HOME/.hermes/cache/scratch/aerodb-sql-audit-recheck"
```

Prerequisites: Python 3.9+, Cargo/Rust edition 2024, repository dependencies plus tempfile cached locally. Cargo build runs offline. `run_probes.py` builds `sql_audit.rs` as a temporary standalone Cargo project depending on the provided repository path, not by changing repository source. Build output goes under the output directory `.runner-target`. Database files use tempfile directories (honor TMPDIR).

The adapter uses Engine::execute, captures emitted row text and explicit error markers. Each case runs in a fresh process/database. Exact fixture/query strings, expected row multiset/order/error pattern, actual rows and error text are retained in results.json. Setup errors are findings, never parser-success passes. Expected failures carry `!` in fixture specification. Hard process or build failures never masquerade as semantic success. Classifier self-checks exercise runtime failure, wrong rows, and missing expected errors before SQL launches. Manual-reviewed feature-absence/valid-statement-error classifications are explicit in assess().

Exit 0 means the audit finished, NOT that all probes passed. Consult summary.statuses and full cases. The committed results-0.5.1.json is a snapshot; a rerun writes a separate results.json and does not overwrite it by default when an external output path is supplied. The report/roadmap and machine feature matrix identify official IDs and selected probe scope. Default output is audit/results/ (gitignored).

Baseline tests should be run in a detached scratch worktree because existing repo tests create relative database files. Raw baseline and per-case logs for this run are in /home/urunsiyabend/.hermes/cache/scratch/aerodb-sql-audit. No source fixes, version changes, commit or push were made.
