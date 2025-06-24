<p align="center">
<img src="images/logo.png" width="200px" height="200px" alt="AeroDB">
</p>

AeroDB is a lightweight, portable relational database engine designed for efficiency and simplicity. It utilizes a B-tree-based architecture to manage and organize data, ensuring fast and reliable access. All data— including tables, indexes, and metadata— is stored compactly within a single file, making AeroDB highly suitable for embedded applications, standalone deployments, and scenarios where minimal configuration and footprint are critical. Its streamlined design supports easy integration while maintaining the core functionalities expected from a relational database management system.

## Installation

Ensure that [Rust](https://www.rust-lang.org/) and `cargo` are installed. Clone the repository and build the release binary:

```bash
git clone https://github.com/yourname/aerodb.git
cd aerodb
cargo build --release
```

Alternatively, install the command-line tool directly with:

```bash
cargo install --path .
```

## Building

AeroDB uses [Cargo](https://doc.rust-lang.org/cargo/). To build the project run:

```bash
cargo build
```

## Running

After building, launch the CLI with:

```bash
cargo run
```

A prompt will appear where you can enter simple SQL commands (CREATE TABLE, INSERT, SELECT, DELETE) or type `.exit` to quit.

## Usage

A simple session might look like:

```sql
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
```

### Inserting Data

Rows can be inserted individually or in bulk:

```sql
INSERT INTO tbl [(col,...)] VALUES (v1,...), (v2,...);
```

The resulting output will list all inserted rows. See the `tests/` directory for additional query examples.

## Schema Changes

The engine supports basic DDL operations:

```sql
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
CREATE INDEX idx_name ON users(name);
DROP INDEX idx_name;
DROP TABLE users;
```

`DROP INDEX` removes the specified index from the catalog so new queries fall back to full table scans when appropriate.

## Project Structure

- `src/` – Rust source code for the database engine.
- `tests/` – Integration tests showcasing various SQL features.
- `images/` – Project images and logos used in documentation.
- `Cargo.toml` – Project configuration and dependencies.

## Development Approach

Development follows a **Test-Driven Development (TDD)** workflow:

1. **Write failing tests** for new features or bug fixes.
2. **Implement** the minimal code to make those tests pass.
3. **Run the full test suite** to ensure all tests succeed.
4. **Refactor** for readability and maintainability while keeping tests green.
5. Add additional tests for uncovered edge cases.

## Contributing

Pull requests are welcome! Please open an issue to discuss major changes beforehand. When contributing, ensure all tests pass and follow the existing code style.

## TODO

These tasks outline upcoming work and reference articles we plan to publish.

- [x] **Expand SQL parser** to support JOINs, nested queries and basic functions.
- [x] **Flesh out transactions** with ACID semantics and accompanying tests.
- [ ] **Document storage engine** internals and page layout in a dedicated article.
- [x] **Add secondary indexes** to accelerate lookups on non-primary keys.
- [ ] **Implement concurrency control** (locking or MVCC) with tests.
- [ ] **Write tutorial articles** detailing the B-Tree implementation and SQL parsing strategy.

## License

This project is released under the [MIT License](LICENSE).
