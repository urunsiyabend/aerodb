<p align="center">
<img src="images/logo.png" width="200px" height="200px" alt="AeroDB">
</p>

AeroDB is a lightweight, portable relational database engine designed for efficiency and simplicity. It utilizes a B-tree-based architecture to manage and organize data, ensuring fast and reliable access. All data— including tables, indexes, and metadata— is stored compactly within a single file, making AeroDB highly suitable for embedded applications, standalone deployments, and scenarios where minimal configuration and footprint are critical. Its streamlined design supports easy integration while maintaining the core functionalities expected from a relational database management system.

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

## Development Approach

Development follows a **Test-Driven Development (TDD)** workflow:

1. **Write failing tests** for new features or bug fixes.
2. **Implement** the minimal code to make those tests pass.
3. **Run the full test suite** to ensure all tests succeed.
4. **Refactor** for readability and maintainability while keeping tests green.
5. Add additional tests for uncovered edge cases.

## TODO

These tasks outline upcoming work and reference articles we plan to publish.

- [ ] **Expand SQL parser** to support JOINs, nested queries and basic functions.
- [ ] **Flesh out transactions** with ACID semantics and accompanying tests.
- [ ] **Document storage engine** internals and page layout in a dedicated article.
- [x] **Add secondary indexes** to accelerate lookups on non-primary keys.
- [ ] **Implement concurrency control** (locking or MVCC) with tests.
- [ ] **Write tutorial articles** detailing the B-Tree implementation and SQL parsing strategy.

