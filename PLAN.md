# DataFusion Integration Plan — Suggestions

## Context

readstat-rs already produces Arrow `RecordBatch` objects as its core data representation. DataFusion 52.x uses Arrow v57, which is the exact same version readstat-rs uses. This means integration requires **zero data conversion** — the existing `RecordBatch` from the parsing pipeline can be handed directly to `MemTable::try_new()`.

### New Dependencies Required (All Approaches)

```toml
datafusion = "52"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

**Trade-off**: DataFusion is a substantial dependency (~200+ transitive crates). It will increase compile times and binary size significantly. Consider making it a feature flag (`--features sql`) so users who don't need SQL don't pay the cost.

### Runtime Change

readstat-rs is currently fully synchronous (Rayon + Crossbeam channels). DataFusion requires Tokio async. The simplest integration wraps DataFusion calls in a `tokio::runtime::Runtime::new().block_on(...)` block, keeping the rest of the codebase sync. No need to make `main` async.

---

## Suggestion A: `--sql` Flag on Existing `data` Subcommand

Add a `--sql <QUERY>` option to the `data` subcommand. The table is automatically registered as `data` (or as the file stem, e.g. `cars`).

### CLI Usage

```bash
# Select specific columns with a WHERE filter
readstat data cars.sas7bdat -o filtered.parquet -f parquet \
  --sql "SELECT make, model, mpg FROM data WHERE mpg > 30"

# Aggregation
readstat data cars.sas7bdat -o summary.csv \
  --sql "SELECT make, COUNT(*) as cnt, AVG(mpg) as avg_mpg FROM data GROUP BY make"

# No --sql flag = current behavior (no DataFusion involved)
readstat data cars.sas7bdat -o out.csv
```

### How It Works

1. Read SAS file into `Vec<RecordBatch>` (existing pipeline, all chunks)
2. Register as MemTable in DataFusion SessionContext
3. Execute SQL query → get result `Vec<RecordBatch>`
4. Write result batches to output file using existing writers

### Pros

- Minimal CLI surface change — one new flag
- Backwards compatible — without `--sql`, behavior is unchanged
- Users already understand the `data` subcommand
- Subsumes `--columns` (use SELECT) and `--rows` (use LIMIT) naturally
- Full SQL power: WHERE, GROUP BY, ORDER BY, JOIN (if multiple files registered), window functions, etc.

### Cons

- SQL strings on the command line can be awkward (quoting, escaping)
- Long queries are hard to read inline
- Must read entire file into memory before querying (no streaming optimization for filtered reads)

### Interaction with Existing Flags

- `--columns` and `--sql` should be mutually exclusive (SQL SELECT replaces column filtering)
- `--rows` could still be used as a pre-filter (read only N rows from SAS before SQL), or be mutually exclusive with `--sql` (use SQL LIMIT instead)
- `--format` and `--output` work unchanged on the SQL result

---

## Suggestion B: `--sql-file <PATH>` Flag (Companion to A)

Same as Suggestion A but reads the SQL from a file. This naturally pairs with `--sql` — offer both.

### CLI Usage

```bash
# query.sql contains: SELECT make, model FROM data WHERE mpg > 30
readstat data cars.sas7bdat -o filtered.csv --sql-file query.sql
```

### Pros

- Handles complex, multi-line queries cleanly
- Queries become version-controllable artifacts
- Avoids shell quoting issues
- Can contain comments for documentation

### Cons

- Extra file to manage
- For simple queries, more friction than inline

### Recommendation

Offer both `--sql` and `--sql-file` as mutually exclusive options (similar to `--columns` / `--columns-file` pattern that already exists).

---

## Suggestion C: New `query` Subcommand

A dedicated subcommand focused on SQL queries, separate from `data`.

### CLI Usage

```bash
# Basic query
readstat query cars.sas7bdat --sql "SELECT * FROM cars WHERE mpg > 30"

# Query with file output
readstat query cars.sas7bdat -o result.parquet -f parquet \
  --sql "SELECT make, AVG(mpg) FROM cars GROUP BY make"

# Query from file
readstat query cars.sas7bdat --sql-file analysis.sql -o result.csv

# Multi-table query (register multiple files)
readstat query cars.sas7bdat prices.sas7bdat \
  --sql "SELECT c.make, p.price FROM cars c JOIN prices p ON c.make = p.make"

# Default output to stdout as CSV (like preview)
readstat query cars.sas7bdat --sql "SELECT make, mpg FROM cars LIMIT 5"
```

### How It Works

1. Accept one or more input SAS files
2. Register each as a MemTable (table name = file stem)
3. Execute SQL
4. Output to file or stdout

### Pros

- Clean separation of concerns — `data` is for conversion, `query` is for analysis
- Can accept multiple input files for JOINs
- Doesn't complicate the existing `data` subcommand
- Can default to stdout (like `preview`) for quick exploration

### Cons

- New subcommand adds CLI surface area
- Some overlap with `data` (both can select columns and filter)
- Users must learn which subcommand to use

---

## Suggestion D: `--where` Filter Flag (Lightweight, No Full SQL)

Instead of full SQL, add a `--where` flag that accepts a SQL WHERE clause expression. DataFusion can parse and evaluate just the expression.

### CLI Usage

```bash
# Filter rows
readstat data cars.sas7bdat -o filtered.csv --where "mpg > 30 AND make = 'Toyota'"

# Combine with existing column selection
readstat data cars.sas7bdat -o filtered.csv --columns make,model,mpg --where "mpg > 30"
```

### How It Works

1. Read SAS file into RecordBatches
2. Register in DataFusion
3. Construct query: `SELECT {columns or *} FROM data WHERE {expression}`
4. Execute and write results

### Pros

- Simpler mental model — just row filtering
- Composes naturally with existing `--columns` flag
- Less intimidating for users who don't know SQL
- Shorter command lines

### Cons

- No aggregation (GROUP BY), no ordering (ORDER BY), no joins
- Still requires DataFusion as a dependency for expression evaluation
- Users who want full SQL will outgrow this quickly
- Implementing a restricted interface on top of a full SQL engine feels wasteful

---

## Suggestion E: Interactive REPL Mode

Add a `repl` or `shell` subcommand that drops the user into an interactive SQL session.

### CLI Usage

```bash
readstat shell cars.sas7bdat
# > Loaded table "cars" (428 rows, 15 columns)
# sql> SELECT make, COUNT(*) FROM cars GROUP BY make ORDER BY 2 DESC LIMIT 5;
# +--------+-------+
# | make   | count |
# +--------+-------+
# | Toyota |    28 |
# | ...    |   ... |
# sql> .quit
```

### Pros

- Exploratory analysis without re-reading the file for each query
- Familiar to users of sqlite3, psql, datafusion-cli
- Great for data discovery and ad-hoc analysis

### Cons

- Significant implementation effort (line editing, history, display formatting)
- Scope creep — readstat-rs is a conversion tool, not an analysis tool
- Could use `datafusion-cli` or `datafusion-dft` externally instead
- Adds `rustyline` or similar dependency

---

## Comparison Matrix

| Criteria                    | A: --sql | B: --sql-file | C: query subcmd | D: --where | E: REPL |
|-----------------------------|----------|---------------|-----------------|------------|---------|
| Implementation effort       | Low      | Low           | Medium          | Low        | High    |
| Full SQL power              | Yes      | Yes           | Yes             | No         | Yes     |
| Multi-table JOINs           | No*      | No*           | Yes             | No         | Yes     |
| Backwards compatible        | Yes      | Yes           | Yes             | Yes        | Yes     |
| Shell quoting issues        | Yes      | No            | Yes (--sql)     | Yes        | No      |
| Composable with --columns   | No**     | No**          | N/A             | Yes        | N/A     |
| Exploratory use             | Poor     | Poor          | Poor            | Poor       | Great   |
| Scope creep risk            | Low      | Low           | Medium          | Low        | High    |
| New CLI surface area        | Minimal  | Minimal       | Moderate        | Minimal    | Moderate|

\* Could be extended to accept multiple inputs later
\** `--sql` SELECT replaces `--columns`

---

## My Recommendation

**Start with A + B together** (the `--sql` and `--sql-file` flags on the `data` subcommand). Here's why:

1. **Lowest implementation effort** — ~200-300 lines of new code
2. **Zero breaking changes** — existing CLI works identically without the new flags
3. **Full SQL power** from day one — WHERE, GROUP BY, ORDER BY, LIMIT, aggregations, window functions, CASE expressions
4. **Natural extension of existing patterns** — mirrors `--columns` / `--columns-file`
5. **Feature-gate the dependency** — `datafusion` and `tokio` behind a `sql` feature flag so default builds stay lean

If the feature proves valuable, you can later:
- Add a `query` subcommand (Suggestion C) for multi-file JOINs
- Add a `--where` shorthand (Suggestion D) for simple filtering
- Add a REPL (Suggestion E) for exploration

### Proposed Implementation Steps (if A+B chosen)

1. Add `datafusion` and `tokio` as optional dependencies behind `sql` feature
2. Add `--sql` and `--sql-file` arguments to the `data` subcommand (mutually exclusive with each other, and with `--columns`/`--columns-file`)
3. Create a `rs_query.rs` module that:
   - Takes `Vec<RecordBatch>` + schema + SQL string
   - Creates SessionContext, registers MemTable as table named after the input file stem
   - Executes SQL, returns `Vec<RecordBatch>`
4. In the data pipeline, after reading all batches, if `--sql` is present: run through DataFusion before writing
5. Add integration tests with the existing test SAS files
6. Also add `--sql` and `--sql-file` to the `preview` subcommand for consistency
