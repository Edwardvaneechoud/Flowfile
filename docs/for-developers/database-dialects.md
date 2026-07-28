# Database Dialects

How Flowfile supports multiple SQL databases through one contract, and how to add a new one.

## Why a dialect contract

Before the contract existed, the database vocabulary was duplicated in six disagreeing
places (a core Pydantic `Literal`, a wider frame-API `Literal`, the worker's port map, a
postgres-family set in `shared`, and three hard-coded frontend dropdowns), and adding MySQL
took a 39-file change. Today a single registry in **`shared/db_dialects/`** owns the
vocabulary and the per-dialect behavior; every other layer consumes it:

- **flowfile_core** validates `database_type` against the registry, routes reads/writes and
  schema prediction through it, and serves the catalog at `GET /db_dialects`.
- **flowfile_worker** uses it for the TCP pre-flight (`file_based`, `default_port`) and the
  read strategy.
- **The frontend** renders its dialect dropdowns from `GET /db_dialects` — a new dialect
  appears in the UI with **zero frontend changes**.
- **flowfile_frame** validates `create_database_connection(database_type=...)` against it.

## The `DbDialect` surface

A dialect is one class in `shared/db_dialects/` registered in `_BUILTIN_DIALECTS`
(`shared/db_dialects/__init__.py`). The base class **is** the generic dialect — its method
bodies are the historical postgres-shaped code paths, so a dialect with no overrides
changes nothing.

| Member | Purpose |
|---|---|
| `name`, `display_name` | Registry key (lowercase) and UI label |
| `file_based` | Drives the file-path UI, the password skip, and the worker pre-flight skip |
| `default_port`, `supports_ssl` | Catalog metadata for the connection form |
| `sqlalchemy_driver` | Driver suffix for SQLAlchemy URIs (e.g. `mysql+pymysql`) |
| `sqlglot_name` | Dialect name for SQL parsing in the contract tests |
| `is_available()` / `install_hint` | Driver import probe + pip hint when missing |
| `build_uri(...)` | Base (connectorx-style) URI construction |
| `read(query, uri, logger, cancel_check)` | Read strategy. Base = the hedged connectorx/SQLAlchemy race in `shared/db_reader.py` |
| `write(df, uri, table_name, if_exists)` | Write strategy. Base = SQLAlchemy Core inserts in `shared/db_writer.py` |
| `limit_query(sql, n)` | Row-limit clause (`LIMIT n` base; SQL Server will override with `TOP`) |
| `table_schema` / `query_schema` | **Fast-schema hooks**: return a real `pl.Schema` without reading rows, or `None` to keep the caller's generic SQLAlchemy-inspection path |
| `list_schemas` / `list_tables` | Browse hooks for `/db_schemas` and `/db_tables`, `None` = generic inspector |

**The fast-schema requirement.** Every connector must be able to predict its output schema
without loading data — that is what keeps downstream schema prediction instant in the
Designer. DuckDB is the reference implementation: it wraps any query in
`SELECT * FROM (...) LIMIT 0`, which DuckDB plans without executing, returning real column
types even in query mode (where generic dialects fall back to an all-String sample probe).

**Secrets never enter `shared/`.** Dialect methods receive plain, already-decrypted
strings; `$ffsec$` decryption happens in core/worker callers, as everywhere else.

**Security note.** User SQL always passes `shared/sql_validation.py` first. Its
table-function rejection (`read_csv(...)`, `read_parquet(...)`) is load-bearing for
DuckDB: it blocks arbitrary file reads through user queries.

## Adding a connector (checklist)

For a dialect that fits the current connection model (host/port/database/user/password/SSL
or a file path):

1. `shared/db_dialects/<name>.py` — a `DbDialect` subclass (copy `duckdb.py` for
   file-based or native-driver dialects; server dialects that connectorx supports often
   need only metadata + a `limit_query`/driver override).
2. Register it in `_BUILTIN_DIALECTS` (`shared/db_dialects/__init__.py`).
3. Add the driver dependency to the root `pyproject.toml` (decide main vs extra; the
   desktop and Docker builds must ship complete).
4. Tests: `shared/tests/db_dialects/test_<name>_dialect.py` plus the contract suite runs
   automatically for every registered dialect (URI round-trip, limit-query parseability,
   if_exists semantics and prediction parity for file-based dialects).
5. Docs: a row in the connections page table, a mention in reading/writing-data, and a
   tested example under `docs/examples/integrations/` when feasible.
6. Verify the desktop build still bundles (`make services && make test_built_services`) —
   native drivers occasionally need PyInstaller attention (see `connectorx_hook.py`).

**When you still must touch core:** a connector whose *connection shape* is new — e.g.
Snowflake's account/warehouse/role or BigQuery's service-account key — additionally needs
connection-model fields (`input_schema.py` + an Alembic migration for an `extra_params`
column), frontend form fields, and secret-handling decisions. The contract shrinks a
connector from "touch six packages" to "one spec module + tests", but it cannot hide a new
credential shape.

## Roadmap

### Wave 2 — SQL Server

- connectorx supports mssql, so the base `read()` (hedged connectorx/SQLAlchemy) works
  as-is; overrides needed: `limit_query` (`SELECT TOP n`), `sqlglot_name="tsql"`,
  `sqlalchemy_driver` (pyodbc vs pymssql decision — pymssql avoids the system ODBC
  dependency), `default_port=1433`.
- Docker fixture `test_utils/mssql/` following the mysql fixture pattern
  (`mcr.microsoft.com/mssql/server`, Linux CI only).
- The SQL-type→Polars map in core already contains the MSSQL type names.

### Wave 3 — Snowflake

- No connectorx support: native `snowflake-connector-python` Arrow fetch as the `read()`
  override; `LIMIT 0`/`DESCRIBE` for fast schema.
- Needs the connection-model extension described above (account/warehouse/role via a
  guarded `extra_params` column — mirror the Kafka blocked-config guard in
  `shared/kafka/models.py` so extra params can never override auth) — the first Alembic
  migration of this campaign.
- Packaging: likely a `flowfile[snowflake]` extra for pip, installed unconditionally in
  desktop/Docker builds (maintainer decision at implementation time).
- Testing: [fakesnow](https://github.com/tekumara/fakesnow) in CI plus live tests gated on
  credentials (`skipif` unless a Snowflake test account is configured), per the maintainer's
  recorded decision.

### Wave 4 — BigQuery

- New credential shape (service-account JSON) and project/dataset instead of host/port.
- Reads via the BigQuery Storage API (Arrow); writes via load jobs (`if_exists` maps to
  `WRITE_APPEND`/`WRITE_TRUNCATE`/`WRITE_EMPTY`).
- Testing against `goccy/bigquery-emulator`, following the fake-gcs fixture pattern.

## Guardrails

- `shared/tests/db_dialects/test_dialect_contract.py` runs every registered dialect
  through registry sanity, URI round-trips, sqlglot-parseable limit queries, catalog
  serialization, and (for file-based dialects) a full write/read/predict parity leg.
- `flowfile_core/tests/flowfile/external_sources/test_dialect_vocabulary.py` pins the
  core model validation to `KNOWN_DIALECT_NAMES`.
- Legacy stored connections with pre-registry type strings (e.g. `redshift`) keep working
  through `get_dialect_or_generic` (URI behavior preserved verbatim) and stay updatable —
  only *switching* a connection to an unknown type is rejected.
