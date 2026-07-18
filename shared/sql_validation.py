"""Shared read-only SQL validation for user-supplied queries.

Used by both flowfile_core and flowfile_worker so the two services enforce the
*same* gate — the worker must not trust core (any future or compromised caller
of the worker's SQL endpoint would otherwise bypass validation entirely).

The gate rejects anything that is not a single read-only SELECT/WITH statement
and rejects SQL table-valued functions (``read_csv``/``read_parquet``/...),
which ``pl.SQLContext`` would otherwise execute to read server files and issue
outbound requests.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


class UnsafeSQLError(ValueError):
    """Raised when a SQL query contains unsafe operations."""

    pass


# Literal reader/scanner table functions — the fallback denylist used only when
# the parser below cannot build an AST. The AST walk is the primary, allowlist-
# style gate (it rejects *any* function used as a table source, not just these).
_TABLE_FUNCTION_RE = re.compile(
    r"\b(read_csv|read_parquet|read_ipc|read_json|read_ndjson|read_avro|"
    r"read_database|read_delta|scan_csv|scan_parquet|scan_ipc|scan_ndjson|"
    r"scan_delta|scan_iceberg)\s*\(",
    re.IGNORECASE,
)

_TABLE_FUNC_MSG = (
    "SQL table functions are not allowed (found '{name}(...)'). "
    "Reference connected inputs by name (input_1, input_2, ...)."
)


def validate_sql_query(query: str) -> None:
    """
    Validate that a SQL query is safe for execution (read-only SELECT statements only).

    This function checks that the query:
    1. Is a SELECT statement (not INSERT, UPDATE, DELETE, etc.)
    2. Does not contain DDL statements (DROP, CREATE, ALTER, TRUNCATE)
    3. Does not contain other dangerous operations
    4. Does not use SQL table functions (read_csv, read_parquet, ...) that would
       read server files or make outbound requests

    Args:
        query: The SQL query string to validate

    Raises:
        UnsafeSQLError: If the query contains unsafe operations
    """
    if not query or not query.strip():
        raise UnsafeSQLError("SQL query cannot be empty")

    normalized = _remove_sql_comments(query)
    normalized = " ".join(normalized.split()).upper()

    if not _is_select_query(normalized):
        raise UnsafeSQLError(
            "Only SELECT queries are allowed. "
            "The query must start with SELECT or WITH (for common table expressions)."
        )

    # Check for dangerous DDL statements
    ddl_patterns = [
        (r"\bDROP\s+", "DROP statements are not allowed"),
        (r"\bCREATE\s+", "CREATE statements are not allowed"),
        (r"\bALTER\s+", "ALTER statements are not allowed"),
        (r"\bTRUNCATE\s+", "TRUNCATE statements are not allowed"),
        (r"\bRENAME\s+", "RENAME statements are not allowed"),
    ]

    for pattern, error_msg in ddl_patterns:
        if re.search(pattern, normalized):
            raise UnsafeSQLError(error_msg)

    # Check for dangerous DML statements (these shouldn't appear in a SELECT)
    dml_patterns = [
        (r"\bINSERT\s+INTO\b", "INSERT statements are not allowed"),
        (r"\bUPDATE\s+\w+\s+SET\b", "UPDATE statements are not allowed"),
        (r"\bDELETE\s+FROM\b", "DELETE statements are not allowed"),
    ]

    for pattern, error_msg in dml_patterns:
        if re.search(pattern, normalized):
            raise UnsafeSQLError(error_msg)

    # Check for dangerous operations that could be used maliciously
    dangerous_patterns = [
        (r"\bEXEC(UTE)?\s*\(", "EXECUTE statements are not allowed"),
        (r"\bCALL\s+", "CALL statements (stored procedures) are not allowed"),
        (r"\bGRANT\s+", "GRANT statements are not allowed"),
        (r"\bREVOKE\s+", "REVOKE statements are not allowed"),
        (r"\bCOPY\b", "COPY statements are not allowed"),
        (r"\bMERGE\s+", "MERGE statements are not allowed"),
        (r"\bINTO\s+OUTFILE\b", "INTO OUTFILE is not allowed"),
        (r"\bLOAD\s+DATA\b", "LOAD DATA statements are not allowed"),
        (r"\bPRAGMA\b", "PRAGMA statements are not allowed"),
        (r"\bATTACH\b", "ATTACH statements are not allowed"),
        (r"\bDETACH\b", "DETACH statements are not allowed"),
    ]

    for pattern, error_msg in dangerous_patterns:
        if re.search(pattern, normalized):
            raise UnsafeSQLError(error_msg)

    # Reject SQL table functions (read_csv / read_parquet / ...): they would let
    # user SQL read files and make outbound requests through pl.SQLContext.
    _reject_table_functions(query)


def _reject_table_functions(query: str) -> None:
    """Reject any SQL table-valued function used as a table source.

    Registered inputs are referenced by plain table name on every Flowfile SQL
    surface, so no legitimate query needs a function in a FROM/JOIN clause.
    The parser-based walk is the primary gate (rejects *any* function as a table
    source, so new reader functions are covered automatically); the regex is a
    fallback for the rare query the parser cannot build an AST for.
    """
    stripped = _remove_sql_comments(query)

    try:
        import sqlglot
        from sqlglot import exp

        for stmt in sqlglot.parse(stripped, error_level=sqlglot.ErrorLevel.IGNORE):
            if stmt is None:
                continue
            for table in stmt.find_all(exp.Table):
                inner = table.this
                if isinstance(inner, exp.Func):
                    raise UnsafeSQLError(_TABLE_FUNC_MSG.format(name=_func_name(inner)))
    except UnsafeSQLError:
        raise
    except Exception as exc:  # pragma: no cover - parser degradation is rare
        logger.debug("sqlglot table-function check degraded, using regex fallback: %s", exc)

    match = _TABLE_FUNCTION_RE.search(stripped)
    if match:
        raise UnsafeSQLError(_TABLE_FUNC_MSG.format(name=match.group(1).lower()))


def _func_name(func) -> str:
    """Best-effort SQL function name for an sqlglot Func/Anonymous node.

    Known reader nodes (ReadCSV/ReadParquet/...) expose the canonical name via
    ``sql_name()``; an Anonymous function keeps its name in ``this`` (``.name``
    returns the first *argument* for these, not the function).
    """
    from sqlglot import exp

    if isinstance(func, exp.Anonymous):
        name = str(func.this or "")
    else:
        try:
            name = func.sql_name() or ""
        except Exception:
            name = ""
    return (name or type(func).__name__).lower()


def _remove_sql_comments(query: str) -> str:
    """
    Remove SQL comments from a query string.

    Handles:
    - Single line comments (-- comment)
    - Multi-line comments (/* comment */)
    """
    # Remove multi-line comments using a non-backtracking pattern
    # Matches /* followed by (non-* chars OR * not followed by /) then */
    result = re.sub(r"/\*(?:[^*]|\*(?!/))*\*/", " ", query)
    # Remove single-line comments - explicitly match non-newline chars to avoid backtracking
    result = re.sub(r"--[^\r\n]*", " ", result)
    return result


def _is_select_query(normalized_query: str) -> bool:
    """
    Check if a normalized (uppercase, whitespace-cleaned) query is a SELECT statement.

    Allows:
    - SELECT ...
    - WITH ... SELECT ... (CTEs)
    """
    if normalized_query.startswith("SELECT ") or normalized_query.startswith("SELECT\t"):
        return True

    # Check for WITH clause (CTE) that leads to SELECT
    if normalized_query.startswith("WITH ") or normalized_query.startswith("WITH\t"):
        # CTEs should eventually have a SELECT
        # Make sure there's a SELECT after the WITH clause and no dangerous statements
        if " SELECT " in normalized_query or "\tSELECT " in normalized_query:
            return True

    return False
