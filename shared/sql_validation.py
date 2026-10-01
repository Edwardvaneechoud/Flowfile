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
_TABLE_FUNCTION_NAMES = (
    r"read_csv|read_parquet|read_ipc|read_json|read_ndjson|read_avro|"
    r"read_database|read_delta|scan_csv|scan_parquet|scan_ipc|scan_ndjson|"
    r"scan_delta|scan_iceberg"
)
_TABLE_FUNCTION_RE = re.compile(rf"\b({_TABLE_FUNCTION_NAMES})\s*\(", re.IGNORECASE)
_TABLE_FUNCTION_NAME_RE = re.compile(_TABLE_FUNCTION_NAMES, re.IGNORECASE)
# polars takes a qualified name's first part as the function: read_csv.x(...) reads.
_TABLE_FUNCTION_CALL_RE = re.compile(rf"({_TABLE_FUNCTION_NAMES})\s*[(.]", re.IGNORECASE)
_SQL_TOKEN = re.compile(r"""--[^\n]*|/\*|'(?:[^']|'')*'|"(?:[^"]|"")*"|`(?:[^`]|``)*`|[^-/'"`]+|.""", re.DOTALL)
_COMMENT_MARKER = re.compile(r"/\*|\*/")

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


def uses_table_function(query: str) -> bool:
    """Whether ``query`` uses a SQL table function; it fails closed, since answering no lets polars read.

    True when the gate :func:`validate_sql_query` applies rejects it, when a known name is followed by
    ``(`` or ``.`` in :func:`_sql_code`'s reading of it, or when it names one and :func:`_sql_code`
    cannot read it. A ``read_csv(`` in a comment or a CTE named ``scan_results(a)`` is not one.
    """
    if _TABLE_FUNCTION_NAME_RE.search(query):
        code = _sql_code(query)
        if code is None or _TABLE_FUNCTION_CALL_RE.search(code):
            return True
    try:
        _reject_table_functions(query)
    except UnsafeSQLError:
        return True
    return False


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

    match = _TABLE_FUNCTION_RE.search(stripped) or _TABLE_FUNCTION_RE.search(_sql_code(query) or "")
    if match:
        raise UnsafeSQLError(_TABLE_FUNC_MSG.format(name=match.group(1).lower()))


def _sql_code(query: str) -> str | None:
    """``query`` as polars' SQL lexer reads it: comments (nested ones too) blanked, quote characters dropped.

    ``None`` when this cannot be told: an unclosed quote or comment, or a ``$`` or backslash (dollar
    quoting and escaped strings, which it does not model).
    """
    if "$" in query or "\\" in query:
        return None
    code, i = [], 0
    while i < len(query):
        token = _SQL_TOKEN.match(query, i).group()
        if token == "/*":
            depth = 0
            for marker in _COMMENT_MARKER.finditer(query, i):
                depth += 1 if marker.group() == "/*" else -1
                if depth == 0:
                    break
            else:
                return None
            code.append(" ")
            i = marker.end()
            continue
        if token in ("'", '"', "`"):
            return None
        i += len(token)
        if token.startswith("--"):
            code.append(" ")
        else:
            code.append(token[1:-1] if token[0] in "'\"`" else token)
    return "".join(code)


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
    - Multi-line comments (/* comment */), unnested; an unclosed ``/*`` and the text after it are kept

    The block-comment scan is linear: once an opener has no closer, no later opener has one either.
    """
    parts, i = [], 0
    while (start := query.find("/*", i)) >= 0 and (end := query.find("*/", start + 2)) >= 0:
        parts.append(query[i:start] + " ")
        i = end + 2
    result = "".join(parts) + query[i:]
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
