"""``fl.sql``: a SQL Query node over any number of frames, returned as its output frame."""

from __future__ import annotations

import re
import textwrap
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from flowfile_frame.native import NativeNodeError, Node

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame

_TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_INPUT_SLOT = re.compile(r"input_(\d+)")
_WITH_KEYWORD = re.compile(r"with\b", re.IGNORECASE)
_DESCRIPTION_LIMIT = 80


def _skip_comment(query: str, i: int) -> int:
    """The index after the SQL comment starting at ``i``, or ``i`` when none starts there."""
    if query.startswith("--", i):
        end = query.find("\n", i)
        return len(query) if end < 0 else end + 1
    if query.startswith("/*", i):
        end = query.find("*/", i + 2)
        return len(query) if end < 0 else end + 2
    return i


def _leading_with(query: str) -> re.Match[str] | None:
    """The query's own leading ``WITH`` keyword, found after whitespace and whole comments."""
    i = 0
    while i < len(query):
        if query[i].isspace():
            i += 1
            continue
        after = _skip_comment(query, i)
        if after == i:
            break
        i = after
    return _WITH_KEYWORD.match(query, i)


def _quoted_text_spans_lines(query: str) -> bool:
    """Whether a quoted literal or identifier runs across a line break, or is never closed."""
    i = 0
    while i < len(query):
        after = _skip_comment(query, i)
        if after != i:
            i = after
        elif query[i] in "'\"":
            end = query.find(query[i], i + 1)
            if end < 0 or "\n" in query[i:end]:
                return True
            i = end + 1
        else:
            i += 1
    return False


def _tidy(query: str) -> str:
    """``query`` stripped and, unless that would edit a multi-line quoted text, dedented."""
    if not _quoted_text_spans_lines(query):
        query = textwrap.dedent(query)
    return query.strip()


def _check_table_name(name: str, slot: int) -> None:
    """Refuse a name that is no plain SQL identifier or that is another input's ``input_<n>``."""
    if not isinstance(name, str) or not _TABLE_NAME.fullmatch(name):
        raise NativeNodeError(f"SQL table name {name!r} must be a plain identifier (letters, digits, underscores)")
    other = _INPUT_SLOT.fullmatch(name)
    if other and int(other.group(1)) != slot:
        raise NativeNodeError(
            f"SQL table name {name!r} would shadow another input: this frame is input_{slot}; pick another name"
        )


def _with_table_names(query: str, aliases: Mapping[int, str]) -> str:
    """``query`` behind a ``WITH <name> AS (SELECT * FROM input_<slot>)`` entry per alias.

    The SQL Query node registers its inputs as ``input_1``, ``input_2``, ...; the header maps
    each chosen name onto its slot and is merged into the query's own leading ``WITH``. A name
    that already is its own slot's ``input_<slot>`` needs no entry; table names are
    case-sensitive, so ``Input_1`` does.
    """
    entries = [f"{name} AS (SELECT * FROM input_{slot})" for slot, name in aliases.items() if name != f"input_{slot}"]
    if not entries:
        return query
    header = "WITH " + ",\n     ".join(entries)
    own_with = _leading_with(query)
    if own_with:
        return f"{header},\n{query[: own_with.start()]}{query[own_with.end() :].lstrip()}"
    return f"{header}\n{query}"


def _default_description(query: str) -> str:
    """The query's first non-empty line, cut like the canvas default so the generated header never labels the node."""
    first_line = next((line.strip() for line in query.splitlines() if line.strip()), "")
    if len(first_line) > _DESCRIPTION_LIMIT:
        return first_line[: _DESCRIPTION_LIMIT - 3] + "..."
    return first_line


def _sql_frame(
    query: str, positional: Sequence[FlowFrame], named: Mapping[str, FlowFrame], description: str | None
) -> FlowFrame:
    """Place the SQL Query node: positional frames are ``input_1..k``, named ones follow and get an alias."""
    from flowfile_frame.flow_frame import FlowFrame

    if not isinstance(query, str) or not query.strip():
        raise NativeNodeError("SQL query is empty")
    frames = [*positional, *named.values()]
    for frame in frames:
        if not isinstance(frame, FlowFrame):
            raise NativeNodeError(
                f"fl.sql takes FlowFrames, got {type(frame).__name__}; wrap a Polars frame with fl.FlowFrame(...)"
            )
    aliases = dict(enumerate(named, start=len(positional) + 1))
    for slot, name in aliases.items():
        _check_table_name(name, slot)

    query = _tidy(query)
    node = Node(
        "sql_query",
        *frames,
        settings={"sql_query_input": {"sql_code": _with_table_names(query, aliases)}},
        description=description if description is not None else _default_description(query),
    )
    return node.output


def sql(query: str, /, *frames: FlowFrame, description: str | None = None, **tables: FlowFrame) -> FlowFrame:
    """Run ``query`` over the given frames as a SQL Query node and return its output frame.

    Positional frames are the tables ``input_1``, ``input_2``, ...; a frame passed by keyword
    is the table of that name (and also ``input_<n>``, numbered after the positional ones).
    The names are stored in the node's SQL as a ``WITH <name> AS (SELECT * FROM input_<n>)``
    header, which the designer shows as-is. Without frames the query reads no tables and the
    node starts a new graph. The query uses the Polars SQL dialect and must be a single
    ``SELECT`` or ``WITH`` statement; flow parameters go in as ``${name}`` (``Parameter.ref``).
    Frames on different graphs are merged onto one.

    Example::

        fl.sql("SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
               orders=orders, customers=customers)
    """
    return _sql_frame(query, frames, tables, description)
