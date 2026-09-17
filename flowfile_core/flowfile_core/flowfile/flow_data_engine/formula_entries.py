"""The ordered entries a formula node evaluates, and the error that attributes a failure to one.

Kept in its own module so `flow_graph` can build entries without importing the (very large)
`flow_data_engine` module for the types alone.
"""

import re
from dataclasses import dataclass
from typing import Literal

import polars as pl

FormulaErrorKind = Literal["config", "parse", "missing_column", "type"]

_MISSING_COLUMN_RE = re.compile(r'unable to find column "(.*?)"')


@dataclass(frozen=True)
class FormulaEntry:
    """One formula-node row: where it sits, what column it writes, and how."""

    position: int
    """1-based row number as the user sees it; blank rows are counted but never evaluated."""

    output_name: str
    expression: str
    output_data_type: pl.DataType | None = None


class FormulaEntryError(Exception):
    """A formula entry that could not be built, attributed to its row and output column.

    `kind` distinguishes the causes a static check can tell apart: `"config"` (blank output
    name), `"parse"` (the expression does not parse), `"missing_column"` (it references a
    column the frame does not have at that point in the chain) and `"type"` (any other Polars
    error raised while resolving the step's schema).
    """

    def __init__(self, position: int, output_name: str, detail: str, kind: FormulaErrorKind):
        self.position = position
        self.output_name = output_name
        self.detail = detail
        self.kind = kind
        label = f'Formula {position} ("{output_name}")' if output_name else f"Formula {position}"
        super().__init__(f"{label}: {detail}")


def first_line(message: str) -> str:
    """The first non-empty line of an exception message, so an error stays a single line."""
    for line in str(message).splitlines():
        if line.strip():
            return line.strip()
    return str(message).strip()


def missing_column_detail(message: str) -> str:
    """Name the column Polars could not find, falling back to its own first line."""
    match = _MISSING_COLUMN_RE.search(str(message))
    return f"column '{match.group(1)}' not found" if match else first_line(message)
