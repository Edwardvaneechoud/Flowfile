"""Position mapping between CodeMirror cell coordinates and Jedi coordinates.

v1 analyzes the current cell only (executed prior cells already live in the
namespace), so the mapping is near-identity — but we still clamp positions so an
out-of-range request can never make Jedi raise. CodeMirror gives a 1-based line
and a 0-based column within that line; Jedi uses 1-based line / 0-based column,
so lines and columns map straight through after clamping.

Kept as a separate, dependency-free module so the mapping can be unit-tested in
isolation (it's the one piece with off-by-one risk).
"""


def _lines(code: str) -> list[str]:
    return code.split("\n")


def to_jedi_position(code: str, line: int, column: int) -> tuple[int, int]:
    """Clamp a CodeMirror (1-based line, 0-based column) into a valid Jedi position.

    Returns a ``(line, column)`` guaranteed to sit inside ``code`` so Jedi's
    ``complete``/``help``/``get_signatures`` never raise on a stray coordinate.
    """
    rows = _lines(code)
    if not rows:
        return 1, 0
    line = max(1, min(line, len(rows)))
    line_text = rows[line - 1]
    column = max(0, min(column, len(line_text)))
    return line, column


def from_jedi_position(line: int, column: int) -> tuple[int, int]:
    """Inverse of :func:`to_jedi_position` for v1 single-cell analysis (identity).

    Exists so diagnostic ranges (P2) map back through one named seam rather than
    open-coding the identity at every call site.
    """
    return line, column


def clamp_end_position(code: str, line: int, column: int) -> tuple[int, int]:
    """Clamp an end position (used for diagnostic ranges) to within ``code``."""
    return to_jedi_position(code, line, column)
