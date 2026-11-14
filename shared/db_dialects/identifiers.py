"""The identifier shapes the database nodes accept, written once for the settings models and the dialects.

``PLAIN_PATH`` is the historical rule every dialect keeps: dotted parts of letters, digits and underscores, not
starting with a digit. ``IDENTIFIER_PATH`` is the wider shape a dialect with ``quoted_identifiers`` takes (and the
superset the settings models accept, since a node names a connection, not a type): each part a plain identifier that
may also hold ``$``, or a double-quoted one whose inner quotes are doubled, so it cannot end early; parts are joined
by dots outside quotes. Nothing else passes either shape: no spaces, semicolons or comment markers outside quotes and
no unbalanced quotes.
"""

from __future__ import annotations

import re

PLAIN_PART = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
IDENTIFIER_PART = r'(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")+")'
IDENTIFIER_PATH = re.compile(rf"^{IDENTIFIER_PART}(?:\.{IDENTIFIER_PART})*$")
IDENTIFIER = re.compile(rf"^{IDENTIFIER_PART}$")
_PART = re.compile(IDENTIFIER_PART)


def is_plain_path(value: str) -> bool:
    """Every dot-separated part letters, digits and underscores, not starting with a digit."""
    return bool(value) and all(PLAIN_PART.match(part) for part in value.split("."))


def is_identifier_path(value: str) -> bool:
    """Dotted parts, each plain (``$`` allowed) or well-formed double-quoted."""
    return bool(IDENTIFIER_PATH.match(value or ""))


def identifier_parts(value: str) -> list[str]:
    """The parts of a path ``is_identifier_path`` accepts, split on the dots outside quotes; quotes are kept."""
    if not is_identifier_path(value):
        raise ValueError(f"'{value}' is not a dotted identifier path.")
    return _PART.findall(value)
