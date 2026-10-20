"""Python literals for the values a flow stores, written as code that runs."""

from __future__ import annotations

import datetime
import decimal
import math
import zoneinfo


def python_literal(value, imports: set[str]) -> str:
    """``repr(value)`` as code that runs, the imports it needs added to ``imports``.

    A non-finite float is ``float("nan")`` / ``float("inf")`` (its repr is a bare name); a date, time or
    timedelta reprs as ``datetime.<type>(...)`` and a zone as ``zoneinfo.ZoneInfo(...)``, so those modules are
    imported; a decimal is written ``decimal.Decimal('...')``, since ``Decimal`` is also a Polars dtype name a
    script may import. Lists, tuples and dicts are rendered element by element; anything else keeps its repr,
    an ``Expr`` included (its repr is code).
    """
    if isinstance(value, float) and not math.isfinite(value):
        return f'float("{value}")'
    if isinstance(value, list):
        return "[" + ", ".join(python_literal(item, imports) for item in value) + "]"
    if isinstance(value, tuple):
        items = [python_literal(item, imports) for item in value]
        return "(" + ", ".join(items) + ("," if len(items) == 1 else "") + ")"
    if isinstance(value, dict):
        items = (f"{python_literal(key, imports)}: {python_literal(item, imports)}" for key, item in value.items())
        return "{" + ", ".join(items) + "}"
    if isinstance(value, datetime.date | datetime.time | datetime.timedelta):
        imports.add("import datetime")
        if isinstance(getattr(value, "tzinfo", None), zoneinfo.ZoneInfo):
            imports.add("import zoneinfo")
    elif isinstance(value, decimal.Decimal):
        imports.add("import decimal")
        return f"decimal.Decimal({str(value)!r})"
    return repr(value)
