"""SQL-text rewriting and Delta-history parsing helpers.

Pure functions extracted from ``catalog.service``. ``service`` re-exports
the underscore-prefixed names for backward compatibility with tests that
import them directly.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from flowfile_core.schemas.catalog_schema import DeltaVersionCommit
from shared.delta_utils import format_delta_timestamp

_TABLE_INTRODUCERS = r"\b(?:FROM|JOIN|INTO|UPDATE)\b|,"


def rewrite_qualified_references(query: str, qualified_names: Iterable[str]) -> str:
    """Rewrite ``ns.table`` / ``"ns"."table"`` / mixed variants to ``"ns.table"``.

    Only rewrites occurrences matching a known registered qualified name, so column
    qualifiers like ``t.col`` on unrelated aliases are untouched.
    """
    for qualified_name in sorted(qualified_names, key=len, reverse=True):
        if "." not in qualified_name:
            continue
        namespace, _, table = qualified_name.partition(".")
        ns_esc, table_esc = re.escape(namespace), re.escape(table)
        ns_part = rf'(?:(?<![\w"]){ns_esc}|"{ns_esc}")'
        table_part = rf'(?:{table_esc}(?![\w"])|"{table_esc}")'
        pattern = re.compile(rf"{ns_part}\s*\.\s*{table_part}")
        query = pattern.sub(f'"{qualified_name}"', query)
    return query


def is_table_reference(name: str, query: str) -> bool:
    """Return True iff ``name`` appears as an actual table reference in ``query``.

    Matches after a table-introducing keyword (``FROM``/``JOIN``/``INTO``/``UPDATE``)
    or a comma (continuation of a ``FROM`` list). Accepts both the bare identifier
    and its double-quoted form; rejects lookalikes that continue into a longer
    identifier (e.g. ``t`` should not match inside ``test-table``). This avoids
    false positives from column aliases (``SELECT x AS t``) and substrings.
    """
    escaped = re.escape(name)
    pattern = re.compile(
        rf'(?:{_TABLE_INTRODUCERS})\s+(?:"{escaped}"|{escaped}(?![\w"-]))',
        re.IGNORECASE,
    )
    return bool(pattern.search(query))


def parse_delta_history(raw_history: list[dict]) -> list[DeltaVersionCommit]:
    """Convert raw deltalake history dicts into typed ``DeltaVersionCommit`` models."""
    return [
        DeltaVersionCommit(
            version=h.get("version"),
            timestamp=format_delta_timestamp(h.get("timestamp")),
            operation=h.get("operation"),
            parameters=h.get("operationParameters"),
        )
        for h in raw_history
    ]
