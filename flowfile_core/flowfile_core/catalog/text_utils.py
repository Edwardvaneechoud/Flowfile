"""SQL-text rewriting and Delta-history parsing helpers.

Pure functions extracted from ``catalog.service``. ``service`` re-exports
the underscore-prefixed names for backward compatibility with tests that
import them directly.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable

from flowfile_core.schemas.catalog_schema import DeltaVersionCommit
from shared.delta_utils import format_delta_timestamp

_TABLE_INTRODUCERS = r"\b(?:FROM|JOIN|INTO|UPDATE)\b|,"


def rewrite_qualified_references(query: str, qualified_names: Iterable[str]) -> str:
    """Rewrite an N-part ``a.b.c`` reference (bare / double- / single-quoted / mixed
    segments) to the single quoted identifier ``"a.b.c"`` the flat SQL engine registers.

    Only rewrites occurrences matching a known registered qualified name, so column
    qualifiers like ``t.col`` on unrelated aliases are untouched. Longest-first so a
    3-part name is collapsed before its 2-part suffix. Single quotes are always string
    literals in this dialect, so a full dotted chain of quoted/bare segments matching a
    *registered* table name is unambiguously a (mis-quoted) table reference.
    """
    for qualified_name in sorted(qualified_names, key=len, reverse=True):
        if "." not in qualified_name:
            continue
        parts = qualified_name.split(".")  # names can't contain '.' (reject_dot_in_name), so lossless
        segments = []
        for i, part in enumerate(parts):
            esc = re.escape(part)
            bare = esc
            if i == 0:
                bare = rf'(?<![\w"\']){bare}'
            if i == len(parts) - 1:
                bare = rf'{bare}(?![\w"\'])'
            segments.append(rf"""(?:"{esc}"|'{esc}'|{bare})""")
        pattern = re.compile(r"\s*\.\s*".join(segments))
        query = pattern.sub(f'"{qualified_name}"', query)
    return query


_RELATION_NOT_FOUND = re.compile(r"relation '([^']+)' was not found", re.IGNORECASE)


def friendly_relation_error(error: str, query: str) -> str:
    """Clarify polars' ``relation 'X' was not found`` for multi-part references.

    Polars reports only the FIRST segment of an ``a.b.c`` reference, so a missing
    table reads as a missing catalog (``relation 'global-catalog' was not found``).
    When the query contains a multi-part reference starting with that segment, name
    the full reference the user actually wrote instead.
    """
    match = _RELATION_NOT_FOUND.search(error)
    if match is None:
        return error
    seg = re.escape(match.group(1))
    part = r"""(?:"[^"]*"|'[^']*'|[\w-]+)"""
    ref = re.compile(rf"""(?:"{seg}"|'{seg}'|{seg})(?:\s*\.\s*{part})+""")
    found = ref.search(query)
    if found is None:
        return error  # single-part / not a dotted reference: polars' message is already clear
    display = re.sub(r"\s*\.\s*", ".", found.group(0)).replace('"', "").replace("'", "")
    return (
        f"Table '{display}' was not found in the catalog. "
        f"Check the name is spelled correctly and the table is registered."
    )


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


def hash_source_versions(versions_json: str | None) -> str:
    """Stable cache key for a virtual table's source-version state."""
    if not versions_json:
        return "noversions"
    return hashlib.sha256(versions_json.encode()).hexdigest()
