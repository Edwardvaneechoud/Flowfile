"""SQL table-reference matching shared by Core and notebook kernels."""

import re

_TABLE_INTRODUCERS = r"\b(?:FROM|JOIN|INTO|UPDATE)\b|,"


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
