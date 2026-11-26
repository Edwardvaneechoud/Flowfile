"""Which catalog columns are kept from AI, read from the semantic ledger.

A column is *kept from AI* when its level is above glosslayer's own AI ceiling (``internal``) by the strongest live
fact that can set it (``glosslayer.visibility.above``: its own, its semantic type's or its table's, accepted or still
waiting for review). A table above it keeps all of its columns.

The ledger is read without the caller's scope (the question is what must not leave), on core's engine and without the
Flowfile plugin, and nothing is cached. A ledger that cannot be read, or a missing ``glosslayer``, keeps every
table asked about (fail closed). Every ``glosslayer`` import is function-local.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Withheld:
    """What is kept from AI for some tables: named columns, whole tables, and tables whose level could not be read."""

    columns: dict[int, frozenset[str]] = field(default_factory=dict)
    tables: frozenset[int] = frozenset()
    unreadable: frozenset[int] = frozenset()

    def whole_table(self, table_id: int) -> bool:
        return table_id in self.tables or table_id in self.unreadable

    def blocks(self, table_id: int, column: str) -> bool:
        """Whether ``column`` of ``table_id`` is kept from AI."""
        return self.whole_table(table_id) or column in self.columns.get(table_id, ())

    def names(self, table_id: int, columns: Iterable[str]) -> list[str]:
        """The names among ``columns`` (a table's columns) that are kept from AI, in their order."""
        return [c for c in columns if self.blocks(table_id, c)]

    def __bool__(self) -> bool:
        return bool(self.tables or self.unreadable or any(self.columns.values()))


def read_withheld(db: Any, table_ids: Iterable[int]) -> Withheld:
    """What is kept from AI among ``table_ids`` (catalog table ids), read on the caller's ``db`` session."""
    ids = sorted({int(t) for t in table_ids})
    if not ids:
        return Withheld()
    try:
        from glosslayer.model import Confidentiality
        from glosslayer.store import SQLStore
        from glosslayer.visibility import above
    except ImportError:
        logger.warning("glosslayer is not installed; keeping tables %s from AI", ids)
        return Withheld(unreadable=frozenset(ids))
    from flowfile_core.catalog.semantic import host
    from flowfile_core.database.connection import engine
    from flowfile_core.database.models import CatalogTable

    try:
        keys = {host.table_key(row, db): row.id for row in db.query(CatalogTable).filter(CatalogTable.id.in_(ids))}
        found = above(SQLStore(engine), Confidentiality.INTERNAL)
    except Exception:  # noqa: BLE001
        logger.warning("confidentiality could not be read; keeping tables %s from AI", ids, exc_info=True)
        return Withheld(unreadable=frozenset(ids))
    return Withheld(
        columns={keys[k]: names for k, names in found.columns.items() if k in keys},
        tables=frozenset(keys[k] for k in found.datasets if k in keys),
    )
