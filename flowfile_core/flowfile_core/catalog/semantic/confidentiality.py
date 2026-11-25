"""Which catalog columns are kept from AI, read from the semantic ledger.

A column is *kept from AI* when its effective confidentiality is ``confidential`` or ``restricted``: the highest of
every live fact that can set it (its own, its semantic type's and its table's, each accepted or still waiting for
review, so a proposal to raise the level protects at once). A table at such a level keeps all of its columns. This is
glosslayer's own AI ceiling (``ai_max = internal``) applied where Flowfile reads data for a model.

The ledger is read without the caller's scope (the question is what must not leave) and nothing is cached. Without
``glosslayer`` nothing is withheld; a ledger that cannot be read keeps every table asked about (fail closed). Every
``glosslayer`` import is function-local.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from flowfile_core.catalog.semantic.snapshot import LIVE

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
        from glosslayer.ids import Kind, SubjectId
        from glosslayer.model import Confidentiality, Status
        from glosslayer.store import Query

        from flowfile_core.catalog.semantic import host
        from flowfile_core.database.models import CatalogTable

        store = host.host().gloss.store
    except ImportError:
        return Withheld()
    except Exception:  # noqa: BLE001
        logger.warning("semantic ledger unavailable; keeping tables %s from AI", ids, exc_info=True)
        return Withheld(unreadable=frozenset(ids))
    try:
        keys = {host.table_key(row, db): row.id for row in db.query(CatalogTable).filter(CatalogTable.id.in_(ids))}
        query = Query(attributes=("confidentiality", "semantic_type"), statuses=[Status(s) for s in LIVE])
        parsed = [(SubjectId.parse(a.subject), a) for a in store.find(query)]
        levels = [(sid, Confidentiality(a.value)) for sid, a in parsed if a.attribute == "confidentiality"]
    except Exception:  # noqa: BLE001
        logger.warning("confidentiality could not be read; keeping tables %s from AI", ids, exc_info=True)
        return Withheld(unreadable=frozenset(ids))

    high = [sid for sid, level in levels if level >= Confidentiality.CONFIDENTIAL]
    types = {sid.ref for sid in high if sid.kind is Kind.TYPE}
    typed = [sid for sid, a in parsed if a.attribute == "semantic_type" and a.value in types]
    columns: dict[int, set[str]] = {}
    for sid in [*high, *typed]:
        if sid.kind is Kind.FIELD and sid.dataset in keys:
            columns.setdefault(keys[sid.dataset], set()).add(sid.name)
    tables = {keys[sid.dataset] for sid in high if sid.kind is Kind.DATASET and sid.dataset in keys}
    return Withheld(columns={t: frozenset(c) for t, c in columns.items()}, tables=frozenset(tables))
