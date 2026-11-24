"""Which catalog columns are kept from AI, read from the semantic ledger.

A column is *kept from AI* when its effective confidentiality is ``confidential`` or ``restricted``. The effective
level is the highest of every live fact that can set it: the column's own ``confidentiality``, the confidentiality of
the semantic type it is (or is proposed to be), and its table's ``confidentiality``, each the accepted fact or any
fact still waiting for review (a proposal to raise the level protects at once). A table at such a level keeps all of
its columns from AI. This is glosslayer's own AI ceiling (``ai_max = internal``: above it a model gets a subject's
name and type only) applied where Flowfile reads data for a model.

The ledger is read without the caller's scope: the question is what must not leave, not what this user may read.
Each call is one ledger query (the live ``confidentiality`` and ``semantic_type`` facts) plus one catalog query for
the tables' keys; nothing is cached, so a raised level counts on the next request. When ``glosslayer`` is not
installed nothing is withheld (as before the ledger existed). When the ledger cannot be read, every table asked
about is ``unreadable`` and keeps all its columns from AI (fail closed). Every ``glosslayer`` import is
function-local.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

WITHHELD_LEVELS: tuple[str, ...] = ("confidential", "restricted")
_RANKS = {"public": 0, "internal": 1, "confidential": 2, "restricted": 3}
_LIVE = ("proposed", "reviewed", "stale", "accepted")
_ATTRIBUTES = ("confidentiality", "semantic_type")


def is_withheld(level: Any) -> bool:
    """Whether a confidentiality level (a string or glosslayer's enum) keeps a subject from AI."""
    return str(getattr(level, "value", level) or "").strip().lower() in WITHHELD_LEVELS


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


def _rank(value: Any) -> int:
    return _RANKS.get(str(getattr(value, "value", value) or "").strip().lower(), -1)


def read_withheld(db: Any, table_ids: Iterable[int]) -> Withheld:
    """What is kept from AI among ``table_ids`` (catalog table ids), read on the caller's ``db`` session."""
    ids = sorted({int(t) for t in table_ids})
    if not ids:
        return Withheld()
    try:
        from glosslayer.ids import Kind, SubjectId
        from glosslayer.model import Status
        from glosslayer.store import Query

        from flowfile_core.catalog.semantic import host as gloss_host

        ledger = gloss_host.host()
    except ImportError:
        return Withheld()
    except Exception:  # noqa: BLE001
        logger.warning("semantic ledger unavailable; keeping tables %s from AI", ids, exc_info=True)
        return Withheld(unreadable=frozenset(ids))

    try:
        from flowfile_core.database.models import CatalogTable

        keys: dict[str, int] = {}
        for row in db.query(CatalogTable).filter(CatalogTable.id.in_(ids)):
            keys[gloss_host.table_key(row, db)] = row.id
        facts = ledger.gloss.store.find(Query(attributes=_ATTRIBUTES, statuses=[Status(s) for s in _LIVE]))
    except Exception:  # noqa: BLE001
        logger.warning("confidentiality could not be read; keeping tables %s from AI", ids, exc_info=True)
        return Withheld(unreadable=frozenset(ids))

    field_level: dict[tuple[str, str], int] = {}
    field_types: dict[tuple[str, str], set[str]] = {}
    dataset_level: dict[str, int] = {}
    type_level: dict[str, int] = {}
    for a in facts:
        try:
            sid = SubjectId.parse(a.subject)
        except ValueError:
            continue
        if a.attribute == "semantic_type":
            if sid.kind is Kind.FIELD and sid.dataset in keys and isinstance(a.value, str):
                field_types.setdefault((sid.dataset, sid.name or ""), set()).add(a.value)
            continue
        rank = _rank(a.value)
        if sid.kind is Kind.FIELD and sid.dataset in keys:
            key = (sid.dataset, sid.name or "")
            field_level[key] = max(field_level.get(key, -1), rank)
        elif sid.kind is Kind.DATASET and sid.dataset in keys:
            dataset_level[sid.dataset] = max(dataset_level.get(sid.dataset, -1), rank)
        elif sid.kind is Kind.TYPE and sid.name:
            type_level[sid.name] = max(type_level.get(sid.name, -1), rank)

    floor = _RANKS["confidential"]
    columns: dict[int, set[str]] = {}
    for (dataset, column), names in field_types.items():
        if max((type_level.get(n, -1) for n in names), default=-1) >= floor:
            columns.setdefault(keys[dataset], set()).add(column)
    for (dataset, column), rank in field_level.items():
        if rank >= floor:
            columns.setdefault(keys[dataset], set()).add(column)
    tables = {keys[d] for d, rank in dataset_level.items() if rank >= floor}
    return Withheld(columns={t: frozenset(c) for t, c in columns.items()}, tables=frozenset(tables))


def withheld_columns(db: Any, table_ids: Iterable[int], columns: dict[int, Iterable[str]]) -> dict[int, set[str]]:
    """The kept-from-AI names among each table's ``columns``, for the tables that have any."""
    found = read_withheld(db, table_ids)
    out = {tid: set(found.names(tid, cols)) for tid, cols in columns.items()}
    return {tid: names for tid, names in out.items() if names}


__all__ = ["WITHHELD_LEVELS", "Withheld", "is_withheld", "read_withheld", "withheld_columns"]
