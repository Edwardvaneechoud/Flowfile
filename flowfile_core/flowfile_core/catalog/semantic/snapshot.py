"""A namespace's semantic model read from the ledger into the API's own shapes (``schemas/semantic_schema.py``).

``build_snapshot`` takes an actor-bound view (``host.view_for``) and the catalog's tables for one namespace. With
``accepted_only`` it is the published model, the only one a query may be compiled from. Reads are a fixed number of
ledger queries per snapshot (live facts, the catalog layer's joins, their events, the versions), never one per table.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from flowfile_core.schemas.semantic_schema import FactOut, FieldOut, JoinOut, MetricOut, SemanticTableOut

ACCEPTED = "accepted"
OPEN = ("proposed", "reviewed", "stale")
LIVE = (*OPEN, ACCEPTED)
GROUPED_KINDS = ("relationship", "metric")
_SOURCE_KINDS = ("human", "ai", "inferred", "catalog", "import", "system")


@dataclass(frozen=True)
class TableInfo:
    """What the catalog says about one table: its id, dataset key, name, description and ``(column, dtype)`` list."""

    table_id: int
    key: str
    name: str
    description: str | None = None
    columns: tuple[tuple[str, str | None], ...] = ()

    def has_column(self, name: str) -> bool:
        return any(c == name for c, _dtype in self.columns)


@dataclass
class ModelSnapshot:
    """``related_tables``: catalog tables outside the model that a join may reach, by key."""

    namespace_id: int
    level: int
    path: str
    model_facts: dict[str, FactOut] = field(default_factory=dict)
    tables: list[SemanticTableOut] = field(default_factory=list)
    joins: list[JoinOut] = field(default_factory=list)
    facts: list[FactOut] = field(default_factory=list)
    related_tables: dict[str, TableInfo] = field(default_factory=dict)

    def table(self, key_or_name: str) -> SemanticTableOut | None:
        return next((t for t in self.tables if key_or_name in (t.key, t.name)), None)

    def model_value(self, attribute: str) -> Any:
        f = self.model_facts.get(attribute)
        return f.value if f is not None else None

    @property
    def metrics(self) -> list[MetricOut]:
        return [m for t in self.tables for m in t.metrics]

    def progress(self) -> dict[str, int]:
        """Counts of the live facts; facts the catalog itself accepted (column types) are left out."""
        return {
            "accepted": sum(f.status == ACCEPTED and f.source_kind != "catalog" for f in self.facts),
            "pending": sum(f.status in ("proposed", "reviewed") for f in self.facts),
            "stale": sum(f.status == "stale" for f in self.facts),
        }


def _best(rows: list[FactOut]) -> dict[str, FactOut]:
    """One fact per attribute: accepted wins, then the most recently updated."""
    best: dict[str, FactOut] = {}
    for f in rows:
        cur = best.get(f.attribute)
        if cur is None or (f.status == ACCEPTED, f.updated_at) > (cur.status == ACCEPTED, cur.updated_at):
            best[f.attribute] = f
    return best


def _open(rows: list[FactOut]) -> list[FactOut]:
    return [f for f in rows if f.status in OPEN]


def read_facts(store: Any, assertions: Any) -> list[FactOut]:
    """Ledger assertions as facts; ``via``, ``rationale`` and the last accept come from their events."""
    facts: dict[str, FactOut] = {}
    for a in assertions:
        kind = a.source.split(":", 1)[0] if ":" in a.source else "other"
        facts[a.id] = FactOut(
            id=a.id,
            subject=a.subject,
            attribute=a.attribute,
            value=a.value,
            status=a.status.value,
            source=a.source,
            source_kind=kind if kind in _SOURCE_KINDS else "other",
            created_by=a.created_by,
            updated_at=a.updated_at,
            confidence=a.confidence,
            group=a.group,
        )
    for ev in store.events(assertion_ids=list(facts)) if facts else ():
        f = facts[ev.assertion_id]
        if ev.type.value == "proposed":
            f.via, f.rationale = ev.via, ev.data.get("comment")
        elif ev.type.value in ("accepted", "restored"):
            f.accepted_by, f.accepted_at = ev.actor, ev.ts
    return list(facts.values())


def build_snapshot(
    view: Any,
    *,
    namespace_id: int,
    level: int,
    path: str,
    tables: list[TableInfo],
    known_tables: dict[str, int],
    catalog_path: str | None = None,
    accepted_only: bool = False,
) -> ModelSnapshot:
    """The model of the namespace at ``path`` as the ``view``'s actor may read it.

    ``catalog_path`` (for a schema) adds the catalog layer's joins that touch one of its ``tables``, read-only;
    ``known_tables`` maps any dataset key of the catalog to its table id, so a join's far end still names its table.
    """
    from glosslayer.ids import Kind, SubjectId

    statuses = [ACCEPTED] if accepted_only else list(LIVE)
    found = view.find(namespace=path, statuses=statuses)
    if catalog_path:
        found += view.find(subject_prefix=f"relationship:{catalog_path}::", statuses=statuses)
    facts = read_facts(view.store, [a for a in found if not a.redacted])
    keys = {t.key for t in tables}

    rows: dict[str, list[FactOut]] = {}
    subjects: dict[str, Any] = {}
    columns: dict[str, list[str]] = {}
    for f in facts:
        if f.subject not in subjects:
            sid = subjects[f.subject] = SubjectId.parse(f.subject)
            if sid.kind is Kind.FIELD:
                columns.setdefault(sid.dataset, []).append(sid.name)
        rows.setdefault(f.subject, []).append(f)
    metrics = sorted(s for s, sid in subjects.items() if sid.kind is Kind.METRIC and sid.dataset in keys)
    joins = sorted(s for s, sid in subjects.items() if sid.kind is Kind.RELATIONSHIP)
    versions = view.store.subject_versions(metrics + joins) if metrics or joins else {}

    snap = ModelSnapshot(namespace_id, level, path, model_facts=_best(rows.get(f"model:{path}", [])), facts=facts)
    for t in tables:
        own = rows.get(f"dataset:{t.key}", [])
        out = SemanticTableOut(
            table_id=t.table_id, key=t.key, name=t.name, description=t.description, facts=_best(own), pending=_open(own)
        )
        if "description" in out.facts:
            out.description = out.facts["description"].value
        dtypes = dict(t.columns)
        for name in [*dtypes, *sorted(set(columns.get(t.key, ())) - dtypes.keys())]:
            own = rows.get(f"field:{t.key}/{name}", [])
            out.fields.append(FieldOut(name=name, dtype=dtypes.get(name), facts=_best(own), pending=_open(own)))
        snap.tables.append(out)

    by_key = {t.key: t for t in snap.tables}
    for subject in metrics:
        m = _metric(subjects[subject], rows[subject], known_tables, versions.get(subject))
        if m is not None:
            by_key[m.table].metrics.append(m)
    snap.joins = [
        j for s in joins if (j := _join(subjects[s], rows[s], path, catalog_path, keys, known_tables)) is not None
    ]
    return snap


def _definition(rows: list[FactOut]) -> tuple[dict[str, FactOut], str, str | None, str | None]:
    """The shown definition's facts, its status, its group, and the group of an open edit beside it."""
    accepted = [f for f in rows if f.status == ACCEPTED]
    open_rows = sorted(_open(rows), key=lambda f: f.updated_at, reverse=True)
    pending_group = open_rows[0].group if open_rows else None
    if accepted:
        group = next((f for f in accepted if f.attribute in ("expression", "from")), accepted[0]).group
        return _best(accepted), ACCEPTED, group, pending_group if pending_group != group else None
    shown = [f for f in open_rows if f.group == pending_group] if pending_group else open_rows
    status = "stale" if any(f.status == "stale" for f in shown) else shown[0].status
    return _best(shown), status, pending_group, None


def _metric(sid: Any, rows: list[FactOut], ids: dict[str, int], version: int | None) -> MetricOut | None:
    facts, status, group, pending_group = _definition(rows)
    if "expression" not in facts:
        return None
    expr = facts["expression"]
    values = {a: f.value for a, f in facts.items() if a in MetricOut.model_fields and f.value is not None}
    return MetricOut(
        **values | {"expression": expr.value.get("sql") if isinstance(expr.value, dict) else expr.value},
        key=str(sid),
        name=sid.name,
        table=sid.dataset,
        table_id=ids.get(sid.dataset),
        status=status,
        group=group,
        version=version,
        approved_by=expr.accepted_by if status == ACCEPTED else None,
        approved_at=expr.accepted_at if status == ACCEPTED else None,
        pending_group=pending_group,
    )


def _join(
    sid: Any, rows: list[FactOut], path: str, catalog_path: str | None, keys: set[str], ids: dict[str, int]
) -> JoinOut | None:
    facts, status, group, _pending = _definition(rows)
    attrs = {a: f.value for a, f in facts.items()}
    if not all(attrs.get(k) for k in ("from", "to", "from_columns", "to_columns")):
        return None
    ns = sid.namespace or ""
    if catalog_path and ns == catalog_path and not {attrs["from"], attrs["to"]} & keys:
        return None
    return JoinOut(
        key=str(sid),
        name=sid.name or "",
        layer="schema" if "." in ns else "catalog",
        from_table=attrs["from"],
        to_table=attrs["to"],
        from_table_id=ids.get(attrs["from"]),
        to_table_id=ids.get(attrs["to"]),
        from_columns=attrs["from_columns"],
        to_columns=attrs["to_columns"],
        description=attrs.get("description"),
        status=status,
        group=group,
    )
