"""A namespace's semantic model read from the ledger into plain dataclasses.

``build_snapshot`` takes an actor-bound view (``host.view_for``) and the catalog's tables for one namespace and
returns a ``ModelSnapshot``: the tables with their columns and per-column facts, the joins and the metrics, each fact
with its review status. It knows nothing of FastAPI or Pydantic, so the metric compiler can read the same thing the
API shows; ``accepted_only=True`` (or ``ModelSnapshot.accepted_only()``) gives the published model, the only one a
query may be compiled from.

Reads are a fixed number of ledger queries per snapshot (the namespace's live facts, the catalog layer's joins, the
events behind those facts, the metric versions), never one per table.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

ACCEPTED = "accepted"
OPEN = ("proposed", "reviewed", "stale")
LIVE = (*OPEN, ACCEPTED)
GROUPED_KINDS = ("relationship", "metric")


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
class FactSnap:
    id: str
    subject: str
    attribute: str
    value: Any
    status: str
    source: str
    created_by: str
    updated_at: datetime
    confidence: float | None = None
    group: str | None = None
    via: str | None = None
    rationale: str | None = None
    accepted_by: str | None = None
    accepted_at: datetime | None = None

    @property
    def source_kind(self) -> str:
        kind = self.source.split(":", 1)[0] if ":" in self.source else "other"
        return kind if kind in ("human", "ai", "inferred", "catalog", "import", "system") else "other"

    @property
    def is_open(self) -> bool:
        return self.status in OPEN


@dataclass
class ColumnSnap:
    """``facts``: the shown value per attribute (accepted wins, else the newest open); ``pending``: all open facts."""

    name: str
    dtype: str | None
    facts: dict[str, FactSnap] = field(default_factory=dict)
    pending: list[FactSnap] = field(default_factory=list)

    def value(self, attribute: str) -> Any:
        f = self.facts.get(attribute)
        return f.value if f is not None else None


@dataclass
class MetricSnap:
    """A table's metric as one definition: the accepted group when there is one, else the newest open group."""

    key: str
    name: str
    table: str
    table_id: int | None
    facts: dict[str, FactSnap]
    status: str
    group: str | None
    version: int | None = None
    approved_by: str | None = None
    approved_at: datetime | None = None
    pending_group: str | None = None

    def value(self, attribute: str) -> Any:
        f = self.facts.get(attribute)
        return f.value if f is not None else None

    @property
    def expression(self) -> str | None:
        expr = self.value("expression")
        return expr.get("sql") if isinstance(expr, dict) else expr


@dataclass
class JoinSnap:
    """A join from the many side (``from_table``) to the one side (``to_table``); ``layer`` is its owner's level."""

    key: str
    name: str
    namespace: str
    layer: str
    from_table: str
    to_table: str
    from_columns: list[str]
    to_columns: list[str]
    description: str | None
    status: str
    group: str | None
    facts: dict[str, FactSnap] = field(default_factory=dict)
    from_table_id: int | None = None
    to_table_id: int | None = None


@dataclass
class TableSnap:
    table_id: int
    key: str
    name: str
    description: str | None
    columns: list[ColumnSnap] = field(default_factory=list)
    facts: dict[str, FactSnap] = field(default_factory=dict)
    pending: list[FactSnap] = field(default_factory=list)
    metrics: list[MetricSnap] = field(default_factory=list)

    def column(self, name: str) -> ColumnSnap | None:
        return next((c for c in self.columns if c.name == name), None)


@dataclass
class ModelSnapshot:
    namespace_id: int
    level: int
    path: str
    model_facts: dict[str, FactSnap] = field(default_factory=dict)
    tables: list[TableSnap] = field(default_factory=list)
    joins: list[JoinSnap] = field(default_factory=list)
    facts: list[FactSnap] = field(default_factory=list)
    related_tables: dict[str, TableInfo] = field(default_factory=dict)
    """Catalog tables outside the model a join may reach, by dataset key; the metric compiler reads their columns."""

    def table(self, key_or_name: str) -> TableSnap | None:
        return next((t for t in self.tables if key_or_name in (t.key, t.name)), None)

    @property
    def metrics(self) -> list[MetricSnap]:
        return [m for t in self.tables for m in t.metrics]

    def metric(self, name: str, table: str | None = None) -> MetricSnap | None:
        """The metric called ``name`` (optionally on ``table``, a key or a short name); ``None`` when absent."""
        for m in self.metrics:
            if m.name == name and (table is None or table in (m.table, self.table(m.table).name)):
                return m
        return None

    def progress(self) -> dict[str, int]:
        """Counts of the live facts; facts the catalog itself accepted (column types) are left out."""
        out = {"accepted": 0, "pending": 0, "stale": 0}
        for f in self.facts:
            if f.status == ACCEPTED:
                if f.source_kind != "catalog":
                    out["accepted"] += 1
            elif f.status == "stale":
                out["stale"] += 1
            elif f.status in OPEN:
                out["pending"] += 1
        return out

    def accepted_only(self) -> ModelSnapshot:
        """The published model: accepted facts only, metrics and joins whose definition is accepted."""
        return _filter_accepted(self)


def _best(rows: list[FactSnap]) -> dict[str, FactSnap]:
    """One fact per attribute: accepted wins, then the most recently updated."""
    best: dict[str, FactSnap] = {}
    for f in rows:
        cur = best.get(f.attribute)
        if cur is None or (f.status != ACCEPTED, -f.updated_at.timestamp()) < (
            cur.status != ACCEPTED,
            -cur.updated_at.timestamp(),
        ):
            best[f.attribute] = f
    return best


def fact_from_assertion(a: Any) -> FactSnap:
    return FactSnap(
        id=a.id,
        subject=a.subject,
        attribute=a.attribute,
        value=a.value,
        status=a.status.value,
        source=a.source,
        created_by=a.created_by,
        updated_at=a.updated_at,
        confidence=a.confidence,
        group=a.group,
    )


def annotate(store: Any, facts: list[FactSnap]) -> None:
    """Fill ``via`` and ``rationale`` from each fact's proposed event, ``accepted_by``/``at`` from its last accept."""
    if not facts:
        return
    by_id = {f.id: f for f in facts}
    for ev in store.events(assertion_ids=list(by_id)):
        f = by_id.get(ev.assertion_id)
        if f is None:
            continue
        etype = ev.type.value
        if etype == "proposed":
            f.via = ev.via
            f.rationale = ev.data.get("comment")
        elif etype in ("accepted", "restored"):
            f.accepted_by, f.accepted_at = ev.actor, ev.ts


def read_facts(view: Any, path: str, *, catalog_path: str | None = None, statuses=LIVE) -> list[FactSnap]:
    """Live facts at or under ``path``, plus (``catalog_path``) the catalog layer's join facts, annotated."""
    rows = view.find(namespace=path, statuses=list(statuses))
    if catalog_path and catalog_path != path:
        rows += view.find(subject_prefix=f"relationship:{catalog_path}::", statuses=list(statuses))
    facts = [fact_from_assertion(a) for a in rows if not a.redacted]
    annotate(view.store, facts)
    return facts


def build_snapshot(
    view: Any,
    *,
    namespace_id: int,
    level: int,
    path: str,
    tables: list[TableInfo],
    catalog_path: str | None = None,
    known_tables: dict[str, int] | None = None,
    accepted_only: bool = False,
) -> ModelSnapshot:
    """The model of the namespace at ``path`` as the ``view``'s actor may read it.

    ``tables`` are the catalog tables the model covers; ``catalog_path`` (for a schema) adds the catalog layer's
    joins that touch one of them, read-only; ``known_tables`` maps any dataset key of the catalog to its table id, so
    a join's far end in another schema still names its table.
    """
    from glosslayer.ids import Kind, SubjectId

    statuses = (ACCEPTED,) if accepted_only else LIVE
    facts = read_facts(view, path, catalog_path=catalog_path, statuses=statuses)
    by_key = {t.key: t for t in tables}
    ids = dict(known_tables or {})
    ids.update({t.key: t.table_id for t in tables})

    model_rows: list[FactSnap] = []
    dataset_rows: dict[str, list[FactSnap]] = {}
    field_rows: dict[tuple[str, str], list[FactSnap]] = {}
    metric_rows: dict[str, list[FactSnap]] = {}
    join_rows: dict[str, list[FactSnap]] = {}
    for f in facts:
        sid = SubjectId.parse(f.subject)
        if sid.kind is Kind.MODEL and sid.namespace == path:
            model_rows.append(f)
        elif sid.kind is Kind.DATASET and sid.dataset in by_key:
            dataset_rows.setdefault(sid.dataset, []).append(f)
        elif sid.kind is Kind.FIELD and sid.dataset in by_key:
            field_rows.setdefault((sid.dataset, sid.name), []).append(f)
        elif sid.kind is Kind.METRIC and sid.dataset in by_key:
            metric_rows.setdefault(f.subject, []).append(f)
        elif sid.kind is Kind.RELATIONSHIP:
            join_rows.setdefault(f.subject, []).append(f)

    versions = view.store.subject_versions(list(metric_rows) + list(join_rows)) if (metric_rows or join_rows) else {}
    snap = ModelSnapshot(namespace_id=namespace_id, level=level, path=path, model_facts=_best(model_rows), facts=facts)

    for t in tables:
        rows = dataset_rows.get(t.key, [])
        ts = TableSnap(
            table_id=t.table_id,
            key=t.key,
            name=t.name,
            description=t.description,
            facts=_best(rows),
            pending=[f for f in rows if f.is_open],
        )
        names = [c for c, _ in t.columns]
        extra = sorted({c for (k, c) in field_rows if k == t.key and c not in names})
        dtypes = dict(t.columns)
        for name in [*names, *extra]:
            rows = field_rows.get((t.key, name), [])
            ts.columns.append(
                ColumnSnap(name=name, dtype=dtypes.get(name), facts=_best(rows), pending=[f for f in rows if f.is_open])
            )
        snap.tables.append(ts)

    tables_by_key = {t.key: t for t in snap.tables}
    for subject, rows in sorted(metric_rows.items()):
        m = _metric(subject, rows, ids, versions.get(subject))
        if m is not None:
            tables_by_key[m.table].metrics.append(m)

    for subject, rows in sorted(join_rows.items()):
        j = _join(subject, rows, path, catalog_path, by_key, ids)
        if j is not None:
            snap.joins.append(j)
    return snap


def _definition(rows: list[FactSnap]) -> tuple[list[FactSnap], str, str | None, str | None]:
    """The rows of the shown definition, its status, its group, and the group of an open edit beside it."""
    accepted = [f for f in rows if f.status == ACCEPTED]
    open_rows = sorted((f for f in rows if f.is_open), key=lambda f: f.updated_at, reverse=True)
    pending_group = open_rows[0].group if open_rows else None
    if accepted:
        expr = next((f for f in accepted if f.attribute in ("expression", "from")), accepted[0])
        return accepted, ACCEPTED, expr.group, pending_group if pending_group != expr.group else None
    if not open_rows:
        return [], "proposed", None, None
    group = pending_group
    shown = [f for f in open_rows if f.group == group] if group else open_rows
    status = "stale" if any(f.status == "stale" for f in shown) else shown[0].status
    return shown, status, group, None


def _metric(subject: str, rows: list[FactSnap], ids: dict[str, int], version: int | None) -> MetricSnap | None:
    from glosslayer.ids import SubjectId

    shown, status, group, pending_group = _definition(rows)
    facts = _best(shown)
    if "expression" not in facts:
        return None
    sid = SubjectId.parse(subject)
    expr = facts["expression"]
    return MetricSnap(
        key=subject,
        name=sid.name or "",
        table=sid.dataset or "",
        table_id=ids.get(sid.dataset or ""),
        facts=facts,
        status=status,
        group=group,
        version=version,
        approved_by=expr.accepted_by if status == ACCEPTED else None,
        approved_at=expr.accepted_at if status == ACCEPTED else None,
        pending_group=pending_group,
    )


def _join(
    subject: str,
    rows: list[FactSnap],
    path: str,
    catalog_path: str | None,
    by_key: dict[str, TableInfo],
    ids: dict[str, int],
) -> JoinSnap | None:
    from glosslayer.ids import SubjectId

    sid = SubjectId.parse(subject)
    shown, status, group, _pending = _definition(rows)
    facts = _best(shown)
    attrs = {a: f.value for a, f in facts.items()}
    if not all(attrs.get(k) for k in ("from", "to", "from_columns", "to_columns")):
        return None
    ns = sid.namespace or ""
    if catalog_path and ns == catalog_path and ns != path:
        if attrs["from"] not in by_key and attrs["to"] not in by_key:
            return None
        layer = "catalog"
    else:
        layer = "catalog" if "." not in ns else "schema"
    return JoinSnap(
        key=subject,
        name=sid.name or "",
        namespace=ns,
        layer=layer,
        from_table=attrs["from"],
        to_table=attrs["to"],
        from_columns=list(attrs["from_columns"]),
        to_columns=list(attrs["to_columns"]),
        description=attrs.get("description"),
        status=status,
        group=group,
        facts=facts,
        from_table_id=ids.get(attrs["from"]),
        to_table_id=ids.get(attrs["to"]),
    )


def _filter_accepted(snap: ModelSnapshot) -> ModelSnapshot:
    def keep(d: dict[str, FactSnap]) -> dict[str, FactSnap]:
        return {a: f for a, f in d.items() if f.status == ACCEPTED}

    tables = [
        replace(
            t,
            facts=keep(t.facts),
            pending=[],
            columns=[replace(c, facts=keep(c.facts), pending=[]) for c in t.columns],
            metrics=[replace(m, pending_group=None) for m in t.metrics if m.status == ACCEPTED],
        )
        for t in snap.tables
    ]
    return replace(
        snap,
        model_facts=keep(snap.model_facts),
        tables=tables,
        joins=[j for j in snap.joins if j.status == ACCEPTED],
        facts=[f for f in snap.facts if f.status == ACCEPTED],
    )
