"""The semantic model of a catalog namespace: read, propose, review, history, sync and export.

One model per schema (namespace level 1) and one per catalog (level 0), the catalog's holding the joins that cross
its schemas. Facts live in the glosslayer ledger; this service maps a namespace to its dotted path, checks the
caller against the catalog's own grants (reading needs the namespace visible, proposing needs it writable) and
leaves accepting to glosslayer's policy through the actor-bound view (``host.view_for``).

Proposals are checked against the catalog before they are stored, so the ledger never holds a fact about a column
that does not exist. Ledger errors are translated into the catalog's exception family here, so the router maps
them like every other catalog error and never answers 401.

The ledger is kept current lazily: nothing syncs at startup and the project importer reaches no hook, so reading a
model runs ``host.sync()`` when this process has not synced in the last minute.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.exceptions import (
    NamespaceNotFoundError,
    NotAuthorizedError,
    SemanticConflictError,
    SemanticNotFoundError,
    SemanticValueError,
)
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.semantic import host
from flowfile_core.catalog.semantic.compiler import (
    CompiledQuery,
    candidate_snapshot,
    compile_metric_query,
    with_candidate,
)
from flowfile_core.catalog.semantic.snapshot import (
    ACCEPTED,
    GROUPED_KINDS,
    LIVE,
    FactSnap,
    MetricSnap,
    ModelSnapshot,
    TableInfo,
    annotate,
    build_snapshot,
    fact_from_assertion,
)
from flowfile_core.catalog.service import CatalogService
from flowfile_core.schemas import semantic_schema as s

logger = logging.getLogger(__name__)

SYNC_INTERVAL_SECONDS = 60.0
_sync_lock = threading.Lock()
_last_sync = 0.0


def maybe_sync(force: bool = False) -> Any:
    """Pull-sync the catalog into the ledger unless this process did so in the last minute (always with ``force``).

    A failed lazy sync is logged and the model is read as it is; the explicit sync route lets it raise.
    """
    global _last_sync
    with _sync_lock:
        if not force and time.monotonic() - _last_sync < SYNC_INTERVAL_SECONDS:
            return None
        try:
            report = host.sync()
        except Exception:
            if force:
                raise
            logger.warning("Semantic ledger: sync before reading a model failed", exc_info=True)
            return None
        _last_sync = time.monotonic()
        return report


def validate_metric(
    table: TableInfo,
    proposal: s.MetricProposal,
    execute: Callable[[str], Any] | None = None,
    snapshot: ModelSnapshot | None = None,
) -> None:
    """Compile a proposed metric with no dimensions and, given ``execute``, dry-run it before it is stored.

    ``snapshot`` is the one a query compiles against (``_query_snapshot``: the catalog's joins and tables), so a
    formula reading a joined table's column is checked as it will run; without it only the base table is known.
    ``execute`` takes the SQL and answers a ``SqlQueryResult`` (one row is asked for). A compile failure or an engine
    error is the ``SemanticValueError`` the author sees, with the engine's message. Compute that cannot be reached
    does not block authoring: the dry-run is skipped and the proposal stored.
    """
    from requests import RequestException

    from flowfile_core.flowfile.execution.errors import ComputeError

    candidate = _metric_snap(table, proposal)
    try:
        snap = candidate_snapshot(table, candidate)
        if snapshot is not None:
            snap = with_candidate(snapshot, table, candidate)
        compiled = compile_metric_query(snap, s.MetricQuery(metric=proposal.name), metric_tables={table.key})
    except ValueError as exc:
        raise SemanticValueError(f"Metric {proposal.name!r} does not compile: {exc}") from None
    if execute is None:
        return
    try:
        result = execute(compiled.sql)
    except (ComputeError, RequestException) as exc:
        logger.warning(
            "Semantic model: metric %r stored without a dry-run, compute unavailable: %s", proposal.name, exc
        )
        return
    if result.error:
        raise SemanticValueError(f"Metric {proposal.name!r} does not run: {result.error}")


def _metric_snap(table: TableInfo, p: s.MetricProposal) -> MetricSnap:
    """The proposal as an accepted-looking ``MetricSnap``, the shape the compiler reads."""
    now = datetime.now(UTC)
    values = {
        "expression": p.expression,
        "filter": _clean(p.filter),
        "time_column": _clean(p.time_column),
        "dimensions": list(p.dimensions or []) or None,
    }
    facts = {
        attr: FactSnap(
            id=attr, subject="", attribute=attr, value=v, status=ACCEPTED, source="", created_by="", updated_at=now
        )
        for attr, v in values.items()
        if v is not None
    }
    return MetricSnap(
        key=p.name, name=p.name, table=table.key, table_id=table.table_id, facts=facts, status=ACCEPTED, group=None
    )


@contextmanager
def _ledger_errors(user_id: int | None) -> Iterator[None]:
    """Translate glosslayer's exceptions into the catalog's: 403, 404, 409 and 422 as the router maps them."""
    from glosslayer.model import InvalidTransition
    from glosslayer.policy import PermissionDenied

    try:
        yield
    except PermissionDenied as exc:
        err = NotAuthorizedError(user_id or -1)
        err.args = (str(exc),)
        raise err from None
    except InvalidTransition as exc:
        raise SemanticConflictError(str(exc)) from None
    except KeyError as exc:
        raise SemanticNotFoundError(str(exc.args[0]) if exc.args else "Not found") from None
    except ValueError as exc:
        raise SemanticValueError(str(exc)) from None


def _translate(user_id: int | None, exc: Exception) -> Exception:
    try:
        with _ledger_errors(user_id):
            raise exc
    except Exception as translated:
        return translated
    return exc


def _clean(value: Any) -> Any:
    return None if isinstance(value, str) and not value.strip() else value


def _blank(value: Any) -> bool:
    """A cleared value: nothing, blank text or an empty list. Proposing one means "remove what is there"."""
    return value is None or (isinstance(value, str) and not value.strip()) or (isinstance(value, list) and not value)


# A metric's parts an edit may leave out; the definition itself (``expression``) is always proposed.
_METRIC_OPTIONAL = ("description", "aggregate", "filter", "time_column", "dimensions", "format")


def _withdrawals(view: Any, assertions: list[Any]) -> list[tuple[str, Any]]:
    """How to take each live fact away: ``reject`` an open fact someone else stated, ``retract`` anything else.

    Retracting is withdrawing what is held (a person's own proposal, or an accepted fact); rejecting is a decision
    on another's proposal, which also keeps the rules and the AI from proposing the same value again.
    """
    out = []
    for a in assertions:
        stated_by_caller = view.actor is not None and view.actor in (a.sources or ())
        out.append(("reject" if a.status.value != ACCEPTED and not stated_by_caller else "retract", a))
    return out


def _withdraw(view: Any, assertions: list[Any], comment: str | None = None) -> list[Any]:
    """Take every fact in ``assertions`` away in one store transaction; the first refusal undoes them all."""
    plan = _withdrawals(view, assertions)
    changed: list[Any] = []
    with view.store.transaction():
        for action in ("reject", "retract"):
            ids = [a.id for act, a in plan if act == action]
            if not ids:
                continue
            done, errors = view.transition_many(action, ids, comment=comment)
            if errors:
                raise errors[0][1]
            changed += done
    return changed


def _may_withdraw(view: Any, assertions: list[Any]) -> bool:
    """Whether glosslayer's policy lets the view's actor take all of ``assertions`` away (``_withdrawals``)."""
    return all(view.policy.can(view.actor, action, a, view.context) for action, a in _withdrawals(view, assertions))


def fact_out(f: FactSnap) -> s.FactOut:
    return s.FactOut(
        id=f.id,
        attribute=f.attribute,
        value=f.value,
        status=f.status,
        source=f.source,
        source_kind=f.source_kind,
        via=f.via,
        confidence=f.confidence,
        rationale=f.rationale,
        created_by=f.created_by,
        updated_at=f.updated_at,
        group=f.group,
    )


class SemanticModelService:
    """Per-request service over one user's view of the ledger; built by the router's dependency."""

    def __init__(self, db: Session, user: Any, access: AccessResolver | None):
        self.db = db
        self.user = user
        self.user_id = getattr(user, "id", None)
        self.access = access
        self.repo = SQLAlchemyCatalogRepository(db)
        self._view = None

    @property
    def view(self) -> Any:
        if self._view is None:
            self._view = host.view_for(self.user, self.access)
        return self._view

    @property
    def _restricted(self) -> bool:
        return self.access is not None and self.access.restricted

    def _namespace(self, namespace_id: int) -> tuple[Any, str]:
        ns = self.repo.get_namespace(namespace_id)
        if ns is None or ns.level not in (0, 1):
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        if self._restricted and ns.id not in self.access.visible_namespace_ids():
            raise NotAuthorizedError(self.user_id or -1, "read this namespace's semantic model")
        return ns, host.namespace_path(ns, self.db)

    def _require_writable(self, ns: Any) -> None:
        if self._restricted and ns.id not in self.access.writable_namespace_ids():
            raise NotAuthorizedError(self.user_id or -1, "change this namespace's semantic model")

    def _catalog_of(self, ns: Any) -> Any:
        return ns if ns.level == 0 else self.repo.get_namespace(ns.parent_id)

    def _catalog_tables(self, catalog: Any) -> tuple[dict[int, TableInfo], dict[int, int]]:
        """Every table of the catalog's schemas, by id, and the schema id each lives in."""
        service = CatalogService(self.repo)
        tables: dict[int, TableInfo] = {}
        schema_of: dict[int, int] = {}
        for schema in self.repo.list_child_namespaces(catalog.id):
            for t in service.list_tables(schema.id):
                if not t.qualified_name or not host._keyable(t.qualified_name):
                    continue
                tables[t.id] = TableInfo(
                    table_id=t.id,
                    key=t.qualified_name,
                    name=t.name,
                    description=t.description,
                    columns=tuple((c.name, c.dtype) for c in t.schema_columns),
                )
                schema_of[t.id] = schema.id
        return tables, schema_of

    def _scope(self, namespace_id: int) -> tuple[Any, str, str, dict[int, TableInfo], dict[int, TableInfo], dict]:
        """The namespace, its path, its catalog's path, the model's tables, all the catalog's tables, schema ids."""
        ns, path = self._namespace(namespace_id)
        catalog = self._catalog_of(ns)
        catalog_path = host.namespace_path(catalog, self.db)
        all_tables, schema_of = self._catalog_tables(catalog)
        model_tables = all_tables if ns.level == 0 else {i: t for i, t in all_tables.items() if schema_of[i] == ns.id}
        return ns, path, catalog_path, model_tables, all_tables, schema_of

    def snapshot(self, namespace_id: int, *, accepted_only: bool = False, sync: bool = True) -> ModelSnapshot:
        """The model as plain dataclasses (``snapshot.py``), for this service and the metric compiler."""
        if sync:
            maybe_sync()
        ns, path, catalog_path, model_tables, all_tables, _ = self._scope(namespace_id)
        with _ledger_errors(self.user_id):
            return build_snapshot(
                self.view,
                namespace_id=ns.id,
                level=ns.level,
                path=path,
                tables=sorted(model_tables.values(), key=lambda t: t.key),
                catalog_path=catalog_path if ns.level == 1 else None,
                known_tables={t.key: t.table_id for t in all_tables.values()},
                accepted_only=accepted_only,
            )

    def can_manage(self, path: str) -> bool:
        from glosslayer.ids import path_covers

        if not host.host().sharing:
            return True
        attrs = self.view.context.attrs if self.view.context else {}
        return bool(attrs.get("admin")) or any(path_covers(p, path) for p in attrs.get("manage") or ())

    def get_model(self, namespace_id: int) -> s.SemanticModelOut:
        snap = self.snapshot(namespace_id)
        return self._model_out(snap)

    def _removable(self, snap: ModelSnapshot) -> dict[str, bool]:
        """Per metric and join subject of ``snap``: whether the policy lets this caller remove it (``remove``)."""
        subjects = {m.key for m in snap.metrics} | {j.key for j in snap.joins}
        ids: dict[str, list[str]] = {}
        for f in snap.facts:
            if f.subject in subjects:
                ids.setdefault(f.subject, []).append(f.id)
        if not ids:
            return {}
        try:
            with _ledger_errors(self.user_id):
                held = self.view.store.get_many({i for group in ids.values() for i in group})
                return {
                    subject: _may_withdraw(self.view, [held[i] for i in group if i in held])
                    for subject, group in ids.items()
                }
        except Exception:  # noqa: BLE001
            logger.warning("Semantic model: could not tell which facts this caller may remove", exc_info=True)
            return {}

    def _model_out(self, snap: ModelSnapshot) -> s.SemanticModelOut:
        removable = self._removable(snap)

        def value(attr: str) -> Any:
            f = snap.model_facts.get(attr)
            return f.value if f is not None else None

        tables = []
        for t in snap.tables:
            fields = [
                s.FieldOut(
                    name=c.name,
                    dtype=c.dtype,
                    facts={a: fact_out(f) for a, f in c.facts.items()},
                    pending=[fact_out(f) for f in c.pending],
                )
                for c in t.columns
            ]
            metrics = [
                s.MetricOut(
                    key=m.key,
                    name=m.name,
                    table=m.table,
                    table_id=m.table_id,
                    expression=m.expression,
                    description=m.value("description"),
                    aggregate=m.value("aggregate"),
                    filter=m.value("filter"),
                    time_column=m.value("time_column"),
                    dimensions=m.value("dimensions") or [],
                    format=m.value("format"),
                    status=m.status,
                    version=m.version,
                    approved_by=m.approved_by,
                    approved_at=m.approved_at,
                    group=m.group,
                    pending_group=m.pending_group,
                    can_remove=removable.get(m.key, False),
                )
                for m in t.metrics
            ]
            described = t.facts.get("description")
            tables.append(
                s.SemanticTableOut(
                    table_id=t.table_id,
                    key=t.key,
                    name=t.name,
                    description=described.value if described is not None else t.description,
                    fields=fields,
                    metrics=metrics,
                    facts={a: fact_out(f) for a, f in t.facts.items()},
                    pending=[fact_out(f) for f in t.pending],
                )
            )
        joins = [
            s.JoinOut(
                key=j.key,
                name=j.name,
                from_table=j.from_table,
                to_table=j.to_table,
                from_table_id=j.from_table_id,
                to_table_id=j.to_table_id,
                from_columns=j.from_columns,
                to_columns=j.to_columns,
                description=j.description,
                status=j.status,
                group=j.group,
                layer=j.layer,
                can_remove=removable.get(j.key, False),
            )
            for j in snap.joins
        ]
        return s.SemanticModelOut(
            namespace_id=snap.namespace_id,
            level=snap.level,
            key=snap.path,
            name=value("name"),
            description=value("description"),
            ai_instructions=value("ai_instructions"),
            tables=tables,
            joins=joins,
            progress=s.SemanticProgress(**snap.progress()),
            can_manage=self.can_manage(snap.path),
            review_mode="team" if host.host().sharing else "solo",
        )

    def update_model(self, namespace_id: int, body: s.SemanticModelUpdate) -> s.SemanticModelOut:
        from glosslayer.ledger import Proposal

        ns, path = self._namespace(namespace_id)
        self._require_writable(ns)
        subject = f"model:{path}"
        props = [
            Proposal(subject, attr, getattr(body, attr))
            for attr in ("name", "description", "ai_instructions")
            if getattr(body, attr) is not None
        ]
        if props:
            with _ledger_errors(self.user_id):
                self.view.propose_many(props)
        return self.get_model(namespace_id)

    def propose(
        self,
        namespace_id: int,
        proposals: list[Any],
        *,
        ai_source: str | None = None,
        comment: str | None = None,
    ) -> list[s.FactOut]:
        """Check every proposal against the catalog, then store them in one ledger call.

        With ``ai_source`` (``ai:<provider>/<model>``) the facts are stated by that actor through a view bound with
        that ``via``, so glosslayer queues them for review and never lets the AI accept; the checks are the same as
        for a person's proposal. ``comment`` is added to each fact's rationale (a join keeps its measurement first).

        A person's blank value for a column or table attribute (an emptied description, "Not set") removes what is
        there instead of storing an empty value: its live facts are taken away (``_withdraw``), and are not in the
        answer. Likewise a person's metric edit that is accepted at once (solo) and leaves out an optional part (a
        cleared filter or description) retracts that part's accepted value; a queued edit keeps it until decided.
        """
        from glosslayer.ids import SubjectId
        from glosslayer.ledger import Proposal
        from glosslayer.model import is_structural
        from glosslayer.modeling import Metric, Relationship

        ns, path, catalog_path, model_tables, all_tables, schema_of = self._scope(namespace_id)
        self._require_writable(ns)

        def model_table(table_id: int) -> TableInfo:
            t = model_tables.get(table_id)
            if t is None:
                raise SemanticValueError(f"Table id={table_id} is not part of this semantic model")
            return t

        has_metric = any(p.kind == "metric" for p in proposals)
        existing = self._metric_names(path) if has_metric else {}
        # Live joins, so a metric over a join still waiting for review can be proposed beside it.
        metric_snapshot = self._query_snapshot(namespace_id, preview=True)[1] if has_metric else None
        props: list[Any] = []
        cleared: list[tuple[str, str]] = []
        metric_edits: list[tuple[str, set[str]]] = []
        with _ledger_errors(self.user_id):
            for p in proposals:
                if p.kind in ("field", "table"):
                    t = model_table(p.table_id)
                    if p.kind == "field":
                        if not t.has_column(p.column):
                            raise SemanticValueError(f"Table {t.name!r} has no column {p.column!r}")
                        sid = SubjectId.for_field(t.key, p.column)
                    else:
                        sid = SubjectId.for_dataset(t.key)
                    if is_structural(sid, p.attribute):
                        raise SemanticValueError(f"{p.attribute!r} is read from the catalog and cannot be proposed")
                    if _blank(p.value):
                        if not ai_source:
                            cleared.append((str(sid), p.attribute))
                        continue
                    props.append(Proposal(str(sid), p.attribute, p.value))
                elif p.kind == "join":
                    join_props = self._join_proposals(
                        p, ns, path, catalog_path, model_tables, all_tables, schema_of, Relationship
                    )
                    rationale = self._check_new_join(p, all_tables)
                    for jp in join_props:
                        jp.comment = rationale
                    props += join_props
                else:
                    t = model_table(p.table_id)
                    owner = existing.get(p.name)
                    if owner is not None and not p.edit:
                        raise SemanticConflictError(f"A metric called {p.name!r} already exists in this model")
                    if owner is not None and owner != t.key:
                        raise SemanticConflictError(f"Metric {p.name!r} belongs to {owner}, not {t.key}")
                    if p.time_column and not t.has_column(p.time_column):
                        raise SemanticValueError(f"Table {t.name!r} has no column {p.time_column!r}")
                    validate_metric(t, p, execute=self._dry_run, snapshot=metric_snapshot)
                    metric = Metric(
                        p.name,
                        p.expression,
                        description=_clean(p.description),
                        aggregate=_clean(p.aggregate),
                        filter=_clean(p.filter),
                        time_column=_clean(p.time_column),
                        dimensions=list(p.dimensions or []),
                        format=_clean(p.format),
                        table=t.key,
                    )
                    metric_props = list(metric.proposals())
                    if p.edit and not ai_source:
                        metric_edits.append((str(metric.subject), {mp.attribute for mp in metric_props}))
                    props += metric_props
            if comment:
                for prop in props:
                    prop.comment = f"{prop.comment} {comment}" if getattr(prop, "comment", None) else comment
            if ai_source:
                for prop in props:
                    prop.confidence = 0.6
                view = host.view_for(self.user, self.access, via=ai_source)
                results = view.propose_many(props, source=ai_source)
            else:
                view = self.view
                results = view.propose_many(props) if props else []
                gone = self._cleared_facts(view, cleared, metric_edits, results)
                if gone:
                    _withdraw(view, gone, "cleared")
        facts = list({r.assertion.id: fact_from_assertion(r.assertion) for r in results}.values())
        annotate(view.store, facts)
        return [fact_out(f) for f in facts]

    @staticmethod
    def _cleared_facts(
        view: Any, cleared: list[tuple[str, str]], metric_edits: list[tuple[str, set[str]]], results: list[Any]
    ) -> list[Any]:
        """The live facts a person's blank values and accepted metric edits take away (``propose``)."""
        gone: dict[str, Any] = {}
        for subject, attribute in cleared:
            for a in view.find(subject=subject, attribute=attribute, statuses=list(LIVE)):
                gone[a.id] = a
        for subject, kept in metric_edits:
            mine = [r.assertion for r in results if r.assertion.subject == subject]
            if not mine or any(a.status.value != ACCEPTED for a in mine):
                continue
            for a in view.find(subject=subject, statuses=[ACCEPTED]):
                if a.attribute in _METRIC_OPTIONAL and a.attribute not in kept:
                    gone[a.id] = a
        return [a for a in gone.values() if not a.redacted]

    def _join_proposals(self, p, ns, path, catalog_path, model_tables, all_tables, schema_of, Relationship) -> list:
        """A join's facts, owned by the schema when both ends are in it, else by the catalog."""
        ends = []
        for table_id in (p.from_table_id, p.to_table_id):
            t = all_tables.get(table_id)
            if t is None:
                raise SemanticValueError(f"Table id={table_id} is not in this catalog; a join cannot cross catalogs")
            ends.append(t)
        if ns.level == 1 and not any(t.table_id in model_tables for t in ends):
            raise SemanticValueError("A join in a schema's model must touch one of its tables")
        if not p.from_columns or len(p.from_columns) != len(p.to_columns):
            raise SemanticValueError("A join needs the same number of columns on both sides, at least one")
        for t, cols in zip(ends, (p.from_columns, p.to_columns), strict=True):
            missing = [c for c in cols if not t.has_column(c)]
            if missing:
                raise SemanticValueError(f"Table {t.name!r} has no column {missing[0]!r}")
        if ends[0].table_id == ends[1].table_id and list(p.from_columns) == list(p.to_columns):
            raise SemanticValueError(
                f"A join of {ends[0].name} to itself needs different columns on each side, such as a parent "
                f"column pointing at the table's key; the same column on both sides joins every row to itself"
            )
        from_schema, to_schema = schema_of[ends[0].table_id], schema_of[ends[1].table_id]
        if from_schema == to_schema:
            layer = path if ns.level == 1 else host.namespace_path(self.repo.get_namespace(from_schema), self.db)
        else:
            layer = catalog_path
        rel = Relationship(
            p.name,
            ends[0].key,
            ends[1].key,
            from_columns=list(p.from_columns),
            to_columns=list(p.to_columns),
            description=_clean(p.description),
            namespace=layer,
        )
        return list(rel.proposals())

    def _check_new_join(self, p: s.JoinProposal, all_tables: dict[int, TableInfo]) -> str:
        """Measure a proposed join's two sides; refuse a reversed or many-to-many one, else answer its evidence."""
        from flowfile_core.catalog.semantic.join_check import check_join

        found = check_join(
            all_tables[p.from_table_id],
            list(p.from_columns),
            all_tables[p.to_table_id],
            list(p.to_columns),
            self._dry_run,
        )
        if found.verdict in ("reversed", "many_to_many"):
            raise SemanticValueError(found.message)
        if found.verdict == "unchecked":
            logger.warning("Semantic model: join %r stored unchecked: %s", p.name, found.message)
            return found.message
        return f"Checked: {found.message}"

    def check_joins(self, namespace_id: int) -> list[s.JoinCheckOut]:
        """Every join of the model (accepted and pending, both layers) measured the way a new join is."""
        from flowfile_core.catalog.semantic.join_check import JoinCheck, check_join

        snap = self.snapshot(namespace_id)
        _ns, _path, _cp, _model, all_tables, _schema_of = self._scope(namespace_id)
        by_key = {t.key: t for t in all_tables.values()}
        out = []
        for j in snap.joins:
            f = all_tables.get(j.from_table_id) if j.from_table_id is not None else by_key.get(j.from_table)
            t = all_tables.get(j.to_table_id) if j.to_table_id is not None else by_key.get(j.to_table)
            if f is None or t is None or not j.from_columns or len(j.from_columns) != len(j.to_columns):
                found = JoinCheck("unchecked", "Direction not checked: a table or a column of this join is missing.")
            elif any(not f.has_column(c) for c in j.from_columns) or any(not t.has_column(c) for c in j.to_columns):
                found = JoinCheck("unchecked", "Direction not checked: a column of this join is no longer there.")
            else:
                try:
                    found = check_join(f, list(j.from_columns), t, list(j.to_columns), self._dry_run)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Semantic model: checking join %r failed", j.name, exc_info=True)
                    found = JoinCheck("unchecked", f"Direction not checked: {exc}")
            out.append(s.JoinCheckOut(key=j.key, name=j.name, verdict=found.verdict, message=found.message))
        return out

    def _metric_names(self, path: str) -> dict[str, str]:
        """Live metric names under ``path`` and the dataset each belongs to."""
        from glosslayer.ids import SubjectId

        names: dict[str, str] = {}
        for a in self.view.find(namespace=path, kind="metric", statuses=list(LIVE)):
            sid = SubjectId.parse(a.subject)
            if sid.name and sid.dataset:
                names.setdefault(sid.name, sid.dataset)
        return names

    def review_queue(self, namespace_id: int) -> list[s.ReviewItemOut]:
        """Open facts of the model, one entry per fact, one per group for a join or a metric."""
        from glosslayer.ids import Kind, SubjectId

        ns, path = self._namespace(namespace_id)
        catalog = self._catalog_of(ns)
        all_tables, _ = self._catalog_tables(catalog)
        ids = {t.key: t for t in all_tables.values()}
        with _ledger_errors(self.user_id):
            facts = [fact_from_assertion(a) for a in self.view.pending(namespace=path) if not a.redacted]
            annotate(self.view.store, facts)
        groups: dict[str, list[FactSnap]] = {}
        order: list[tuple[str, Any]] = []
        for f in sorted(facts, key=lambda f: f.updated_at):
            kind = f.subject.split(":", 1)[0]
            if kind in GROUPED_KINDS and f.group:
                if f.group not in groups:
                    order.append(("group", f.group))
                groups.setdefault(f.group, []).append(f)
            else:
                order.append(("fact", f))
        items = []
        for what, ref in order:
            rows = groups[ref] if what == "group" else [ref]
            head = rows[0]
            sid = SubjectId.parse(head.subject)
            table = ids.get(sid.dataset) if sid.dataset else None
            if sid.kind is Kind.FIELD:
                label = f"{table.name if table else sid.dataset}.{sid.name}"
            elif sid.kind is Kind.DATASET:
                label = table.name if table else (sid.dataset or "")
            elif sid.kind is Kind.MODEL:
                label = sid.namespace or "model"
            else:
                label = sid.name or head.subject
            items.append(
                s.ReviewItemOut(
                    **fact_out(head).model_dump(),
                    subject=head.subject,
                    subject_kind=sid.kind.value,
                    subject_label=label,
                    table=sid.dataset,
                    table_id=table.table_id if table else None,
                    column=sid.name if sid.kind is Kind.FIELD else None,
                    group_facts=[fact_out(f) for f in rows] if what == "group" else [],
                )
            )
        return items

    def review(self, namespace_id: int, action: s.ReviewAction) -> s.ReviewResultOut:
        from glosslayer.ids import path_covers

        if action.subject:
            return self.remove(namespace_id, action.subject, comment=action.comment, action=action.action)
        ns, path = self._namespace(namespace_id)
        if not action.ids and not action.group:
            raise SemanticValueError("Give the ids or the group to decide")
        view = self.view
        with _ledger_errors(self.user_id):
            if action.group:
                targets = view.find(group=action.group)
                if action.action == "retract":
                    # a group's superseded or rejected facts are history, not something to retract
                    targets = [a for a in targets if a.status.value in LIVE]
            else:
                targets = [view.get(i) for i in action.ids]
            if not targets:
                raise SemanticNotFoundError(f"No facts in group {action.group!r}")
            for a in targets:
                if not path_covers(path, a.scope_path):
                    raise SemanticNotFoundError(f"No fact {a.id!r} in this semantic model")
            if action.group and action.action in ("accept", "reject"):
                decide = view.accept_group if action.action == "accept" else view.reject_group
                changed, errors = decide(action.group, comment=action.comment), []
            else:
                changed, errors = view.transition_many(action.action, [a.id for a in targets], comment=action.comment)
        if errors and not changed:
            raise _translate(self.user_id, errors[0][1])
        facts = [fact_from_assertion(a) for a in changed]
        annotate(view.store, facts)
        return s.ReviewResultOut(
            changed=[fact_out(f) for f in facts],
            errors=[s.ReviewError(id=aid, message=str(exc)) for aid, exc in errors],
        )

    def remove(
        self, namespace_id: int, subject: str, *, comment: str | None = None, action: str = "retract"
    ) -> s.ReviewResultOut:
        """Remove a metric or a join: every live fact of ``subject`` taken away in one transaction (``_withdraw``).

        Append-only: each fact moves to retracted (or rejected, for someone else's open proposal) and the history
        keeps it, so the same name can be proposed again. Whether the caller may is glosslayer's policy; a refusal
        undoes the whole removal and comes back as the policy's sentence (403). A catalog-layer join that touches
        this schema can be removed from the schema's model too, as its history can be read there.
        """
        from glosslayer.ids import Kind

        if action != "retract":
            raise SemanticValueError("A metric or a join is removed with the retract action")
        with _ledger_errors(self.user_id):
            sid = self._subject_in_model(namespace_id, subject)
            if sid.kind not in (Kind.METRIC, Kind.RELATIONSHIP):
                raise SemanticValueError("Only a metric or a join is removed whole; clear a column's value instead")
            live = [a for a in self.view.find(subject=str(sid), statuses=list(LIVE)) if not a.redacted]
            if not live:
                raise SemanticNotFoundError(f"{subject!r} holds nothing to remove")
            changed = _withdraw(self.view, live, comment or "removed")
        facts = [fact_from_assertion(a) for a in changed]
        annotate(self.view.store, facts)
        return s.ReviewResultOut(changed=[fact_out(f) for f in facts])

    def _subject_in_model(self, namespace_id: int, subject: str) -> Any:
        """``subject`` parsed, when it belongs to this namespace's model (a catalog join touching a schema counts)."""
        from glosslayer.ids import Kind, SubjectId, path_covers

        ns, path = self._namespace(namespace_id)
        sid = SubjectId.parse(subject)
        scope = sid.scope_path
        catalog_join = ns.level == 1 and sid.kind is Kind.RELATIONSHIP and scope == path.rsplit(".", 1)[0]
        if not (path_covers(path, scope) or catalog_join):
            raise SemanticNotFoundError(f"{subject!r} is not part of this semantic model")
        return sid

    def join_dependents(self, namespace_id: int, key: str) -> list[s.JoinDependentOut]:
        """The accepted metrics that stop compiling without the join ``key``; nothing is read from the data.

        Each metric is compiled against the published, catalog-wide snapshot a query uses, with and without the
        join: plain (its formula and stored filter), by its time grain, and by each allowed dimension. What compiles
        with the join and not without it is what the join is needed for. A metric that may be grouped by any column
        loses only the joined columns it could have been grouped by, so it is not listed.
        """
        from dataclasses import replace

        live = self.snapshot(namespace_id)
        if not any(j.key == key for j in live.joins):
            raise SemanticNotFoundError(f"No join {key!r} in this semantic model")
        _ns, snap, _tables = self._query_snapshot(namespace_id, preview=False)
        if not any(j.key == key for j in snap.joins):
            return []
        without = replace(snap, joins=[j for j in snap.joins if j.key != key])
        out = []
        for m in snap.metrics:
            cases: list[tuple[str, s.MetricQuery]] = [("formula", s.MetricQuery(metric=m.name))]
            if m.value("time_column"):
                cases.append(("time column", s.MetricQuery(metric=m.name, grain="month")))
            for d in m.value("dimensions") or []:
                if isinstance(d, str):
                    cases.append((f"dimension {d}", s.MetricQuery(metric=m.name, dimensions=[d])))
            uses, message = [], ""
            for what, query in cases:
                try:
                    compile_metric_query(snap, query, metric_tables={m.table})
                except ValueError:
                    continue
                try:
                    compile_metric_query(without, query, metric_tables={m.table})
                except ValueError as exc:
                    uses.append(what)
                    message = message or str(exc)
            if uses:
                out.append(
                    s.JoinDependentOut(
                        metric=m.name, metric_key=m.key, table=m.table, table_id=m.table_id, uses=uses, message=message
                    )
                )
        return out

    def history(self, namespace_id: int, subject: str) -> list[s.SemanticEventOut]:
        from glosslayer.ids import SubjectId

        with _ledger_errors(self.user_id):
            sid = SubjectId.parse(subject)
            self._subject_in_model(namespace_id, subject)
            events = self.view.history(sid)
        known = self.view.store.get_many({e.assertion_id for e in events}) if events else {}
        out = []
        for e in events:
            a = known.get(e.assertion_id)
            out.append(
                s.SemanticEventOut(
                    seq=e.seq,
                    id=e.id,
                    ts=e.ts,
                    type=e.type.value,
                    assertion_id=e.assertion_id,
                    attribute=a.attribute if a is not None else e.data.get("attribute"),
                    actor=e.actor,
                    via=e.via,
                    comment=e.data.get("comment") or e.data.get("reason"),
                    value=e.data.get("value"),
                )
            )
        return out

    def sync(self, namespace_id: int) -> s.SemanticSyncOut:
        self._namespace(namespace_id)
        report = maybe_sync(force=True)
        return s.SemanticSyncOut(
            new=list(report.new),
            changed=list(report.changed),
            moved=dict(report.moved),
            gone=list(report.gone),
            proposed=report.proposed,
            accepted=report.accepted,
            staled=report.staled,
        )

    def export(self, namespace_id: int) -> str:
        """The namespace's accepted model as Ossie YAML."""
        _ns, path = self._namespace(namespace_id)
        with _ledger_errors(self.user_id):
            return self.view.to_yaml(namespace=path)

    def suggest_candidates(
        self, namespace_id: int, table_ids: list[int] | None = None
    ) -> tuple[Any, dict[int, Any], str | None]:
        """Rule candidates for the model's tables (``suggest.py``), built in memory; nothing is stored here.

        Returns the ``Suggestions``, the model's tables by id and, when statistics fell back, why.
        """
        from flowfile_core.catalog.semantic.suggest import collect_stats, suggest

        _ns, _path, _cp, model_tables, _all, _schema_of = self._scope(namespace_id)
        tables = [t for t in sorted(model_tables.values(), key=lambda t: t.key)]
        if table_ids:
            wanted = set(table_ids)
            unknown = wanted - set(model_tables)
            if unknown:
                raise SemanticValueError(f"Table id={min(unknown)} is not part of this semantic model")
        stats, problem = collect_stats(tables, self._stats_query)
        sugg = suggest(stats)
        if table_ids:
            keep = set(table_ids)
            sugg.roles = [r for r in sugg.roles if r.table_id in keep]
            sugg.joins = [j for j in sugg.joins if j.from_table_id in keep or j.to_table_id in keep]
            sugg.metrics = [m for m in sugg.metrics if m.table_id in keep]
        sugg.stats = {st.table.table_id: st for st in stats}
        return sugg, model_tables, problem

    def suggest_scope(self, namespace_id: int) -> tuple[ModelSnapshot, dict[int, TableInfo], dict[int, TableInfo]]:
        """The live model (pending facts included), its own tables and every table of its catalog, by id."""
        snap = self.snapshot(namespace_id)
        _ns, _path, _cp, model_tables, all_tables, _schema_of = self._scope(namespace_id)
        return snap, model_tables, all_tables

    def measure_join(self, from_table: TableInfo, from_columns: list[str], to_table: TableInfo, to_columns: list[str]):
        """A join's two sides measured as a new join is (``join_check.check_join``), without storing anything."""
        from flowfile_core.catalog.semantic.join_check import check_join

        return check_join(from_table, list(from_columns), to_table, list(to_columns), self._dry_run)

    def _stats_query(self, sql: str) -> Any:
        return self._catalog_service().execute_sql_query(sql, max_rows=1, user_id=self.user_id)

    def suggest(self, namespace_id: int, kinds: list[str] | None = None) -> s.SuggestOut:
        """Rule-based proposals (roles, joins, metrics) stored with an inferred source, so they queue for review.

        ``kinds`` (``join``, ``metric``, ``description``, ``role``) stores only those; the rules still run whole,
        since a metric's dimensions come from the joins found. The rules write no descriptions.
        """
        sugg, model_tables, problem = self.suggest_candidates(namespace_id)
        if kinds is not None:
            wanted = set(kinds)
            sugg.roles = sugg.roles if "role" in wanted else []
            sugg.joins = sugg.joins if "join" in wanted else []
            sugg.metrics = sugg.metrics if "metric" in wanted else []
        out = self.store_suggestions(namespace_id, sugg)
        out.tables = len(model_tables)
        out.stats_available = sugg.stats_available and problem is None
        out.stats_message = problem
        return out

    def store_suggestions(
        self,
        namespace_id: int,
        sugg: Any,
        *,
        words: dict[int, dict[str, Any]] | None = None,
        ai_source: str | None = None,
    ) -> s.SuggestOut:
        """Store ``sugg`` as proposals, skipping what the model already holds; ``words`` are an AI's descriptions.

        Roles and joins keep the rules' inferred source. With ``words`` (per table id: ``description``, ``columns``
        {column: sentence}, ``metrics`` {rule name: (name, meaning)}) the descriptions and the renamed metrics are
        stated by ``ai_source`` through a view bound with that ``via``, so glosslayer queues them and never lets the
        AI accept. Each proposal carries its evidence as the rationale (the ledger's comment) and a confidence.
        """
        from glosslayer.ids import SubjectId
        from glosslayer.ledger import Proposal
        from glosslayer.modeling import Metric

        from flowfile_core.catalog.semantic.suggest import RULE_SOURCE

        ns, path, catalog_path, model_tables, all_tables, schema_of = self._scope(namespace_id)
        self._require_writable(ns)
        snap = self.snapshot(namespace_id, sync=False)
        words = words or {}
        out = s.SuggestOut(namespace_id=namespace_id)

        def created(results: list[Any]) -> int:
            return sum(1 for r in results if r.outcome == "created")

        def held(results: list[Any]) -> int:
            return sum(1 for r in results if r.outcome != "created")

        def tag(props: list[Any], rationale: str, confidence: float) -> list[Any]:
            for p in props:
                p.comment, p.confidence = rationale, round(confidence, 2)
            return props

        tables_by_id = {t.table_id: t for t in snap.tables}
        role_props = []
        for r in sugg.roles:
            ts = tables_by_id.get(r.table_id)
            col = ts.column(r.column) if ts else None
            if ts is None or col is None or col.facts.get("role") is not None:
                out.skipped.roles += 1
                continue
            sid = SubjectId.for_field(ts.key, r.column)
            role_props.append(Proposal(str(sid), "role", r.role, r.confidence, [], r.rationale))

        existing_joins = {(j.from_table, j.to_table, tuple(j.from_columns), tuple(j.to_columns)) for j in snap.joins}
        join_props: list[list[Any]] = []
        for j in sugg.joins:
            f, t = all_tables.get(j.from_table_id), all_tables.get(j.to_table_id)
            if f is None or t is None or (f.key, t.key, tuple(j.from_columns), tuple(j.to_columns)) in existing_joins:
                out.skipped.joins += 1
                continue
            from glosslayer.modeling import Relationship

            body = s.JoinProposal(
                kind="join",
                name=j.name,
                from_table_id=j.from_table_id,
                to_table_id=j.to_table_id,
                from_columns=j.from_columns,
                to_columns=j.to_columns,
                description=j.description,
            )
            props = self._join_proposals(
                body, ns, path, catalog_path, model_tables, all_tables, schema_of, Relationship
            )
            join_props.append(tag(props, j.rationale, j.confidence))

        names = self._metric_names(path)
        exprs = {(m.table, (m.expression or "").replace(" ", "").lower()) for m in snap.metrics}
        rule_metrics: list[list[Any]] = []
        ai_metrics: list[list[Any]] = []
        held_metrics = {(x.table, (x.expression or "").replace(" ", "").lower()): x for x in snap.metrics}
        word_props: list[Any] = []
        for m in sugg.metrics:
            t = model_tables.get(m.table_id)
            better = (words.get(m.table_id) or {}).get("metrics", {}).get(m.name)
            if t is None or (t.key, m.expression.replace(" ", "").lower()) in exprs:
                out.skipped.metrics += 1
                held_metric = held_metrics.get((t.key, m.expression.replace(" ", "").lower())) if t else None
                if better and ai_source and held_metric is not None:
                    word_props += _metric_word_proposals(held_metric, better, m.description, SubjectId, Proposal)
                continue
            name, description, by_ai = m.name, m.description, False
            if better and ai_source:
                name, description, by_ai = better[0] or m.name, better[1] or m.description, True
            if name in names:
                if not by_ai or m.name in names:
                    out.skipped.metrics += 1
                    continue
                name, description, by_ai = m.name, m.description, False
            proposal = s.MetricProposal(
                kind="metric",
                table_id=t.table_id,
                name=name,
                expression=m.expression,
                description=description,
                aggregate=m.aggregate,
                time_column=m.time_column,
                dimensions=m.dimensions or None,
            )
            try:
                validate_metric(t, proposal)
            except SemanticValueError:
                out.skipped.metrics += 1
                continue
            names[name] = t.key
            metric = Metric(
                name,
                m.expression,
                description=description,
                aggregate=m.aggregate,
                time_column=m.time_column,
                dimensions=list(m.dimensions or []),
                table=t.key,
            )
            rationale = m.rationale + (" Name and meaning written by AI." if by_ai else "")
            (ai_metrics if by_ai else rule_metrics).append(tag(list(metric.proposals()), rationale, m.confidence))

        desc_props = []
        if ai_source:
            for table_id, w in words.items():
                ts = tables_by_id.get(table_id)
                if ts is None:
                    continue
                text = (w.get("description") or "").strip()
                if text and ts.facts.get("description") is None:
                    desc_props.append(
                        Proposal(str(SubjectId.for_dataset(ts.key)), "description", text, 0.6, [], "Written by AI.")
                    )
                for column, sentence in (w.get("columns") or {}).items():
                    col = ts.column(column)
                    sentence = (sentence or "").strip()
                    if col is None or not sentence:
                        continue
                    if col.facts.get("description") is not None:
                        out.skipped.descriptions += 1
                        continue
                    desc_props.append(
                        Proposal(
                            str(SubjectId.for_field(ts.key, column)), "description", sentence, 0.6, [], "Written by AI."
                        )
                    )

        with _ledger_errors(self.user_id):
            if role_props:
                res = self.view.propose_many(role_props, source=RULE_SOURCE)
                out.added.roles += created(res)
                out.skipped.roles += held(res)
            for props in join_props:
                res = self.view.propose_many(props, source=RULE_SOURCE)
                out.added.joins += 1 if created(res) else 0
                out.skipped.joins += 0 if created(res) else 1
            for props in rule_metrics:
                res = self.view.propose_many(props, source=RULE_SOURCE)
                out.added.metrics += 1 if created(res) else 0
                out.skipped.metrics += 0 if created(res) else 1
            if ai_source and (desc_props or ai_metrics or word_props):
                ai_view = host.view_for(self.user, self.access, via=ai_source)
                if word_props:
                    res = ai_view.propose_many(word_props, source=ai_source)
                    out.added.descriptions += created(res)
                    out.skipped.descriptions += held(res)
                if desc_props:
                    res = ai_view.propose_many(desc_props, source=ai_source)
                    out.added.descriptions += created(res)
                    out.skipped.descriptions += held(res)
                for props in ai_metrics:
                    res = ai_view.propose_many(props, source=ai_source)
                    out.added.metrics += 1 if created(res) else 0
                    out.skipped.metrics += 0 if created(res) else 1
        return out

    def known_values(self, columns: list[tuple[str, str]], cap: int = 50) -> dict[tuple[str, str], list[str]]:
        """The distinct values of ``(table key, column)`` pairs, read with **one** SQL statement through the catalog
        SQL path, so the worker computes and the caller's table access applies. A column with more than ``cap``
        values is left out (it is not a category). Any failure answers ``{}``: the caller carries on without them.
        """
        from flowfile_core.catalog.semantic.compiler import quote_ident, sql_literal

        pairs = list(dict.fromkeys(columns))
        if not pairs:
            return {}
        parts = [
            f"SELECT DISTINCT {sql_literal(str(i))} AS k, CAST({quote_ident(col)} AS VARCHAR) AS v "
            f"FROM {quote_ident(table)}"
            for i, (table, col) in enumerate(pairs)
        ]
        try:
            result = self._catalog_service().execute_sql_query(
                " UNION ALL ".join(parts), max_rows=(cap + 1) * len(pairs), user_id=self.user_id
            )
        except Exception:  # noqa: BLE001
            logger.warning("semantic known values could not be read", exc_info=True)
            return {}
        if result.error:
            logger.warning("semantic known values could not be read: %s", result.error)
            return {}
        out: dict[tuple[str, str], list[str]] = {}
        for k, v in result.rows:
            if v not in (None, ""):
                out.setdefault(pairs[int(k)], []).append(str(v))
        return {key: sorted(vals) for key, vals in out.items() if len(vals) <= cap}

    def _catalog_service(self) -> CatalogService:
        """The catalog service as the catalog router builds it, so table access is enforced the same way."""
        return CatalogService(self.repo, access=self.access)

    def _dry_run(self, sql: str) -> Any:
        return self._catalog_service().execute_sql_query(sql, max_rows=1, user_id=self.user_id)

    def _query_snapshot(self, namespace_id: int, preview: bool) -> tuple[Any, ModelSnapshot, dict[int, TableInfo]]:
        """The snapshot a metric query compiles from, the namespace, and the model's own tables.

        It covers the whole catalog (every schema's joins, so a query can cross schemas over several hops), with
        accepted facts only unless ``preview``, which needs the namespace writable. When the caller cannot read at
        the catalog's level the schema's own snapshot is used, which still holds the catalog-level joins touching it.
        """
        from glosslayer.policy import PermissionDenied

        ns, path, catalog_path, model_tables, all_tables, _ = self._scope(namespace_id)
        if preview:
            self._require_writable(ns)
        maybe_sync()
        catalog = self._catalog_of(ns)
        kwargs = {"known_tables": {t.key: t.table_id for t in all_tables.values()}, "accepted_only": not preview}
        try:
            snap = build_snapshot(
                self.view,
                namespace_id=catalog.id,
                level=0,
                path=catalog_path,
                tables=sorted(all_tables.values(), key=lambda t: t.key),
                **kwargs,
            )
        except PermissionDenied:
            with _ledger_errors(self.user_id):
                snap = build_snapshot(
                    self.view,
                    namespace_id=ns.id,
                    level=ns.level,
                    path=path,
                    tables=sorted(model_tables.values(), key=lambda t: t.key),
                    catalog_path=catalog_path if ns.level == 1 else None,
                    **kwargs,
                )
        snap.related_tables = {t.key: t for t in all_tables.values()}
        return ns, snap, model_tables

    def _compile(
        self, namespace_id: int, query: s.MetricQuery, table_names: dict[str, str] | None = None
    ) -> tuple[Any, CompiledQuery, dict[int, TableInfo]]:
        ns, snap, model_tables = self._query_snapshot(namespace_id, query.preview)
        try:
            compiled = compile_metric_query(
                snap, query, metric_tables={t.key for t in model_tables.values()}, table_names=table_names
            )
        except ValueError as exc:
            raise SemanticValueError(str(exc)) from None
        return ns, compiled, model_tables

    def query_metric(self, namespace_id: int, query: s.MetricQuery) -> s.MetricQueryResult:
        """Compile the query to one SQL statement and run it through the catalog SQL path (the worker computes).

        A worker that stopped after its handshake raises a plain ``requests`` error, not a ``ComputeError``; both
        come back as the result's ``error``, never a 500.
        """
        from requests import RequestException

        from flowfile_core.flowfile.execution.errors import ComputeError

        _ns, compiled, _ = self._compile(namespace_id, query)
        m = compiled.metric
        metric = s.MetricQueryMetric(
            name=compiled.metric_name, version=compiled.metric_version, status=m.status, approved_by=m.approved_by
        )
        try:
            result = self._catalog_service().execute_sql_query(compiled.sql, max_rows=query.limit, user_id=self.user_id)
        except ComputeError as exc:
            return s.MetricQueryResult(sql=compiled.sql, metric=metric, error=str(exc))
        except RequestException:
            return s.MetricQueryResult(
                sql=compiled.sql, metric=metric, error="The compute worker could not be reached. Is it running?"
            )
        return s.MetricQueryResult(
            columns=result.columns,
            dtypes=result.dtypes,
            rows=result.rows,
            total_rows=result.total_rows,
            truncated=result.truncated,
            sql=compiled.sql,
            metric=metric,
            error=result.error,
        )

    def save_metric_query_as_flow(self, namespace_id: int, body: s.MetricQuerySaveAsFlow) -> s.MetricQuerySaveAsFlowOut:
        """Save the compiled query as a flow: one catalog reader per table into a SQL node.

        The SQL node names its inputs ``input_1``, ``input_2`` … in reader order, so the query is compiled a second
        time with those names. ``save_sql_query_as_flow`` finds each reader's table by name in the flow's schema,
        so every table the query reads must live in that schema.
        """
        ns, compiled, _ = self._compile(namespace_id, body.query)
        target = body.namespace_id if body.namespace_id is not None else (ns.id if ns.level == 1 else None)
        if target is None:
            raise SemanticValueError("Choose the schema to save the flow in")
        by_key = {t.key: t for t in self._catalog_tables(self._catalog_of(ns))[0].values()}
        names: list[str] = []
        for key in compiled.tables:
            info = by_key.get(key)
            found = self.repo.get_table_by_name(info.name, target) if info is not None else None
            if found is None or found.id != info.table_id:
                raise SemanticValueError(
                    f"Saving as a flow reads tables of the flow's schema only; {key!r} is in another schema"
                )
            names.append(info.name)
        renamed = {key: f"input_{i + 1}" for i, key in enumerate(compiled.tables)}
        _ns, for_flow, _ = self._compile(namespace_id, body.query, table_names=renamed)
        flow_id = self._catalog_service().save_sql_query_as_flow(
            query=for_flow.sql,
            name=body.name,
            owner_id=self.user_id,
            namespace_id=target,
            description=body.description,
            used_tables=names,
        )
        return s.MetricQuerySaveAsFlowOut(flow_id=flow_id, sql=for_flow.sql)


def tree_summaries(tree: list[Any]) -> None:
    """Fill ``semantic_model`` on every local node of a namespace tree from two ledger queries for the whole tree.

    Counts are of everything at or under a namespace's path; facts the catalog itself accepted are left out, as in
    ``ModelSnapshot.progress``. A table gets the counts at its own key (``catalog.schema.table``), which is the scope
    of its column facts and its own metrics. Any failure (glosslayer missing, a ledger error) leaves the field ``None``.
    """
    try:
        from glosslayer.model import Status
        from glosslayer.store.base import Query

        store = host.host().gloss.store
        counts = store.count_by_status(Query())
        catalog_accepted = store.count_by_status(Query(source_prefix="catalog:", statuses=[Status.ACCEPTED]))
    except Exception:
        logger.debug("Semantic ledger: no counts for the namespace tree", exc_info=True)
        return
    totals: dict[str, dict[str, int]] = {}
    for (scope, status), n in counts.items():
        if not scope:
            continue
        if status.value == ACCEPTED:
            n -= catalog_accepted.get((scope, status), 0)
            bucket = "accepted"
        elif status.value == "stale":
            bucket = "stale"
        elif status.value in ("proposed", "reviewed"):
            bucket = "pending"
        else:
            continue
        parts = scope.split(".")
        for i in range(1, len(parts) + 1):
            entry = totals.setdefault(".".join(parts[:i]), {"accepted": 0, "pending": 0, "stale": 0})
            entry[bucket] += n

    def walk(nodes: list[Any], parent: str | None) -> None:
        for node in nodes:
            if getattr(node, "id", 0) is None or node.id < 0:
                continue
            node_path = f"{parent}.{node.name}" if parent else node.name
            node.semantic_model = s.SemanticModelSummary(**totals.get(node_path, {}))
            for table in getattr(node, "tables", None) or []:
                key = getattr(table, "qualified_name", None) or f"{node_path}.{table.name}"
                table.semantic_model = s.SemanticModelSummary(**totals.get(key, {}))
            walk(node.children, node_path)

    walk(tree, None)


def _metric_word_proposals(held: Any, better: Any, templated: str | None, subject_id: Any, proposal: Any) -> list[Any]:
    """An AI's name and meaning for a metric the model already holds, as proposals that rename nothing.

    The name becomes a **synonym** (so a question saying "revenue" finds ``total_amount``) and the sentence the
    description when the metric has none or only the rules' templated one. A pending metric's words join its
    review group so they are decided with the definition; an accepted metric's queue on their own.
    """
    name, meaning = (list(better) + [None, None])[:2]
    group = None if held.status == "accepted" else held.group
    subject = str(subject_id.for_metric(held.name, held.table))
    out = []
    alias = (name or "").strip().replace("_", " ")
    synonyms = [x for x in (held.value("synonyms") or []) if isinstance(x, str)]
    if alias and alias.lower() != held.name.replace("_", " ").lower() and alias not in synonyms:
        out.append(proposal(subject, "synonyms", [*synonyms, alias], 0.6, [], "Name written by AI.", group=group))
    current = held.value("description")
    meaning = (meaning or "").strip()
    if meaning and (current is None or current == templated) and meaning != current:
        out.append(proposal(subject, "description", meaning, 0.6, [], "Written by AI.", group=group))
    return out
