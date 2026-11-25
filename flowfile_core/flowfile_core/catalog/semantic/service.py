"""The semantic model of a catalog namespace: read, propose, review, history, sync and export.

Facts live in the glosslayer ledger; this service maps a namespace to its dotted path, checks the caller against the
catalog's own grants (reading needs the namespace visible, proposing needs it writable) and leaves accepting to
glosslayer's policy through the actor-bound view (``host.view_for``). Proposals are checked against the catalog before
they are stored, and ledger errors become the catalog's exceptions here.

The ledger is kept current lazily: reading a model runs ``host.sync()`` when this process has not in the last minute.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
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
    compile_metric_query,
    quote_ident,
    sql_literal,
    with_candidate,
)
from flowfile_core.catalog.semantic.join_check import JoinCheck, check_join, unchecked
from flowfile_core.catalog.semantic.snapshot import (
    ACCEPTED,
    GROUPED_KINDS,
    LIVE,
    ModelSnapshot,
    TableInfo,
    build_snapshot,
    read_facts,
)
from flowfile_core.catalog.service import CatalogService
from flowfile_core.schemas import semantic_schema as s

logger = logging.getLogger(__name__)

SYNC_INTERVAL_SECONDS = 60.0
# A metric's parts an edit may leave out; the definition itself (``expression``) is always proposed.
_METRIC_OPTIONAL = ("description", "aggregate", "filter", "time_column", "dimensions", "format", "synonyms")
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
    table: TableInfo, p: s.MetricProposal, snapshot: ModelSnapshot, execute: Callable[[str], Any]
) -> None:
    """Compile a proposed metric against the snapshot a query uses, and dry-run it (one row) before it is stored.

    A compile failure or an engine error is the ``SemanticValueError`` the author sees. Compute that cannot be reached
    does not block authoring: the dry-run is skipped and the proposal stored.
    """
    from requests import RequestException

    from flowfile_core.flowfile.execution.errors import ComputeError

    candidate = s.MetricOut(
        key=p.name,
        name=p.name,
        table=table.key,
        table_id=table.table_id,
        expression=p.expression,
        filter=_clean(p.filter),
        time_column=_clean(p.time_column),
        dimensions=p.dimensions or [],
        status=ACCEPTED,
    )
    try:
        snap = with_candidate(snapshot, candidate)
        compiled = compile_metric_query(snap, s.MetricQuery(metric=p.name), metric_tables={table.key})
    except ValueError as exc:
        raise SemanticValueError(f"Metric {p.name!r} does not compile: {exc}") from None
    try:
        result = execute(compiled.sql)
    except (ComputeError, RequestException) as exc:
        logger.warning("Semantic model: metric %r stored without a dry-run, compute unavailable: %s", p.name, exc)
        return
    if result.error:
        raise SemanticValueError(f"Metric {p.name!r} does not run: {result.error}")


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


def _clean(value: Any) -> Any:
    return None if isinstance(value, str) and not value.strip() else value


def _words(values: list[str] | None) -> list[str]:
    return [w.strip() for w in values or [] if w.strip()]


def _norm(expression: str | None) -> str:
    return (expression or "").replace(" ", "").lower()


def _withdrawals(view: Any, assertions: list[Any]) -> list[tuple[str, Any]]:
    """How to take each live fact away: ``reject`` an open fact someone else stated, ``retract`` anything else.

    Retracting is withdrawing what is held (a person's own proposal, or an accepted fact); rejecting is a decision
    on another's proposal, which also keeps the rules and the AI from proposing the same value again.
    """
    return [
        ("retract" if a.status.value == ACCEPTED or view.actor in (a.sources or ()) else "reject", a)
        for a in assertions
    ]


def _withdraw(view: Any, assertions: list[Any], comment: str) -> list[Any]:
    """Take every fact in ``assertions`` away in one store transaction; the first refusal undoes them all."""
    plan = _withdrawals(view, assertions)
    changed: list[Any] = []
    with view.store.transaction():
        for action in ("reject", "retract"):
            ids = [a.id for act, a in plan if act == action]
            if ids:
                done, errors = view.transition_many(action, ids, comment=comment)
                if errors:
                    raise errors[0][1]
                changed += done
    return changed


@dataclass
class Scope:
    """A namespace, its catalog, and the tables of both by id (``schema_of``: the schema id each table lives in)."""

    ns: Any
    path: str
    catalog: Any
    catalog_path: str
    model_tables: dict[int, TableInfo]
    all_tables: dict[int, TableInfo]
    schema_of: dict[int, int]

    def model_table(self, table_id: int) -> TableInfo:
        t = self.model_tables.get(table_id)
        if t is None:
            raise SemanticValueError(f"Table id={table_id} is not part of this semantic model")
        return t


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

    def namespace(self, namespace_id: int, *, writable: bool = False) -> tuple[Any, str]:
        """The namespace and its path, once the caller may read it (and change it, with ``writable``)."""
        ns = self.repo.get_namespace(namespace_id)
        if ns is None or ns.level not in (0, 1):
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        if self._restricted and ns.id not in self.access.visible_namespace_ids():
            raise NotAuthorizedError(self.user_id or -1, "read this namespace's semantic model")
        if writable and self._restricted and ns.id not in self.access.writable_namespace_ids():
            raise NotAuthorizedError(self.user_id or -1, "change this namespace's semantic model")
        return ns, host.namespace_path(ns, self.db)

    def scope(self, namespace_id: int, *, writable: bool = False) -> Scope:
        ns, path = self.namespace(namespace_id, writable=writable)
        catalog = ns if ns.level == 0 else self.repo.get_namespace(ns.parent_id)
        service = CatalogService(self.repo)
        tables: dict[int, TableInfo] = {}
        schema_of: dict[int, int] = {}
        for schema in self.repo.list_child_namespaces(catalog.id):
            for t in service.list_tables(schema.id):
                if host.is_keyable(t.qualified_name):
                    columns = tuple((c.name, c.dtype) for c in t.schema_columns)
                    tables[t.id] = TableInfo(t.id, t.qualified_name, t.name, t.description, columns)
                    schema_of[t.id] = schema.id
        own = {i: t for i, t in tables.items() if ns.level == 0 or schema_of[i] == ns.id}
        return Scope(ns, path, catalog, host.namespace_path(catalog, self.db), own, tables, schema_of)

    def _build(self, scope: Scope, *, whole_catalog: bool = False, accepted_only: bool = False) -> ModelSnapshot:
        ns, path = (scope.catalog, scope.catalog_path) if whole_catalog else (scope.ns, scope.path)
        tables = scope.all_tables if whole_catalog else scope.model_tables
        return build_snapshot(
            self.view,
            namespace_id=ns.id,
            level=ns.level,
            path=path,
            tables=sorted(tables.values(), key=lambda t: t.key),
            catalog_path=scope.catalog_path if ns.level == 1 else None,
            known_tables={t.key: t.table_id for t in scope.all_tables.values()},
            accepted_only=accepted_only,
        )

    def snapshot(self, namespace_id: int, *, accepted_only: bool = False) -> tuple[ModelSnapshot, Scope]:
        """The model and the scope it was read in."""
        maybe_sync()
        scope = self.scope(namespace_id)
        with _ledger_errors(self.user_id):
            return self._build(scope, accepted_only=accepted_only), scope

    def _query_snapshot(self, namespace_id: int, preview: bool) -> tuple[ModelSnapshot, Scope]:
        """The snapshot a metric query compiles from: the whole catalog's, so a query can cross schemas.

        It holds accepted facts only unless ``preview``, which needs the namespace writable. When the caller cannot
        read at the catalog's level the schema's own is used, which still holds the catalog-level joins touching it.
        """
        from glosslayer.policy import PermissionDenied

        scope = self.scope(namespace_id, writable=preview)
        maybe_sync()
        try:
            snap = self._build(scope, whole_catalog=True, accepted_only=not preview)
        except PermissionDenied:
            with _ledger_errors(self.user_id):
                snap = self._build(scope, accepted_only=not preview)
        snap.related_tables = {t.key: t for t in scope.all_tables.values()}
        return snap, scope

    def can_manage(self, path: str) -> bool:
        from glosslayer.ids import path_covers

        if not host.host().sharing:
            return True
        attrs = self.view.context.attrs if self.view.context else {}
        return bool(attrs.get("admin")) or any(path_covers(p, path) for p in attrs.get("manage") or ())

    def _mark_removable(self, snap: ModelSnapshot) -> None:
        """Set ``can_remove`` on each metric and join: whether the policy lets this caller ``remove`` it."""
        removable = {x.key: x for x in [*snap.metrics, *snap.joins]}
        ids: dict[str, list[str]] = {}
        for f in snap.facts:
            if f.subject in removable:
                ids.setdefault(f.subject, []).append(f.id)
        view = self.view
        try:
            held = view.store.get_many({i for group in ids.values() for i in group}) if ids else {}
            for subject, group in ids.items():
                plan = _withdrawals(view, [held[i] for i in group if i in held])
                removable[subject].can_remove = all(
                    view.policy.can(view.actor, act, a, view.context) for act, a in plan
                )
        except Exception:  # noqa: BLE001
            logger.warning("Semantic model: could not tell which facts this caller may remove", exc_info=True)

    def get_model(self, namespace_id: int) -> s.SemanticModelOut:
        snap, _scope = self.snapshot(namespace_id)
        self._mark_removable(snap)
        return s.SemanticModelOut(
            namespace_id=snap.namespace_id,
            level=snap.level,
            key=snap.path,
            **{attr: snap.model_value(attr) for attr in s.SemanticModelUpdate.model_fields},
            tables=snap.tables,
            joins=snap.joins,
            progress=s.SemanticModelSummary(**snap.progress()),
            can_manage=self.can_manage(snap.path),
            review_mode="team" if host.host().sharing else "solo",
        )

    def update_model(self, namespace_id: int, body: s.SemanticModelUpdate) -> s.SemanticModelOut:
        from glosslayer.ledger import Proposal

        _ns, path = self.namespace(namespace_id, writable=True)
        props = [Proposal(f"model:{path}", attr, value) for attr, value in body if value is not None]
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
        that ``via``, so glosslayer queues them for review and never lets the AI accept. ``comment`` is added to each
        fact's rationale (a join keeps its measurement first).

        A person's blank value for a column or table attribute (an emptied description, "Not set") removes what is
        there instead of storing an empty value. Likewise a person's metric edit that is accepted at once (solo) and
        leaves out an optional part retracts that part's accepted value; a queued edit keeps it until decided.
        """
        from glosslayer.ids import SubjectId
        from glosslayer.ledger import Proposal
        from glosslayer.model import is_structural

        scope = self.scope(namespace_id, writable=True)
        existing: dict[str, str] = {}
        if any(p.kind in ("metric", "metric_synonyms") for p in proposals):
            existing = self._metric_names(scope.path)
            # Live joins, so a metric over a join still waiting for review can be proposed beside it.
            metric_snapshot, _ = self._query_snapshot(namespace_id, preview=True)
        props: list[Any] = []
        cleared: list[tuple[str, str]] = []
        metric_edits: list[tuple[str, set[str]]] = []
        with _ledger_errors(self.user_id):
            for p in proposals:
                if p.kind == "join":
                    props += self._join_proposals(p, scope, rationale=self._check_new_join(p, scope))
                    continue
                if p.kind == "metric":
                    metric = self._checked_metric(p, scope.model_table(p.table_id), existing, metric_snapshot)
                    metric_props = list(metric.proposals())
                    if p.edit and not ai_source:
                        metric_edits.append((str(metric.subject), {mp.attribute for mp in metric_props}))
                    props += metric_props
                    continue
                if p.kind == "metric_synonyms":
                    if p.name not in existing:
                        raise SemanticNotFoundError(f"No metric called {p.name!r} in this model")
                    sid, attribute = SubjectId.for_metric(p.name, existing[p.name]), "synonyms"
                    value = list(dict.fromkeys(_words(p.synonyms)))
                else:
                    t, attribute, value = scope.model_table(p.table_id), p.attribute, _clean(p.value)
                    if p.kind == "field" and not t.has_column(p.column):
                        raise SemanticValueError(f"Table {t.name!r} has no column {p.column!r}")
                    sid = SubjectId.for_field(t.key, p.column) if p.kind == "field" else SubjectId.for_dataset(t.key)
                    if is_structural(sid, attribute):
                        raise SemanticValueError(f"{attribute!r} is read from the catalog and cannot be proposed")
                if value is not None and value != []:
                    props.append(Proposal(str(sid), attribute, value))
                elif not ai_source:
                    cleared.append((str(sid), attribute))
            for prop in props:
                if comment:
                    prop.comment = f"{prop.comment} {comment}" if prop.comment else comment
                if ai_source:
                    prop.confidence = 0.6
            view = host.view_for(self.user, self.access, via=ai_source) if ai_source else self.view
            results = view.propose_many(props, source=ai_source) if props else []
            gone = self._cleared_facts(view, cleared, metric_edits, results)
            if gone:
                _withdraw(view, gone, "cleared")
        stored = {r.assertion.id: r.assertion for r in results}
        return read_facts(view.store, stored.values())

    @staticmethod
    def _cleared_facts(
        view: Any, cleared: list[tuple[str, str]], metric_edits: list[tuple[str, set[str]]], results: list[Any]
    ) -> list[Any]:
        """The live facts a person's blank values and accepted metric edits take away (``propose``)."""
        gone: dict[str, Any] = {}
        for subject, attribute in cleared:
            gone.update({a.id: a for a in view.find(subject=subject, attribute=attribute, statuses=list(LIVE))})
        for subject, kept in metric_edits:
            mine = [r.assertion for r in results if r.assertion.subject == subject]
            if mine and all(a.status.value == ACCEPTED for a in mine):
                for a in view.find(subject=subject, statuses=[ACCEPTED]):
                    if a.attribute in _METRIC_OPTIONAL and a.attribute not in kept:
                        gone[a.id] = a
        return [a for a in gone.values() if not a.redacted]

    def _checked_metric(
        self, p: s.MetricProposal, t: TableInfo, existing: dict[str, str], snapshot: ModelSnapshot
    ) -> Any:
        owner = existing.get(p.name)
        if owner is not None and not p.edit:
            raise SemanticConflictError(f"A metric called {p.name!r} already exists in this model")
        if owner is not None and owner != t.key:
            raise SemanticConflictError(f"Metric {p.name!r} belongs to {owner}, not {t.key}")
        if p.time_column and not t.has_column(p.time_column):
            raise SemanticValueError(f"Table {t.name!r} has no column {p.time_column!r}")
        validate_metric(t, p, snapshot, self._sql)
        return self._metric(p, t)

    @staticmethod
    def _metric(p: s.MetricProposal, t: TableInfo) -> Any:
        from glosslayer.modeling import Metric

        optional = {attr: _clean(getattr(p, attr)) for attr in ("description", "aggregate", "filter", "time_column")}
        return Metric(
            p.name,
            p.expression,
            **optional,
            format=_clean(p.format),
            dimensions=list(p.dimensions or []),
            synonyms=_words(p.synonyms),
            table=t.key,
        )

    def _join_proposals(self, p: s.JoinProposal, scope: Scope, rationale: str, confidence: float | None = None) -> list:
        """A checked join's facts, owned by the schema when both ends are in it, else by the catalog."""
        from glosslayer.modeling import Relationship

        f, t = self._join_ends(p, scope)
        from_schema, to_schema = scope.schema_of[f.table_id], scope.schema_of[t.table_id]
        layer = scope.catalog_path
        if from_schema == to_schema:
            layer = host.namespace_path(self.repo.get_namespace(from_schema), self.db)
        rel = Relationship(
            p.name,
            f.key,
            t.key,
            from_columns=list(p.from_columns),
            to_columns=list(p.to_columns),
            description=_clean(p.description),
            namespace=layer,
        )
        props = list(rel.proposals())
        for prop in props:
            prop.comment, prop.confidence = rationale, confidence
        return props

    @staticmethod
    def _join_ends(p: s.JoinProposal, scope: Scope) -> tuple[TableInfo, TableInfo]:
        ends = []
        for table_id in (p.from_table_id, p.to_table_id):
            if table_id not in scope.all_tables:
                raise SemanticValueError(f"Table id={table_id} is not in this catalog; a join cannot cross catalogs")
            ends.append(scope.all_tables[table_id])
        if scope.ns.level == 1 and not any(t.table_id in scope.model_tables for t in ends):
            raise SemanticValueError("A join in a schema's model must touch one of its tables")
        if not p.from_columns or len(p.from_columns) != len(p.to_columns):
            raise SemanticValueError("A join needs the same number of columns on both sides, at least one")
        for t, cols in zip(ends, (p.from_columns, p.to_columns), strict=True):
            missing = [c for c in cols if not t.has_column(c)]
            if missing:
                raise SemanticValueError(f"Table {t.name!r} has no column {missing[0]!r}")
        if ends[0].table_id == ends[1].table_id and p.from_columns == p.to_columns:
            raise SemanticValueError(
                f"A join of {ends[0].name} to itself needs different columns on each side, such as a parent "
                f"column pointing at the table's key; the same column on both sides joins every row to itself"
            )
        return ends[0], ends[1]

    def measure_join(self, f: TableInfo, from_columns: list[str], t: TableInfo, to_columns: list[str]) -> JoinCheck:
        """A join's two sides measured against the data, without storing anything."""
        return check_join(f, from_columns, t, to_columns, self._sql)

    def _check_new_join(self, p: s.JoinProposal, scope: Scope) -> str:
        """Measure a proposed join's two sides; refuse a reversed or many-to-many one, else answer its evidence."""
        f, t = self._join_ends(p, scope)
        found = self.measure_join(f, p.from_columns, t, p.to_columns)
        if found.verdict in ("reversed", "many_to_many"):
            raise SemanticValueError(found.message)
        if found.verdict == "unchecked":
            logger.warning("Semantic model: join %r stored unchecked: %s", p.name, found.message)
            return found.message
        return f"Checked: {found.message}"

    def check_joins(self, namespace_id: int) -> list[s.JoinCheckOut]:
        """Every join of the model (accepted and pending, both layers) measured the way a new join is."""
        snap, scope = self.snapshot(namespace_id)
        by_key = {t.key: t for t in scope.all_tables.values()}
        out = []
        for j in snap.joins:
            f, t = by_key.get(j.from_table), by_key.get(j.to_table)
            if f is None or t is None or len(j.from_columns) != len(j.to_columns):
                found = unchecked("a table or a column of this join is missing")
            elif not all(map(f.has_column, j.from_columns)) or not all(map(t.has_column, j.to_columns)):
                found = unchecked("a column of this join is no longer there")
            else:
                try:
                    found = self.measure_join(f, j.from_columns, t, j.to_columns)
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

        scope = self.scope(namespace_id)
        tables = {t.key: t for t in scope.all_tables.values()}
        with _ledger_errors(self.user_id):
            facts = read_facts(self.view.store, [a for a in self.view.pending(namespace=scope.path) if not a.redacted])
        entries: dict[str, list[s.FactOut]] = {}
        for f in sorted(facts, key=lambda f: f.updated_at):
            grouped = f.group and f.subject.split(":", 1)[0] in GROUPED_KINDS
            entries.setdefault(f.group if grouped else f.id, []).append(f)
        items = []
        for key, rows in entries.items():
            head = rows[0]
            sid = SubjectId.parse(head.subject)
            table = tables.get(sid.dataset)
            name = table.name if table else sid.dataset
            labels = {
                Kind.FIELD: f"{name}.{sid.name}",
                Kind.DATASET: name,
                Kind.MODEL: sid.namespace or "model",
            }
            items.append(
                s.ReviewItemOut(
                    **head.model_dump(),
                    subject=head.subject,
                    subject_kind=sid.kind.value,
                    subject_label=labels.get(sid.kind, sid.name),
                    table=sid.dataset,
                    table_id=table.table_id if table else None,
                    column=sid.name if sid.kind is Kind.FIELD else None,
                    group_facts=rows if key != head.id else [],
                )
            )
        return items

    def review(self, namespace_id: int, action: s.ReviewAction) -> s.ReviewResultOut:
        from glosslayer.ids import path_covers

        if action.subject:
            return self.remove(namespace_id, action)
        _ns, path = self.namespace(namespace_id)
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
                raise errors[0][1]
        return s.ReviewResultOut(
            changed=read_facts(view.store, changed),
            errors=[s.ReviewError(id=aid, message=str(exc)) for aid, exc in errors],
        )

    def remove(self, namespace_id: int, action: s.ReviewAction) -> s.ReviewResultOut:
        """Remove a metric or a join: every live fact of the subject taken away in one transaction (``_withdraw``).

        Append-only: the history keeps each fact, so the same name can be proposed again. A policy refusal undoes the
        whole removal and comes back as the policy's sentence (403). A catalog-layer join that touches this schema can
        be removed from the schema's model too.
        """
        from glosslayer.ids import Kind

        if action.action != "retract":
            raise SemanticValueError("A metric or a join is removed with the retract action")
        with _ledger_errors(self.user_id):
            sid = self._subject_in_model(namespace_id, action.subject)
            if sid.kind not in (Kind.METRIC, Kind.RELATIONSHIP):
                raise SemanticValueError("Only a metric or a join is removed whole; clear a column's value instead")
            live = [a for a in self.view.find(subject=str(sid), statuses=list(LIVE)) if not a.redacted]
            if not live:
                raise SemanticNotFoundError(f"{action.subject!r} holds nothing to remove")
            changed = _withdraw(self.view, live, action.comment or "removed")
        return s.ReviewResultOut(changed=read_facts(self.view.store, changed))

    def _subject_in_model(self, namespace_id: int, subject: str) -> Any:
        """``subject`` parsed, when it belongs to this namespace's model (a catalog join touching a schema counts)."""
        from glosslayer.ids import Kind, SubjectId, path_covers

        ns, path = self.namespace(namespace_id)
        sid = SubjectId.parse(subject)
        scope = sid.scope_path
        catalog_join = ns.level == 1 and sid.kind is Kind.RELATIONSHIP and scope == path.rsplit(".", 1)[0]
        if not (path_covers(path, scope) or catalog_join):
            raise SemanticNotFoundError(f"{subject!r} is not part of this semantic model")
        return sid

    def join_dependents(self, namespace_id: int, key: str) -> list[s.JoinDependentOut]:
        """The accepted metrics that stop compiling without the join ``key``; nothing is read from the data.

        Each is compiled against the snapshot a query uses, with and without the join: plain (formula and filter), by
        its time grain, and by each allowed dimension. A metric that may be grouped by any column loses only the joined
        columns it could have been grouped by, so it is not listed.
        """
        live, _ = self.snapshot(namespace_id)
        if not any(j.key == key for j in live.joins):
            raise SemanticNotFoundError(f"No join {key!r} in this semantic model")
        snap, _ = self._query_snapshot(namespace_id, preview=False)
        without = replace(snap, joins=[j for j in snap.joins if j.key != key])
        out = []
        for m in snap.metrics:
            cases: list[tuple[str, dict[str, Any]]] = [("formula", {})]
            if m.time_column:
                cases.append(("time column", {"grain": "month"}))
            cases += [(f"dimension {d}", {"dimensions": [d]}) for d in m.dimensions]
            uses, message = [], ""
            for what, kwargs in cases:
                query = s.MetricQuery(metric=m.name, **kwargs)
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
        with _ledger_errors(self.user_id):
            events = self.view.history(self._subject_in_model(namespace_id, subject))
        known = self.view.store.get_many({e.assertion_id for e in events}) if events else {}
        return [
            s.SemanticEventOut(
                seq=e.seq,
                id=e.id,
                ts=e.ts,
                type=e.type.value,
                assertion_id=e.assertion_id,
                attribute=known[e.assertion_id].attribute if e.assertion_id in known else e.data.get("attribute"),
                actor=e.actor,
                via=e.via,
                comment=e.data.get("comment") or e.data.get("reason"),
                value=e.data.get("value"),
            )
            for e in events
        ]

    def sync(self, namespace_id: int) -> s.SemanticSyncOut:
        self.namespace(namespace_id)
        return s.SemanticSyncOut.model_validate(maybe_sync(force=True), from_attributes=True)

    def export(self, namespace_id: int) -> str:
        """The namespace's accepted model as Ossie YAML."""
        _ns, path = self.namespace(namespace_id)
        with _ledger_errors(self.user_id):
            return self.view.to_yaml(namespace=path)

    def suggest_candidates(
        self, namespace_id: int, table_ids: list[int] | None = None
    ) -> tuple[Any, dict[int, TableInfo], str | None]:
        """Rule candidates for the model's tables (``suggest.py``), built in memory; nothing is stored here.

        Returns the ``Suggestions``, the model's tables by id and, when statistics fell back, why.
        """
        from flowfile_core.catalog.semantic.suggest import collect_stats, suggest

        scope = self.scope(namespace_id)
        keep = set(table_ids or scope.model_tables)
        if keep - scope.model_tables.keys():
            scope.model_table(min(keep - scope.model_tables.keys()))
        stats, problem = collect_stats(sorted(scope.model_tables.values(), key=lambda t: t.key), self._sql)
        sugg = suggest(stats)
        sugg.roles = [r for r in sugg.roles if r.table_id in keep]
        sugg.joins = [j for j in sugg.joins if j.from_table_id in keep or j.to_table_id in keep]
        sugg.metrics = [m for m in sugg.metrics if m.table_id in keep]
        sugg.stats = {st.table.table_id: st for st in stats}
        return sugg, scope.model_tables, problem

    def suggest(self, namespace_id: int, kinds: list[str] | None = None) -> s.SuggestOut:
        """Rule-based proposals (roles, joins, metrics) stored with an inferred source, so they queue for review.

        ``kinds`` stores only those; the rules still run whole, since a metric's dimensions come from the joins found.
        """
        sugg, model_tables, problem = self.suggest_candidates(namespace_id)
        if kinds is not None:
            sugg.roles = sugg.roles if "role" in kinds else []
            sugg.joins = sugg.joins if "join" in kinds else []
            sugg.metrics = sugg.metrics if "metric" in kinds else []
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
        stated by ``ai_source`` through a view bound with that ``via``. Each proposal carries its evidence as the
        rationale (the ledger's comment) and a confidence.
        """
        from glosslayer.ids import SubjectId
        from glosslayer.ledger import Proposal

        from flowfile_core.catalog.semantic.suggest import RULE_SOURCE

        scope = self.scope(namespace_id, writable=True)
        with _ledger_errors(self.user_id):
            snap = self._build(scope)
        words = words or {}
        out = s.SuggestOut(namespace_id=namespace_id)

        def skip(what: str, n: int = 1) -> None:
            setattr(out.skipped, what, getattr(out.skipped, what) + n)

        def store(view: Any, source: str, what: str, props: list[Any]) -> None:
            """One batch; a join or a metric counts as one whatever facts it has."""
            if not props:
                return
            results = view.propose_many(props, source=source)
            made, total = sum(r.outcome == "created" for r in results), len(results)
            if what in ("joins", "metrics"):
                made, total = min(made, 1), 1
            setattr(out.added, what, getattr(out.added, what) + made)
            skip(what, total - made)

        by_rules: list[tuple[str, list[Any]]] = []
        by_ai: list[tuple[str, list[Any]]] = []
        tables_by_id = {t.table_id: t for t in snap.tables}
        roles = []
        for r in sugg.roles:
            t = tables_by_id.get(r.table_id)
            col = t.column(r.column) if t else None
            if col is None or "role" in col.facts:
                skip("roles")
                continue
            sid = SubjectId.for_field(t.key, r.column)
            roles.append(Proposal(str(sid), "role", r.role, r.confidence, [], r.rationale))
        by_rules.append(("roles", roles))

        joins = {(j.from_table_id, j.to_table_id, *j.from_columns, *j.to_columns) for j in snap.joins}
        for j in sugg.joins:
            gone = not {j.from_table_id, j.to_table_id} <= scope.all_tables.keys()
            if gone or (j.from_table_id, j.to_table_id, *j.from_columns, *j.to_columns) in joins:
                skip("joins")
                continue
            body = s.JoinProposal(
                kind="join",
                name=j.name,
                from_table_id=j.from_table_id,
                to_table_id=j.to_table_id,
                from_columns=j.from_columns,
                to_columns=j.to_columns,
                description=j.description,
            )
            by_rules.append(("joins", self._join_proposals(body, scope, j.rationale, round(j.confidence, 2))))

        names = self._metric_names(scope.path)
        held = {(m.table, _norm(m.expression)): m for m in snap.metrics}
        word_props: list[Any] = []
        for m in sugg.metrics:
            t = scope.model_tables.get(m.table_id)
            if t is None:
                skip("metrics")
                continue
            better = words.get(m.table_id, {}).get("metrics", {}).get(m.name)
            same = held.get((t.key, _norm(m.expression)))
            if same is not None:
                skip("metrics")
                if better:
                    word_props += _metric_word_proposals(same, *better, m.description)
                continue
            name, description = better or (None, None)
            name, description, by_whom = name or m.name, description or m.description, by_ai if better else by_rules
            if name in names:
                if m.name in names:
                    skip("metrics")
                    continue
                name, description, by_whom = m.name, m.description, by_rules
            proposal = s.MetricProposal(
                kind="metric",
                table_id=t.table_id,
                name=name,
                expression=m.expression,
                description=description,
                aggregate=m.aggregate,
                time_column=m.time_column,
                dimensions=m.dimensions,
            )
            names[name] = t.key
            props = list(self._metric(proposal, t).proposals())
            for prop in props:
                prop.comment = m.rationale + (" Name and meaning written by AI." if by_whom is by_ai else "")
                prop.confidence = round(m.confidence, 2)
            by_whom.append(("metrics", props))

        descriptions = []

        def describe(sid: Any, held_facts: dict[str, Any], text: str | None) -> bool:
            if text and "description" not in held_facts:
                descriptions.append(Proposal(str(sid), "description", text, 0.6, [], "Written by AI."))
            return bool(text) and "description" in held_facts

        for table_id, w in words.items():
            t = tables_by_id.get(table_id)
            if t is None:
                continue
            describe(SubjectId.for_dataset(t.key), t.facts, w["description"])
            for column, text in w["columns"].items():
                col = t.column(column)
                if col is not None and describe(SubjectId.for_field(t.key, column), col.facts, text):
                    skip("descriptions")

        with _ledger_errors(self.user_id):
            for what, props in by_rules:
                store(self.view, RULE_SOURCE, what, props)
            if ai_source and (descriptions or by_ai or word_props):
                ai_view = host.view_for(self.user, self.access, via=ai_source)
                for what, props in [("descriptions", word_props), ("descriptions", descriptions), *by_ai]:
                    store(ai_view, ai_source, what, props)
        return out

    def known_values(self, columns: list[tuple[str, str]], cap: int = 50) -> dict[tuple[str, str], list[str]]:
        """The distinct values of ``(table key, column)`` pairs, read with **one** SQL statement through the catalog
        SQL path. A column with more than ``cap`` values is left out (it is not a category). Any failure answers
        ``{}``: the caller carries on without them.
        """
        pairs = list(dict.fromkeys(columns))
        if not pairs:
            return {}
        parts = [
            f"SELECT DISTINCT {sql_literal(str(i))} AS k, CAST({quote_ident(col)} AS VARCHAR) AS v "
            f"FROM {quote_ident(table)}"
            for i, (table, col) in enumerate(pairs)
        ]
        try:
            result = self._sql(" UNION ALL ".join(parts), max_rows=(cap + 1) * len(pairs))
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

    def _sql(self, sql: str, max_rows: int = 1) -> Any:
        return self._catalog_service().execute_sql_query(sql, max_rows=max_rows, user_id=self.user_id)

    def _compile(
        self, namespace_id: int, query: s.MetricQuery, table_names: dict[str, str] | None = None
    ) -> tuple[CompiledQuery, Scope]:
        snap, scope = self._query_snapshot(namespace_id, query.preview)
        keys = {t.key for t in scope.model_tables.values()}
        try:
            return compile_metric_query(snap, query, metric_tables=keys, table_names=table_names), scope
        except ValueError as exc:
            raise SemanticValueError(str(exc)) from None

    def query_metric(self, namespace_id: int, query: s.MetricQuery) -> s.MetricQueryResult:
        """Compile the query to one SQL statement and run it through the catalog SQL path (the worker computes).

        A worker that stopped after its handshake raises a plain ``requests`` error, not a ``ComputeError``; both
        come back as the result's ``error``, never a 500.
        """
        from requests import RequestException

        from flowfile_core.flowfile.execution.errors import ComputeError

        compiled, _scope = self._compile(namespace_id, query)
        metric = s.MetricQueryMetric.model_validate(compiled.metric, from_attributes=True)
        try:
            answer = self._sql(compiled.sql, max_rows=query.limit).model_dump()
        except ComputeError as exc:
            answer = {"error": str(exc)}
        except RequestException:
            answer = {"error": "The compute worker could not be reached. Is it running?"}
        return s.MetricQueryResult(**answer, sql=compiled.sql, metric=metric)

    def save_metric_query_as_flow(self, namespace_id: int, body: s.MetricQuerySaveAsFlow) -> s.MetricQuerySaveAsFlowOut:
        """Save the compiled query as a flow: one catalog reader per table into a SQL node.

        The SQL node names its inputs ``input_1``, ``input_2`` … in reader order, so the query is compiled a second
        time with those names. ``save_sql_query_as_flow`` finds each reader's table by name in the flow's schema,
        so every table the query reads must live in that schema.
        """
        compiled, scope = self._compile(namespace_id, body.query)
        target = body.namespace_id if body.namespace_id is not None else (scope.ns.id if scope.ns.level == 1 else None)
        if target is None:
            raise SemanticValueError("Choose the schema to save the flow in")
        by_key = {t.key: t for t in scope.all_tables.values()}
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
        for_flow, _scope = self._compile(namespace_id, body.query, table_names=renamed)
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

    Counts are of everything at or under a namespace's path (a table's at its own key); facts the catalog itself
    accepted are left out. Any failure (glosslayer missing, a ledger error) leaves the field ``None``.
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
    buckets = {"accepted": "accepted", "stale": "stale", "proposed": "pending", "reviewed": "pending"}
    totals: dict[str, dict[str, int]] = {}
    for (scope, status), n in counts.items():
        bucket = buckets.get(status.value)
        if not scope or bucket is None:
            continue
        n -= catalog_accepted.get((scope, status), 0)
        parts = scope.split(".")
        for i in range(1, len(parts) + 1):
            entry = totals.setdefault(".".join(parts[:i]), {})
            entry[bucket] = entry.get(bucket, 0) + n

    def walk(nodes: list[Any], parent: str | None) -> None:
        for node in nodes:
            if node.id is None or node.id < 0:
                continue
            path = f"{parent}.{node.name}" if parent else node.name
            node.semantic_model = s.SemanticModelSummary(**totals.get(path, {}))
            for table in node.tables or []:
                key = table.qualified_name or f"{path}.{table.name}"
                table.semantic_model = s.SemanticModelSummary(**totals.get(key, {}))
            walk(node.children, path)

    walk(tree, None)


def _metric_word_proposals(held: s.MetricOut, name: str | None, meaning: str | None, templated: str) -> list[Any]:
    """An AI's name and meaning for a metric the model already holds, as proposals that rename nothing.

    The name becomes a **synonym** (so a question saying "revenue" finds ``total_amount``) and the sentence the
    description when the metric has none or only the rules' templated one. A pending metric's words join its
    review group so they are decided with the definition; an accepted metric's queue on their own.
    """
    from glosslayer.ledger import Proposal

    group = None if held.status == ACCEPTED else held.group
    out = []
    alias = (name or "").replace("_", " ")
    if alias and alias != held.name.replace("_", " ").lower() and alias not in held.synonyms:
        out.append(Proposal(held.key, "synonyms", [*held.synonyms, alias], 0.6, [], "Name written by AI.", group=group))
    if meaning and held.description in (None, templated) and meaning != held.description:
        out.append(Proposal(held.key, "description", meaning, 0.6, [], "Written by AI.", group=group))
    return out
