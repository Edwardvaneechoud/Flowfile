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
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
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
from flowfile_core.catalog.service import CatalogService
from flowfile_core.schemas import semantic_schema as s

logger = logging.getLogger(__name__)

ACCEPTED = "accepted"
LIVE = ("proposed", "reviewed", "stale", ACCEPTED)
RULE_SOURCE = "inferred:flowfile-rules"
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


@dataclass(frozen=True)
class TableInfo:
    """What the catalog says about one table: its id, dataset key, name, description and columns (name, dtype)."""

    uid: int
    key: str
    name: str
    description: str | None = None
    columns: tuple[Any, ...] = ()

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


def _facts_out(view: Any, assertions: Iterable[Any]) -> list[s.FactOut]:
    return [s.FactOut.model_validate(f) for f in view.facts(assertions)]


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
        """The namespace's scope. A restricted caller's holds the tables of the schemas they may read and the tables
        granted to them one by one, the read set ``host.view_for`` hands the ledger, so another schema's tables are
        never listed to them."""
        ns, path = self.namespace(namespace_id, writable=writable)
        catalog = ns if ns.level == 0 else self.repo.get_namespace(ns.parent_id)
        service = CatalogService(self.repo)
        visible = self.access.visible_namespace_ids() if self._restricted else None
        granted = self.access.accessible_ids("catalog_table") if self._restricted else set()
        tables: dict[int, TableInfo] = {}
        schema_of: dict[int, int] = {}
        for schema in self.repo.list_child_namespaces(catalog.id):
            for t in service.list_tables(schema.id):
                if visible is not None and schema.id not in visible and t.id not in granted:
                    continue
                if host.is_keyable(t.qualified_name):
                    tables[t.id] = TableInfo(t.id, t.qualified_name, t.name, t.description, tuple(t.schema_columns))
                    schema_of[t.id] = schema.id
        own = {i: t for i, t in tables.items() if ns.level == 0 or schema_of[i] == ns.id}
        return Scope(ns, path, catalog, host.namespace_path(catalog, self.db), own, tables, schema_of)

    def _build(self, scope: Scope, *, whole_catalog: bool = False, accepted_only: bool = False) -> Any:
        """glosslayer's ``ModelView`` of the scope's namespace, or of its whole catalog; a schema's model shows the
        catalog's joins that touch it."""
        in_schema = scope.ns.level == 1 and not whole_catalog
        tables = scope.all_tables if whole_catalog else scope.model_tables
        return self.view.model_view(
            scope.catalog_path if whole_catalog else scope.path,
            tables=sorted(tables.values(), key=lambda t: t.key),
            related=scope.all_tables.values(),
            joins_of=scope.catalog_path if in_schema else None,
            statuses=[ACCEPTED] if accepted_only else LIVE,
        )

    def snapshot(self, namespace_id: int, *, accepted_only: bool = False) -> tuple[Any, Scope]:
        """The model and the scope it was read in."""
        maybe_sync()
        scope = self.scope(namespace_id)
        with _ledger_errors(self.user_id):
            return self._build(scope, accepted_only=accepted_only), scope

    def _query_snapshot(self, namespace_id: int, preview: bool) -> tuple[Any, Scope]:
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
        return snap, scope

    def can_manage(self, path: str) -> bool:
        from glosslayer.ids import path_covers

        if not host.host().sharing:
            return True
        attrs = self.view.context.attrs if self.view.context else {}
        return bool(attrs.get("admin")) or any(path_covers(p, path) for p in attrs.get("manage") or ())

    def get_model(self, namespace_id: int) -> s.SemanticModelOut:
        snap, scope = self.snapshot(namespace_id)
        out = s.SemanticModelOut(
            namespace_id=scope.ns.id,
            level=scope.ns.level,
            key=scope.path,
            **{attr: snap.value(attr) for attr in s.SemanticModelUpdate.model_fields},
            tables=snap.tables,
            joins=snap.joins,
            progress=snap.counts(),
            can_manage=self.can_manage(scope.path),
            review_mode="team" if host.host().sharing else "solo",
        )
        removable = [*(m for t in out.tables for m in t.metrics), *out.joins]
        allowed = self.view.can_withdraw(x.key for x in removable)
        for x in removable:
            x.can_remove = allowed.get(x.key, False)
        return out

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
        existing: dict[str, Any] = {}
        if any(p.kind in ("metric", "metric_synonyms") for p in proposals):
            existing = self.view.metric_names(scope.path)
            # Live joins, so a metric over a join still waiting for review can be proposed beside it.
            metric_snapshot, _ = self._query_snapshot(namespace_id, preview=True)
        props: list[Any] = []
        cleared: list[tuple[str, str]] = []
        metric_edits: set[str] = set()
        with _ledger_errors(self.user_id):
            for p in proposals:
                if p.kind == "join":
                    props += self._join_proposals(p, scope, rationale=self._check_new_join(p, scope))
                    continue
                if p.kind == "metric":
                    metric = self._checked_metric(p, scope.model_table(p.table_id), existing, metric_snapshot)
                    if p.edit and not ai_source:
                        metric_edits.add(str(metric.subject))
                    props += metric.proposals()
                    continue
                if p.kind == "metric_synonyms":
                    if p.name not in existing:
                        raise SemanticNotFoundError(f"No metric called {p.name!r} in this model")
                    sid, attribute = existing[p.name], "synonyms"
                    value = list(dict.fromkeys(_words(p.synonyms)))
                else:
                    t, attribute, value = scope.model_table(p.table_id), p.attribute, _clean(p.value)
                    if p.kind == "field" and p.column not in t.column_names:
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
            view.clear(cleared, comment="cleared")
            edited = [r for r in results if r.assertion.subject in metric_edits]
            view.retract_left_out(edited, _METRIC_OPTIONAL, comment="cleared")
        stored = {r.assertion.id: r.assertion for r in results}
        return _facts_out(view, stored.values())

    def _checked_metric(self, p: s.MetricProposal, t: TableInfo, existing: dict[str, Any], snapshot: Any) -> Any:
        from glosslayer.query import check_metric

        owner = existing.get(p.name)
        if owner is not None and not p.edit:
            raise SemanticConflictError(f"A metric called {p.name!r} already exists in this model")
        if owner is not None and owner.dataset != t.key:
            raise SemanticConflictError(f"Metric {p.name!r} belongs to {owner.scope_path}, not {t.key}")
        if p.time_column and p.time_column not in t.column_names:
            raise SemanticValueError(f"Table {t.name!r} has no column {p.time_column!r}")
        metric = self._metric(p, t)
        check_metric(snapshot, metric, self._rows)
        return metric

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

    def _join_owner(self, f: TableInfo, t: TableInfo, scope: Scope) -> str:
        """The namespace a join belongs to: the schema when both ends are in it, else the catalog."""
        from_schema, to_schema = scope.schema_of[f.uid], scope.schema_of[t.uid]
        if from_schema != to_schema:
            return scope.catalog_path
        return host.namespace_path(self.repo.get_namespace(from_schema), self.db)

    def _join_proposals(self, p: s.JoinProposal, scope: Scope, rationale: str) -> list:
        from glosslayer.modeling import Relationship

        f, t = self._join_ends(p, scope)
        rel = Relationship(
            p.name,
            f.key,
            t.key,
            from_columns=list(p.from_columns),
            to_columns=list(p.to_columns),
            description=_clean(p.description),
            namespace=self._join_owner(f, t, scope),
        )
        props = list(rel.proposals())
        for prop in props:
            prop.comment = rationale
        return props

    @staticmethod
    def _join_ends(p: s.JoinProposal, scope: Scope) -> tuple[TableInfo, TableInfo]:
        from glosslayer import joins

        ends = []
        for table_id in (p.from_table_id, p.to_table_id):
            if table_id not in scope.all_tables:
                raise SemanticValueError(f"Table id={table_id} is not in this catalog; a join cannot cross catalogs")
            ends.append(scope.all_tables[table_id])
        if scope.ns.level == 1 and not any(t.uid in scope.model_tables for t in ends):
            raise SemanticValueError("A join in a schema's model must touch one of its tables")
        f, t = ends
        columns, labels = {e.key: e.column_names for e in ends}, {e.key: e.name for e in ends}
        joins.validate(f.key, p.from_columns, t.key, p.to_columns, columns, labels=labels)
        return f, t

    def measure_join(self, f: TableInfo, from_columns: list[str], t: TableInfo, to_columns: list[str]) -> Any:
        """A join's two sides measured against the data, without storing anything."""
        from glosslayer import joins

        return joins.check(f.key, from_columns, t.key, to_columns, self._rows, labels={f.key: f.name, t.key: t.name})

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
        from glosslayer import joins

        snap, scope = self.snapshot(namespace_id)
        tables = scope.all_tables.values()
        columns, labels = {t.key: t.column_names for t in tables}, {t.key: t.name for t in tables}
        found = joins.check_all(snap.joins, columns, self._rows, labels=labels)
        return [s.JoinCheckOut(key=j.key, name=j.name, verdict=c.verdict, message=c.message) for j, c in found]

    def review_queue(self, namespace_id: int) -> list[s.ReviewItemOut]:
        """Open facts of the model, one entry per fact, one per group for a join or a metric."""
        scope = self.scope(namespace_id)
        with _ledger_errors(self.user_id):
            entries = self.view.review_queue(scope.path, tables=scope.all_tables.values())
        return [s.ReviewItemOut.model_validate(e) for e in entries]

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
            changed=_facts_out(view, changed),
            errors=[s.ReviewError(id=aid, message=str(exc)) for aid, exc in errors],
        )

    def remove(self, namespace_id: int, action: s.ReviewAction) -> s.ReviewResultOut:
        """Remove a metric or a join: every live fact of the subject withdrawn in one transaction.

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
            changed = self.view.withdraw_subject(sid, comment=action.comment or "removed")
        return s.ReviewResultOut(changed=_facts_out(self.view, changed))

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

        They are compiled against the snapshot a query uses (``glosslayer.query.dependents``).
        """
        from glosslayer.query import dependents

        live, _ = self.snapshot(namespace_id)
        if not any(j.key == key for j in live.joins):
            raise SemanticNotFoundError(f"No join {key!r} in this semantic model")
        snap, _ = self._query_snapshot(namespace_id, preview=False)
        return [
            s.JoinDependentOut(
                metric=d.metric.name,
                metric_key=d.metric.key,
                table=d.metric.table,
                table_id=d.metric.table_uid,
                uses=d.uses,
                message=d.message,
            )
            for d in dependents(snap, key)
        ]

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
        """Rule candidates for the model's tables (``glosslayer.rules``), built in memory; nothing is stored here.

        Returns the ``Suggestions``, the model's tables by id and, when statistics fell back, why.
        """
        from glosslayer import rules

        scope = self.scope(namespace_id)
        keys = {scope.model_table(i).key for i in sorted(table_ids or scope.model_tables)}
        stats, problem = rules.collect_stats(sorted(scope.model_tables.values(), key=lambda t: t.key), self._rows)
        return rules.suggest(stats).only(tables=keys), scope.model_tables, problem

    def suggest(self, namespace_id: int, kinds: list[str] | None = None) -> s.SuggestOut:
        """Rule-based proposals (roles, joins, metrics) stored with an inferred source, so they queue for review.

        ``kinds`` stores only those; the rules still run whole, since a metric's dimensions come from the joins found.
        """
        sugg, model_tables, problem = self.suggest_candidates(namespace_id)
        out = self.store_suggestions(namespace_id, sugg.only(kinds=kinds))
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
        """Store ``sugg`` as proposals, skipping what the model already holds (``glosslayer.rules.propose``).

        ``words`` are an AI's descriptions per table id, as ``rules.Words`` takes them; they are stated by
        ``ai_source`` through a view bound with that ``via``, the rules' own candidates by ``RULE_SOURCE``.
        """
        from glosslayer import rules

        scope = self.scope(namespace_id, writable=True)
        tables, by_key = scope.model_tables, {t.key: t for t in scope.all_tables.values()}
        with _ledger_errors(self.user_id):
            stored = rules.propose(
                self.view,
                self._build(scope),
                sugg,
                source=RULE_SOURCE,
                owner=lambda a, b: self._join_owner(by_key[a], by_key[b], scope),
                words={tables[i].key: rules.Words(**w) for i, w in (words or {}).items() if i in tables},
                worded_by=(host.view_for(self.user, self.access, via=ai_source), ai_source) if ai_source else None,
            )
        return s.SuggestOut(namespace_id=namespace_id, added=stored.added, skipped=stored.skipped)

    def known_values(self, columns: list[tuple[str, str]], cap: int = 50) -> dict[tuple[str, str], list[str]]:
        """The distinct values of ``(table key, column)`` pairs, read with **one** SQL statement through the catalog
        SQL path. A column with more than ``cap`` values is left out (it is not a category). Any failure answers
        ``{}``: the caller carries on without them.
        """
        from glosslayer.dialect import literal, quote

        pairs = list(dict.fromkeys(columns))
        if not pairs:
            return {}
        parts = [
            f"SELECT DISTINCT {literal(str(i))} AS k, CAST({quote(col)} AS VARCHAR) AS v FROM {quote(table)}"
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

    def _rows(self, sql: str) -> list[list[Any]]:
        """glosslayer's ``execute``: one statement's rows, with a failure told apart from compute that is not there."""
        from glosslayer.dialect import EngineUnavailable, QueryFailed
        from requests import RequestException

        from flowfile_core.flowfile.execution.errors import ComputeError

        try:
            result = self._sql(sql)
        except (ComputeError, RequestException) as exc:
            raise EngineUnavailable(str(exc)) from exc
        if result.error:
            raise QueryFailed(result.error)
        return result.rows

    def _compile_query(
        self, namespace_id: int, query: s.MetricQuery, table_names: dict[str, str] | None = None
    ) -> tuple[Any, Scope]:
        from glosslayer.query import MetricQuery, compile_query

        snap, scope = self._query_snapshot(namespace_id, query.preview)
        keys = {t.key for t in scope.model_tables.values()}
        try:
            asked = MetricQuery.from_dict(query.model_dump(exclude={"preview"}))
            return compile_query(snap, asked, tables=keys, table_names=table_names), scope
        except ValueError as exc:
            raise SemanticValueError(str(exc)) from None

    def query_metric(self, namespace_id: int, query: s.MetricQuery) -> s.MetricQueryResult:
        """Compile the query to one SQL statement and run it through the catalog SQL path (the worker computes).

        A worker that stopped after its handshake raises a plain ``requests`` error, not a ``ComputeError``; both
        come back as the result's ``error``, never a 500.
        """
        from requests import RequestException

        from flowfile_core.flowfile.execution.errors import ComputeError

        compiled, _scope = self._compile_query(namespace_id, query)
        metric = s.MetricQueryMetric.model_validate(compiled.metric, from_attributes=True)
        try:
            answer = self._sql(compiled.sql, max_rows=query.limit).model_dump()
        except ComputeError as exc:
            answer = {"error": str(exc)}
        except RequestException:
            answer = {"error": "The compute worker could not be reached. Is it running?"}
        return s.MetricQueryResult(**answer, sql=compiled.sql, tables=list(compiled.tables), metric=metric)

    def save_metric_query_as_flow(self, namespace_id: int, body: s.MetricQuerySaveAsFlow) -> s.MetricQuerySaveAsFlowOut:
        """Save the compiled query as a flow: one catalog reader per table into a SQL node.

        The SQL node names its inputs ``input_1``, ``input_2`` … in reader order, so the query is compiled a second
        time with those names. ``save_sql_query_as_flow`` finds each reader's table by name in the flow's schema,
        so every table the query reads must live in that schema.
        """
        compiled, scope = self._compile_query(namespace_id, body.query)
        target = body.namespace_id if body.namespace_id is not None else (scope.ns.id if scope.ns.level == 1 else None)
        if target is None:
            raise SemanticValueError("Choose the schema to save the flow in")
        by_key = {t.key: t for t in scope.all_tables.values()}
        names: list[str] = []
        for key in compiled.tables:
            info = by_key.get(key)
            found = self.repo.get_table_by_name(info.name, target) if info is not None else None
            if found is None or found.id != info.uid:
                raise SemanticValueError(
                    f"Saving as a flow reads tables of the flow's schema only; {key!r} is in another schema"
                )
            names.append(info.name)
        renamed = {key: f"input_{i + 1}" for i, key in enumerate(compiled.tables)}
        for_flow, _scope = self._compile_query(namespace_id, body.query, table_names=renamed)
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


def visible_summaries(tree: list[Any], access: AccessResolver) -> None:
    """Narrow ``tree_summaries`` to what a restricted caller may read, on their filtered tree.

    A namespace counts everything under its path only when the caller may read that whole path (visible, and for a
    catalog every schema under it visible too, as ``host.view_for`` hands paths); otherwise it counts the namespaces
    and tables left under it, so a count never tells of facts the caller cannot open.
    """
    from flowfile_core.database.models import CatalogNamespace

    visible = access.visible_namespace_ids()
    rows = access.db.query(CatalogNamespace.id, CatalogNamespace.parent_id).all()
    readable = visible - {parent for ns_id, parent in rows if ns_id not in visible}

    def walk(nodes: list[Any]) -> None:
        for node in nodes:
            if node.id is None or node.id < 0:
                continue
            walk(node.children)
            if node.semantic_model is None or node.id in readable:
                continue
            parts = [x.semantic_model for x in [*node.children, *(node.tables or [])] if x.semantic_model]
            node.semantic_model = s.SemanticModelSummary(
                **{k: sum(getattr(p, k) for p in parts) for k in ("accepted", "pending", "stale")}
            )

    walk(tree)
