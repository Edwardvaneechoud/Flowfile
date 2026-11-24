"""Pydantic shapes of a catalog namespace's semantic model (``routes/catalog_semantic.py``).

A model belongs to a schema (namespace level 1) or to a catalog (level 0, the layer holding joins that cross
schemas). Every value shown is a fact from the glosslayer ledger with its own review status, so most shapes carry a
``FactOut`` or a ``status``.

``FactOut.source`` is the actor string that stated the fact (``human:<user id>``, ``ai:<model>``,
``inferred:<rule>``, ``catalog:flowfile``); ``source_kind`` is its part before the colon, so the UI tells a person,
the rules and AI apart without parsing.
"""

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

FactStatus = Literal["proposed", "reviewed", "accepted", "rejected", "superseded", "stale", "retracted"]
SourceKind = Literal["human", "ai", "inferred", "catalog", "import", "system", "other"]


class FactOut(BaseModel):
    """One fact: a value for one attribute of one subject, with its review state and who stated it."""

    id: str
    attribute: str
    value: Any = None
    status: FactStatus
    source: str
    source_kind: SourceKind
    via: str | None = None
    confidence: float | None = None
    rationale: str | None = None
    created_by: str
    updated_at: datetime
    group: str | None = None


class FieldOut(BaseModel):
    """A column: ``facts`` holds the shown value per attribute (accepted wins, else the newest open one);
    ``pending`` every open fact on the column, the shown ones included."""

    name: str
    dtype: str | None = None
    facts: dict[str, FactOut] = Field(default_factory=dict)
    pending: list[FactOut] = Field(default_factory=list)


class MetricOut(BaseModel):
    """A table's metric. The accepted definition when there is one, else the newest proposed one.

    ``pending_group`` is the review group of an open edit beside an accepted definition.
    """

    key: str
    name: str
    table: str
    table_id: int | None = None
    expression: str | None = None
    description: str | None = None
    aggregate: str | None = None
    filter: str | None = None
    time_column: str | None = None
    dimensions: list[str] = Field(default_factory=list)
    format: str | None = None
    status: FactStatus
    version: int | None = None
    approved_by: str | None = None
    approved_at: datetime | None = None
    group: str | None = None
    pending_group: str | None = None


class JoinOut(BaseModel):
    """A join, ``from`` the many side ``to`` the one side. ``layer`` says whether the schema or the catalog owns it;
    a catalog-layer join shown in a schema's model is read-only there."""

    key: str
    name: str
    from_table: str
    to_table: str
    from_table_id: int | None = None
    to_table_id: int | None = None
    from_columns: list[str] = Field(default_factory=list)
    to_columns: list[str] = Field(default_factory=list)
    description: str | None = None
    status: FactStatus
    group: str | None = None
    layer: Literal["schema", "catalog"]


class SemanticTableOut(BaseModel):
    """A catalog table in the model; ``facts`` / ``pending`` are its table-level facts (description, keys, ...)."""

    table_id: int
    key: str
    name: str
    description: str | None = None
    fields: list[FieldOut] = Field(default_factory=list)
    metrics: list[MetricOut] = Field(default_factory=list)
    facts: dict[str, FactOut] = Field(default_factory=dict)
    pending: list[FactOut] = Field(default_factory=list)


class SemanticProgress(BaseModel):
    """Counts of facts; facts the catalog itself accepted (column types and the like) are not counted."""

    accepted: int = 0
    pending: int = 0
    stale: int = 0


class SemanticModelOut(BaseModel):
    namespace_id: int
    level: int
    key: str
    name: str | None = None
    description: str | None = None
    ai_instructions: str | None = None
    tables: list[SemanticTableOut] = Field(default_factory=list)
    joins: list[JoinOut] = Field(default_factory=list)
    progress: SemanticProgress = Field(default_factory=SemanticProgress)
    can_manage: bool = False
    review_mode: Literal["solo", "team"] = "solo"


class SemanticModelUpdate(BaseModel):
    """``PUT …/semantic-model``: facts on ``model:<namespace path>``; a field left out is not touched."""

    name: str | None = None
    description: str | None = None
    ai_instructions: str | None = None


class FieldProposal(BaseModel):
    kind: Literal["field"]
    table_id: int
    column: str
    attribute: str
    value: Any


class TableProposal(BaseModel):
    kind: Literal["table"]
    table_id: int
    attribute: str
    value: Any


class JoinProposal(BaseModel):
    kind: Literal["join"]
    name: str
    from_table_id: int
    to_table_id: int
    from_columns: list[str]
    to_columns: list[str]
    description: str | None = None


class MetricProposal(BaseModel):
    """A metric definition, proposed as one review group. ``edit`` must be true to propose a new definition of a
    metric that already exists in the model."""

    kind: Literal["metric"]
    table_id: int
    name: str
    expression: str
    description: str | None = None
    aggregate: str | None = None
    filter: str | None = None
    time_column: str | None = None
    dimensions: list[str] | None = None
    format: str | None = None
    edit: bool = False


ProposalIn = Annotated[FieldProposal | TableProposal | JoinProposal | MetricProposal, Field(discriminator="kind")]


class ProposalsIn(BaseModel):
    proposals: list[ProposalIn]


class ReviewAction(BaseModel):
    """Decide facts by ``ids`` or one whole ``group``; a join's or metric's group is always decided as a unit."""

    action: Literal["accept", "reject", "retract", "reopen"]
    ids: list[str] | None = None
    group: str | None = None
    comment: str | None = None


class ReviewError(BaseModel):
    id: str
    message: str


class ReviewResultOut(BaseModel):
    changed: list[FactOut] = Field(default_factory=list)
    errors: list[ReviewError] = Field(default_factory=list)


class ReviewItemOut(FactOut):
    """One entry of the review queue. A join or a metric is one entry for its whole group: ``group_facts`` holds
    all of its open facts, this entry's own fields are the first of them."""

    subject: str
    subject_kind: Literal["model", "dataset", "field", "relationship", "metric", "type"]
    subject_label: str
    table: str | None = None
    table_id: int | None = None
    column: str | None = None
    group_facts: list[FactOut] = Field(default_factory=list)


class SemanticEventOut(BaseModel):
    seq: int | None = None
    id: str
    ts: datetime
    type: str
    assertion_id: str
    attribute: str | None = None
    actor: str
    via: str | None = None
    comment: str | None = None
    value: Any = None


class SemanticSyncOut(BaseModel):
    new: list[str] = Field(default_factory=list)
    changed: list[str] = Field(default_factory=list)
    moved: dict[str, str] = Field(default_factory=dict)
    gone: list[str] = Field(default_factory=list)
    proposed: int = 0
    accepted: int = 0
    staled: int = 0


class SemanticModelSummary(BaseModel):
    """The tree badge's counts for one namespace (everything at or under its path)."""

    accepted: int = 0
    pending: int = 0
    stale: int = 0


FilterOp = Literal["=", "!=", "<", "<=", ">", ">=", "in", "not in", "between", "is null", "is not null"]
Grain = Literal["day", "week", "month", "quarter", "year"]


class MetricFilter(BaseModel):
    """``field`` is ``"table.column"`` or a bare column of the metric's table; ``value`` is a list for ``in`` /
    ``not in`` (any length) and ``between`` (two values), absent for the null checks, one value otherwise."""

    field: str
    op: FilterOp
    value: Any = None


class MetricOrder(BaseModel):
    """``field`` is one of the query's dimensions as written (a grain column is ``<time_column>_<grain>``) or the
    metric's name."""

    field: str
    direction: Literal["asc", "desc"] = "asc"


class MetricQuery(BaseModel):
    """One metric by ``dimensions`` (``"table.column"`` or a bare base column), filtered, optionally by time grain.

    ``preview`` lets a user who may change the model include pending facts, to try a metric before it is approved.
    """

    metric: str
    dimensions: list[str] = Field(default_factory=list)
    filters: list[MetricFilter] = Field(default_factory=list)
    grain: Grain | None = None
    order: MetricOrder | None = None
    limit: int = Field(default=1000, ge=1, le=10000)
    preview: bool = False


class MetricQueryMetric(BaseModel):
    name: str
    version: int | None = None
    status: FactStatus | str
    approved_by: str | None = None


class MetricQueryResult(BaseModel):
    """The rows as ``POST /catalog/sql/execute`` answers them, plus the compiled ``sql`` and the metric's identity.

    An execution failure comes back in ``error`` (status 200); a query that cannot compile is a 422.
    """

    columns: list[str] = Field(default_factory=list)
    dtypes: list[str] = Field(default_factory=list)
    rows: list[list] = Field(default_factory=list)
    total_rows: int = 0
    truncated: bool = False
    sql: str
    metric: MetricQueryMetric
    error: str | None = None


class MetricQuerySaveAsFlow(BaseModel):
    """Save a metric query as a registered flow; ``namespace_id`` defaults to the model's schema."""

    query: MetricQuery
    name: str
    namespace_id: int | None = None
    description: str | None = None


class MetricQuerySaveAsFlowOut(BaseModel):
    flow_id: int
    sql: str


class SuggestCounts(BaseModel):
    roles: int = 0
    descriptions: int = 0
    joins: int = 0
    metrics: int = 0


class SuggestOut(BaseModel):
    """What ``POST …/semantic-model/suggest`` (and the AI draft) stored. ``added`` are new proposals; ``skipped`` the
    candidates the model already holds (accepted, pending or once rejected). ``stats_available`` is false when the
    worker could not be reached and the rules ran on names and types alone; ``stats_message`` says why."""

    namespace_id: int
    tables: int = 0
    added: SuggestCounts = Field(default_factory=SuggestCounts)
    skipped: SuggestCounts = Field(default_factory=SuggestCounts)
    stats_available: bool = False
    stats_message: str | None = None


class SemanticDraftIn(BaseModel):
    """``POST /ai/semantic/draft``: words for the rule candidates of ``table_ids`` (default: every table)."""

    namespace_id: int
    table_ids: list[int] | None = None
    provider: str | None = Field(default=None, min_length=1)
    model: str | None = None


class SemanticDraftTable(BaseModel):
    """One table's call(s): ``ok`` with the words stored, or ``degraded`` with the ``reason`` it fell back on."""

    table_id: int
    table: str
    calls: int = 0
    ok: bool = False
    reason: str | None = None
    message: str | None = None


class SemanticDraftOut(SuggestOut):
    provider: str
    model: str | None = None
    table_outcomes: list[SemanticDraftTable] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None


class SemanticAskIn(BaseModel):
    namespace_id: int
    question: str = Field(min_length=1, max_length=1000)
    provider: str | None = Field(default=None, min_length=1)
    model: str | None = None


class SemanticAnsweredWith(BaseModel):
    name: str
    version: int | None = None
    status: str
    approved_by: str | None = None


class SemanticAskOut(BaseModel):
    """``POST /ai/semantic/ask``. ``query`` is what ran (or what the model proposed when it could not run); ``result``
    the rows, which never go back to the model. ``no_metric_fits`` when no accepted metric answers the question
    (``notes`` then holds the model's reason). ``notes`` also lists every name repaired or dropped at the seam."""

    question: str
    provider: str
    model: str | None = None
    query: MetricQuery | None = None
    result: MetricQueryResult | None = None
    answered_with: SemanticAnsweredWith | None = None
    notes: list[str] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None
    no_metric_fits: bool = False
    attempts: int = 0
    partial: bool = False
    """True when the question asked for something the model cannot give (a breakdown it has no dimension for); the
    answer then leaves it out, and ``notes`` says what."""


class AiNotSetUpOut(BaseModel):
    """The 409 body of both AI semantic routes when no provider is named, saved or installed locally."""

    error_code: Literal["AI_NOT_SET_UP"] = "AI_NOT_SET_UP"
    message: str
    local_available: bool
