"""Pydantic shapes of a catalog namespace's semantic model (``routes/catalog_semantic.py``, ``ai/semantic_routes.py``).

A model belongs to a schema (namespace level 1) or to a catalog (level 0, the layer holding joins that cross schemas).
``catalog/semantic/snapshot.py`` builds ``FactOut`` to ``JoinOut`` straight from the ledger and the metric compiler
reads the same objects the API answers.
"""

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator

FactStatus = Literal["proposed", "reviewed", "accepted", "rejected", "superseded", "stale", "retracted"]
SourceKind = Literal["human", "ai", "inferred", "catalog", "import", "system", "other"]
Layer = Literal["schema", "catalog"]
JoinVerdict = Literal["ok", "reversed", "many_to_many", "unchecked"]


class FactOut(BaseModel):
    """One fact: a value for one attribute of one subject. ``source`` is the actor who stated it
    (``human:<user id>``, ``ai:<model>``, ``inferred:<rule>``, ``catalog:flowfile``), ``source_kind`` its first part."""

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
    subject: str = Field(default="", exclude=True)
    accepted_by: str | None = Field(default=None, exclude=True)
    accepted_at: datetime | None = Field(default=None, exclude=True)


class FieldOut(BaseModel):
    """A column: ``facts`` holds the shown value per attribute (accepted wins, else the newest open one);
    ``pending`` every open fact on the column, the shown ones included."""

    name: str
    dtype: str | None = None
    facts: dict[str, FactOut] = Field(default_factory=dict)
    pending: list[FactOut] = Field(default_factory=list)

    def value(self, attribute: str) -> Any:
        f = self.facts.get(attribute)
        return f.value if f is not None else None


class MetricOut(BaseModel):
    """A table's metric: the accepted definition when there is one, else the newest proposed one. ``pending_group`` is
    the review group of an open edit beside it; ``can_remove`` what glosslayer's policy answers this caller."""

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
    synonyms: list[str] = Field(default_factory=list)
    status: FactStatus
    version: int | None = None
    approved_by: str | None = None
    approved_at: datetime | None = None
    group: str | None = None
    pending_group: str | None = None
    can_remove: bool = False


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
    layer: Layer
    can_remove: bool = False


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

    def column(self, name: str) -> FieldOut | None:
        return next((c for c in self.fields if c.name == name), None)


class SemanticModelSummary(BaseModel):
    """Counts of facts; facts the catalog itself accepted (column types and the like) are not counted."""

    accepted: int = 0
    pending: int = 0
    stale: int = 0


class SemanticModelUpdate(BaseModel):
    """``PUT …/semantic-model``: facts on ``model:<namespace path>``; a field left out is not touched."""

    name: str | None = None
    description: str | None = None
    ai_instructions: str | None = None


class SemanticModelOut(SemanticModelUpdate):
    namespace_id: int
    level: int
    key: str
    tables: list[SemanticTableOut] = Field(default_factory=list)
    joins: list[JoinOut] = Field(default_factory=list)
    progress: SemanticModelSummary = Field(default_factory=SemanticModelSummary)
    can_manage: bool = False
    review_mode: Literal["solo", "team"] = "solo"


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
    """A metric definition, proposed as one review group; ``edit`` to propose a new definition of an existing one."""

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
    synonyms: list[str] | None = None
    edit: bool = False


class MetricSynonymsProposal(BaseModel):
    """Other words for an existing metric, proposed on their own (the definition is not proposed again)."""

    kind: Literal["metric_synonyms"]
    name: str
    synonyms: list[str]


ProposalIn = Annotated[
    FieldProposal | TableProposal | JoinProposal | MetricProposal | MetricSynonymsProposal,
    Field(discriminator="kind"),
]


class ProposalsIn(BaseModel):
    proposals: list[ProposalIn]


class ReviewAction(BaseModel):
    """Decide facts by ``ids`` or one whole ``group``; a join's or metric's group is always decided as a unit.

    ``subject`` (a metric's or join's key, with ``retract``) removes it whole, as ``SemanticModelService.remove`` says.
    """

    action: Literal["accept", "reject", "retract", "reopen"]
    ids: list[str] | None = None
    group: str | None = None
    subject: str | None = None
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


class JoinDependentOut(BaseModel):
    """An accepted metric that stops compiling once a join is removed, and the error. ``uses`` is what of it needs the
    join: ``formula`` (its expression or stored filter), ``time column`` or ``dimension <name>``."""

    metric: str
    metric_key: str
    table: str
    table_id: int | None = None
    uses: list[str] = Field(default_factory=list)
    message: str


class JoinCheckOut(BaseModel):
    """One join measured against the data (``join_check.py``): ``reversed`` when its sides are swapped,
    ``many_to_many`` when both repeat, ``unchecked`` without compute or with an empty or unreadable table."""

    key: str
    name: str
    verdict: JoinVerdict
    message: str


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


FilterOp = Literal["=", "!=", "<", "<=", ">", ">=", "in", "not in", "between", "is null", "is not null"]
Grain = Literal["day", "week", "month", "quarter", "year"]


class MetricFilter(BaseModel):
    """``field`` is ``"table.column"`` or a bare column of the metric's table; ``value`` is a list for ``in`` /
    ``not in`` (any length) and ``between`` (two values), absent for the null checks, one value otherwise."""

    field: str
    op: FilterOp
    value: Any = None


class MetricOrder(BaseModel):
    """``field``: one of the query's dimensions as written, ``<time_column>_<grain>``, or the metric's name."""

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
    status: str
    approved_by: str | None = None


class MetricQueryResult(BaseModel):
    """The rows as ``POST /catalog/sql/execute`` answers them, the compiled ``sql`` and the metric's identity. An
    execution failure comes back in ``error`` (status 200); a query that cannot compile is a 422."""

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
    """What the rules (and the AI draft) stored: ``added`` are new proposals, ``skipped`` the candidates the model
    already holds (accepted, pending or once rejected). ``stats_message`` says why statistics were not available."""

    namespace_id: int
    tables: int = 0
    added: SuggestCounts = Field(default_factory=SuggestCounts)
    skipped: SuggestCounts = Field(default_factory=SuggestCounts)
    stats_available: bool = False
    stats_message: str | None = None


SuggestKind = Literal["join", "metric", "description", "role", "synonyms"]


class SuggestIn(BaseModel):
    """Optional body of ``POST …/semantic-model/suggest``: store only these kinds (absent: every kind)."""

    kinds: list[SuggestKind] | None = None


class _AiRequest(BaseModel):
    """The AI routes' common body; without a ``provider`` the local model is used."""

    namespace_id: int
    provider: str | None = Field(default=None, min_length=1)
    model: str | None = None


class SemanticDraftIn(_AiRequest):
    """``POST /ai/semantic/draft``: words for the rule candidates of ``table_ids`` (default: every table)."""

    table_ids: list[int] | None = None


class SemanticDraftTable(BaseModel):
    """One table's call(s): ``ok`` with the words stored, or the ``reason`` it fell back on. ``withheld_columns`` are
    kept from AI: sent by name and type only, never described by the model."""

    table_id: int
    table: str
    calls: int = 0
    ok: bool = False
    reason: str | None = None
    message: str | None = None
    withheld_columns: list[str] = Field(default_factory=list)


class SemanticDraftOut(SuggestOut):
    provider: str
    model: str | None = None
    table_outcomes: list[SemanticDraftTable] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None


class SemanticAskIn(_AiRequest):
    question: str = Field(min_length=1, max_length=1000)


class SemanticAnswerItem(BaseModel):
    """One group or period of an answer: its ``label`` (a group's values, a formatted period, ``""`` for a single
    value), the metric's ``value`` and that value as the sentence shows it (``text``)."""

    label: str
    value: float | int | None = None
    text: str = ""


class SemanticAnswerFacts(BaseModel):
    """The facts the answer sentence was rendered from, computed from the query's rows (never by a model).

    ``value`` is the single value, or the leading value of a top or bottom answer; ``leaders`` the leading groups (all
    tied ones); ``share`` the leader's fraction of the total when the metric adds up; ``change`` a trend's fraction of
    the previous period. ``truncated`` when the rows may not be all.
    """

    intent: str
    value: SemanticAnswerItem | None = None
    leaders: list[SemanticAnswerItem] = Field(default_factory=list)
    runners_up: list[SemanticAnswerItem] = Field(default_factory=list)
    groups: int | None = None
    share: float | None = None
    highest: SemanticAnswerItem | None = None
    lowest: SemanticAnswerItem | None = None
    latest: SemanticAnswerItem | None = None
    previous: SemanticAnswerItem | None = None
    change: float | None = None
    direction: Literal["up", "down", "flat"] | None = None
    truncated: bool = False


class SemanticAskOut(BaseModel):
    """``POST /ai/semantic/ask``. ``query`` is what ran (or what the model proposed when it could not run); ``notes``
    what the question asked for that could not be applied and readings worth checking; ``partial`` when the answer
    leaves part of the question out; ``answer`` and ``facts`` are rendered in code from ``result``'s rows."""

    question: str
    provider: str
    model: str | None = None
    query: MetricQuery | None = None
    result: MetricQueryResult | None = None
    answered_with: MetricQueryMetric | None = None
    notes: list[str] = Field(default_factory=list)
    degraded: bool = False
    reason: str | None = None
    no_metric_fits: bool = False
    attempts: int = 0
    partial: bool = False
    answer: str | None = None
    intent: str | None = None
    facts: SemanticAnswerFacts | None = None


class SemanticSuggestRequestIn(_AiRequest):
    """``POST /ai/semantic/suggest``: one request in a person's own words, within ``kinds`` when given; with ``kinds``
    alone the AI is asked for more of those kinds than the model holds."""

    request: str | None = Field(default=None, max_length=500)
    kinds: list[SuggestKind] | None = None

    @model_validator(mode="after")
    def _asks_for_something(self) -> "SemanticSuggestRequestIn":
        self.request = (self.request or "").strip() or None
        if self.request is None and not self.kinds:
            raise ValueError("Give a request, or the kinds of suggestion wanted")
        return self


class SemanticSuggestionProposed(BaseModel):
    """One suggestion that passed every check and is stored as proposals: ``summary`` says what in one line."""

    kind: SuggestKind
    summary: str
    table: str | None = None
    name: str | None = None
    fact_ids: list[str] = Field(default_factory=list)
    group: str | None = None
    what: str = Field(default="", exclude=True)


class SemanticSuggestionSkipped(BaseModel):
    """A suggestion that was not stored, and why (a check's sentence); ``retry`` when it goes back to the model."""

    what: str
    why: str
    retry: bool = Field(default=True, exclude=True)


class SemanticSuggestRequestOut(BaseModel):
    """What ``POST /ai/semantic/suggest`` stored; everything in ``proposed`` waits for review.

    ``degraded`` with a ``reason`` (``timeout``, ``provider_error``, ``invalid_answer``, ``all_refused``) when the call
    failed or every suggestion was refused. ``attempts`` counts model calls (1 or 2: one repair round).
    """

    request: str | None = None
    kinds: list[SuggestKind] | None = None
    provider: str
    model: str | None = None
    proposed: list[SemanticSuggestionProposed] = Field(default_factory=list)
    skipped: list[SemanticSuggestionSkipped] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    nothing_to_suggest: bool = False
    why: str | None = None
    degraded: bool = False
    reason: str | None = None
    attempts: int = 0
    withheld_columns: int = 0


class AiNotSetUpOut(BaseModel):
    """The 409 body of the AI semantic routes when no provider is named, saved or installed locally."""

    error_code: Literal["AI_NOT_SET_UP"] = "AI_NOT_SET_UP"
    message: str
    local_available: bool


class SuggestRunningOut(BaseModel):
    """The 409 body of the three Suggest routes while another suggestion runs for the same namespace."""

    error_code: Literal["SUGGEST_RUNNING"] = "SUGGEST_RUNNING"
    message: str


class SemanticActivity(BaseModel):
    """One Suggest run (``catalog/semantic/activity.py``): running while ``finished_at`` is absent. ``kind`` is
    ``suggest`` (rules), ``draft`` (AI descriptions) or ``request`` (specific suggestions); no request text is kept."""

    kind: Literal["suggest", "draft", "request"]
    label: str
    started_at: datetime
    user_id: int | None = None
    mine: bool = False
    step: str | None = None
    finished_at: datetime | None = None
    ok: bool | None = None
    summary: str | None = None
    error: str | None = None
    counts: dict[str, int] = Field(default_factory=dict)


class SemanticActivityOut(BaseModel):
    """``GET …/semantic-model/activity``: the run going on now and the last finished one (kept 30 minutes)."""

    running: SemanticActivity | None = None
    last: SemanticActivity | None = None
