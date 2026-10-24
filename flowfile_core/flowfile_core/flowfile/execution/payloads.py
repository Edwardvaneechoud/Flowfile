"""What the operations take: one frozen, keyword-only dataclass per ``OperationKind`` that runs whole, which every
backend accepts.

An operation's ``OpPayload`` is ``{"payload": <the kind's dataclass>}`` (``op_payload``); a backend takes it back out
with ``unpack`` and builds what its transport needs from it: ``LocalBackend`` runs the operation in this process, a
backend that crosses a process builds what its transport needs from the fields and serialises a plan when it sends it.
A payload holds what the node knows: its settings model, the connection those settings resolve to, the credential as
stored (the ``$ffsec$`` ciphertext, never a decrypted secret; a cloud connection as its worker interface, which the
compute decrypts where the operation runs), the input rows of a write as the ``pl.LazyFrame`` itself, the flow and
node the compute files the job under, the ``job_id`` that names the job while it runs, and the in-process hooks only
a backend running in core calls (a cancel check, the node logger, the Kafka consumer).

The Delta writes (``DeltaWritePayload``, ``DeltaMergePayload``, ``DeltaScd2Payload``) report what they wrote as the
``OpResult.metadata``: ``{"skipped": True}`` when the write changed nothing, else the ``table_path``, the ``schema``
(``name``/``dtype`` per column), ``row_count``, ``column_count`` and ``size_bytes``. An SCD2 write adds the Delta
``version`` it settled on, a skip included, and its ``scd2_metrics``. A catalog table's write names the table by its
``CatalogTableIdentity`` instead of a ``path``, and the compute reports the path or URI it resolved that to as the
``table_path``. A visualisation session request
(``VizSessionPayload``) reports the session's answer as the metadata too.

The transform payloads (``FuzzyJoinPayload``, ``RandomSplitPayload``, ``CustomNodePayload``, ``PolarsCodePayload``,
``SqlQueryPayload``) carry their input rows as the ``pl.LazyFrame`` each input engine holds; a Polars Code node's code
and a SQL Query node's statement are the payload's text, which only the backend that runs the operation executes.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from flowfile_core.flowfile.execution.protocol import OpPayload, ResultRef, Schema

if TYPE_CHECKING:
    import polars as pl

    from flowfile_core.configs.flow_logger import NodeLogger
    from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
    from flowfile_core.schemas import input_schema
    from flowfile_core.schemas.cloud_storage_schemas import (
        CloudStorageWriteSettings,
        FullCloudStorageConnectionWorkerInterface,
    )
    from flowfile_core.schemas.input_schema import (
        DatabaseConnection,
        FullDatabaseConnection,
        NodeDatabaseWriter,
        NodeFuzzyMatch,
        NodeRandomSplit,
        OutputSettings,
    )
    from shared.kafka.models import KafkaReadSettings

PAYLOAD_KEY = "payload"
"""The one key of an operation's ``OpPayload``."""


def new_job_id() -> str:
    """A fresh job id."""
    return uuid.uuid4().hex


@dataclass(frozen=True, kw_only=True)
class OperationPayload:
    """What every operation payload carries.

    Attributes:
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: Names the job while it runs: ``cancel(JobHandle(backend.id, job_id))`` reaches it.
    """

    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True, kw_only=True)
class DatabaseReadPayload(OperationPayload):
    """A database table or query read.

    Attributes:
        settings: The node's ``NodeDatabaseReader``: its ``database_settings`` and the ``fields`` it reads.
        connection: The connection the settings resolve to: the inline ``DatabaseConnection``, or the stored
            ``FullDatabaseConnection`` a reference names.
        encrypted_password: The connection's password as stored; ``None`` for a file-based database.
        cancel_check: Called while a read in core runs; ``True`` stops it.
    """

    settings: input_schema.NodeDatabaseReader
    connection: input_schema.DatabaseConnection | input_schema.FullDatabaseConnection
    encrypted_password: str | None
    cancel_check: Callable[[], bool]


@dataclass(frozen=True, kw_only=True)
class KafkaReadPayload(OperationPayload):
    """A Kafka topic read that consumes without committing; the consumed offsets come back as ``metadata["offsets"]``.

    Attributes:
        settings: The topic, consumer group and limits with the connection's consumer config, its secrets as stored.
        read_kafka_source: The consumer a read in core calls, as the flow graph binds it.
    """

    settings: KafkaReadSettings
    read_kafka_source: Callable[..., tuple[Any, Any]]


@dataclass(frozen=True, kw_only=True)
class GoogleAnalyticsReadPayload(OperationPayload):
    """A Google Analytics 4 report read.

    Attributes:
        settings: The node's ``NodeGoogleAnalyticsReader``; its ``google_analytics_settings`` name the report.
        auth_method: How the connection signs in.
        encrypted_credential: The service-account key or the OAuth refresh token, as stored.
        oauth_client_id: The OAuth client of the connection's owner; ``None`` for a service account.
        oauth_client_secret_encrypted: That client's secret, encrypted; ``None`` for a service account.
    """

    settings: input_schema.NodeGoogleAnalyticsReader
    auth_method: Literal["service_account", "oauth"]
    encrypted_credential: str
    oauth_client_id: str | None = None
    oauth_client_secret_encrypted: str | None = None


@dataclass(frozen=True, kw_only=True)
class RestApiReadPayload(OperationPayload):
    """A REST API read.

    Attributes:
        settings: The node's ``NodeRestApiReader``; its ``rest_api_settings`` name the request and how to page.
        encrypted_secret: The auth secret as stored; ``None`` when the request carries none.
    """

    settings: input_schema.NodeRestApiReader
    encrypted_secret: str | None


@dataclass(frozen=True, kw_only=True)
class FileReadPayload(OperationPayload):
    """A file read by a reader Polars cannot scan lazily (Excel, Avro, an Arrow stream, a CSV that is not UTF-8).

    Attributes:
        settings: The node's ``NodeRead``; its ``received_file``, absolute path set, names the file and how to read it.
        node_logger: The node's logger, which the Excel reader reports the engine it used to.
    """

    settings: input_schema.NodeRead
    node_logger: NodeLogger | None


@dataclass(frozen=True, kw_only=True)
class FileWritePayload(OperationPayload):
    """Write a plan's rows to a file.

    Attributes:
        plan: The rows to write.
        settings: Where and how to write them: ``abs_file_path``, ``file_type``, ``write_mode`` and the format's
            sheet name, delimiter and compression.
    """

    plan: pl.LazyFrame
    settings: OutputSettings


@dataclass(frozen=True, kw_only=True)
class DatabaseWritePayload(OperationPayload):
    """Write a plan's rows to a database table.

    Attributes:
        plan: The rows to write.
        settings: The node's settings; ``database_write_settings`` holds the connection mode and ``if_exists``.
        connection: The connection the settings resolve to: the node's inline ``DatabaseConnection``, or the stored
            ``FullDatabaseConnection`` a reference names, whose ``password`` is the stored ciphertext.
        encrypted_password: The connection's password as stored, ``None`` for a file-based database.
        table_name: The table to write, qualified by its schema when the settings name one.
    """

    plan: pl.LazyFrame
    settings: NodeDatabaseWriter
    connection: DatabaseConnection | FullDatabaseConnection
    encrypted_password: str | None
    table_name: str


@dataclass(frozen=True, kw_only=True)
class CloudWritePayload(OperationPayload):
    """Write a plan's rows to object storage through the streaming sink.

    Attributes:
        plan: The rows to write.
        settings: The node's write settings: the resource path, format, mode and format options.
        connection: The connection the node resolves to, its secrets encrypted for the node's user.
    """

    plan: pl.LazyFrame
    settings: CloudStorageWriteSettings
    connection: FullCloudStorageConnectionWorkerInterface


@dataclass(frozen=True, kw_only=True)
class CatalogTableIdentity:
    """A catalog table by identity, which the compute that writes it resolves to a path or URI of its own.

    Attributes:
        table_id: The catalog table's id; ``None`` for a table the write creates.
        table_name: The table's directory under its catalog's storage root, a bare name.
        base_uri: The catalog's object-storage root; ``None`` for a table in the compute's own catalog tables
            directory. With the payload's ``connection`` it is the table's storage target.
    """

    table_id: int | None
    table_name: str
    base_uri: str | None


@dataclass(frozen=True, kw_only=True)
class DeltaWritePayload(OperationPayload):
    """Write a plan's rows to a Delta table.

    Attributes:
        plan: The rows to write.
        path: The table's normalised path or URI; ``None`` when ``table`` names it.
        mode: The Delta write mode (``"overwrite"``, ``"append"`` or ``"error"``).
        partition_by: The partition columns a new table is created with, or ``None``.
        enable_cdf: Turn the table's change data feed on before writing.
        connection: The object-storage connection, its secrets encrypted for the node's user; ``None`` for a table
            on the filesystem the compute writes to.
        table: The catalog table the write names by identity instead of a ``path``.
    """

    plan: pl.LazyFrame
    path: str | None = None
    mode: str
    partition_by: list[str] | None
    enable_cdf: bool
    connection: FullCloudStorageConnectionWorkerInterface | None
    table: CatalogTableIdentity | None = None


@dataclass(frozen=True, kw_only=True)
class DeltaMergePayload(OperationPayload):
    """Upsert, update or delete a plan's rows in a Delta table by key.

    Attributes:
        plan: The rows to merge.
        path: The table's normalised path or URI; ``None`` when ``table`` names it.
        merge_mode: ``"upsert"``, ``"update"`` or ``"delete"``.
        merge_keys: The columns that match a row to the table's.
        partition_by: The partition columns a new table is created with, or ``None``.
        enable_cdf: Turn the table's change data feed on before writing.
        connection: The object-storage connection, its secrets encrypted for the node's user; ``None`` for a table
            on the filesystem the compute writes to.
        table: The catalog table the write names by identity instead of a ``path``.
    """

    plan: pl.LazyFrame
    path: str | None = None
    merge_mode: str
    merge_keys: list[str]
    partition_by: list[str] | None
    enable_cdf: bool
    connection: FullCloudStorageConnectionWorkerInterface | None
    table: CatalogTableIdentity | None = None


@dataclass(frozen=True, kw_only=True)
class DeltaScd2Payload(OperationPayload):
    """Apply a plan's rows to a Delta table as a slowly changing dimension of type 2.

    A missing table is an initial load; on an existing one a changed row is end-dated and inserted again as a new
    version, in one commit, and a batch that changes nothing commits nothing. The fields are the resolved SCD2
    configuration of the write, the one the catalog records for the table. Every field is keyword-only.

    Attributes:
        plan: The rows to apply.
        path: The table's path or URI; ``None`` when ``table`` names it.
        business_keys: The columns that identify a row across versions.
        compare_columns: The columns whose change opens a new version.
        full_snapshot: Also end-date the current rows whose key the batch does not carry.
        surrogate_key_column: The generated column holding each version's key.
        valid_from_column: The generated column holding the instant a version became current.
        valid_to_column: The generated column holding the instant a version stopped being current, null while it is.
        is_current_column: The generated column flagging the current version of each key.
        run_timestamp: The write's one processing instant as an ISO 8601 string: the ``valid_from`` of every version
            it inserts and the ``valid_to`` of every version it closes, from which the surrogate keys derive.
        partition_by: The partition columns a new table is created with, nested above the is-current column, or
            ``None``.
        partition_on_current: Partition a new table by the is-current column.
        connection: The object-storage connection, its secrets encrypted for the user it resolves as (a catalog
            table's owner); ``None`` for a table on the filesystem the compute writes to.
        table: The catalog table the write names by identity instead of a ``path``.
    """

    plan: pl.LazyFrame
    path: str | None = None
    business_keys: list[str]
    compare_columns: list[str]
    full_snapshot: bool
    surrogate_key_column: str
    valid_from_column: str
    valid_to_column: str
    is_current_column: str
    run_timestamp: str
    partition_by: list[str] | None
    partition_on_current: bool
    connection: FullCloudStorageConnectionWorkerInterface | None
    table: CatalogTableIdentity | None = None


@dataclass(frozen=True, kw_only=True)
class FuzzyJoinPayload(OperationPayload):
    """Fuzzy-match the rows of one plan against another's.

    Attributes:
        left: The left rows.
        right: The right rows.
        settings: The node's settings; its ``join_input`` (copied, never changed) holds the fuzzy mappings and the
            column selections of both sides.
        node_logger: The node's logger, which an in-process match reports its progress to; read in core only.
        left_schema: The left rows' columns as the input node knows them, which the worker backend prepares the side
            over instead of resolving its plan; ``None`` resolves the plan.
        right_schema: The right rows' columns, as ``left_schema``.
    """

    left: pl.LazyFrame
    right: pl.LazyFrame
    settings: NodeFuzzyMatch
    node_logger: NodeLogger | None = None
    left_schema: Schema | None = None
    right_schema: Schema | None = None


@dataclass(frozen=True, kw_only=True)
class RandomSplitPayload(OperationPayload):
    """Partition a plan's rows at random into the node's labelled splits.

    Attributes:
        plan: The rows to split.
        settings: The node's settings: its ``splits`` (a name and a percentage each, in output order) and its
            ``seed``; without a seed every run draws a new one.
    """

    plan: pl.LazyFrame
    settings: NodeRandomSplit


@dataclass(frozen=True, kw_only=True)
class CustomNodePayload(OperationPayload):
    """Run a custom node's ``process`` on its inputs.

    Attributes:
        inputs: The node's input rows, in wiring order.
        custom_node: The instance the node was placed with; read in core only. An in-process run builds a fresh
            instance of its class from ``settings_values``; a backend that sends the node elsewhere resolves the
            secrets its ``SecretSelector`` fields select to their ``$ffsec$`` ciphertext from it, so they are looked
            up only where they are sent.
        node_source: The source of the node's file, ``None`` for a class defined without one; such a node can only
            run in core.
        class_name: The class to run from ``node_source``.
        settings_values: The node's settings values by section, with the flow's parameters resolved.
        output_names: The node's declared outputs, in output order.
        user_id: The user the node runs as, whose secrets it reads; ``None`` runs it without a secret context.
    """

    inputs: tuple[pl.LazyFrame, ...]
    custom_node: CustomNodeBase
    node_source: str | None
    class_name: str | None
    settings_values: dict[str, Any]
    output_names: list[str]
    user_id: int | None


@dataclass(frozen=True, kw_only=True)
class PolarsCodePayload(OperationPayload):
    """Run a Polars Code node's code once over its inputs on the backend that holds them.

    The operation's refs are ``{"result": ref}``, carrying the schema of the frame the code returned.

    Attributes:
        code: The node's code as its settings hold it when the node runs, a ``${name}`` parameter resolved.
        inputs: The node's input rows in connection order, each the plan its input engine holds (a leaf over the
            input's result); empty for a node with no input. The designer's prediction sends an empty frame of each
            input's schema.
    """

    code: str
    inputs: tuple[pl.LazyFrame, ...]


@dataclass(frozen=True, kw_only=True)
class SqlQueryPayload(OperationPayload):
    """Translate a SQL Query node's statement once over its inputs on the backend that holds them.

    The backend registers the inputs as ``input_1``, ``input_2``, … in a ``pl.SQLContext``. The operation's refs are
    ``{"result": ref}``, carrying the schema of the frame the query answered.

    Attributes:
        sql_code: The node's statement as its settings hold it when the node runs.
        inputs: The node's input rows in connection order, each the plan its input engine holds (a leaf over the
            input's result); empty for a node with no input. The designer's prediction sends an empty frame of each
            input's schema.
    """

    sql_code: str
    inputs: tuple[pl.LazyFrame, ...]


@dataclass(frozen=True, kw_only=True)
class MlTrainPayload(OperationPayload):
    """Fit a model on a plan's rows; the backend that fits it holds it.

    The operation's refs are ``{"model": <blob ref>}``, the model as a result of kind ``"blob"`` on the backend that
    ran it, and its ``OpResult.metadata`` the model's ``sha256``, ``size_bytes`` and ``model_type``.

    Attributes:
        plan: The training rows.
        model_type: The trainer to fit (``shared.ml.trainers``).
        target_column: The column the model predicts.
        feature_columns: The columns the model reads.
        params: The trainer's parameters.
    """

    plan: pl.LazyFrame
    model_type: str
    target_column: str
    feature_columns: list[str]
    params: dict[str, Any]


@dataclass(frozen=True, kw_only=True)
class MlApplyPayload(OperationPayload):
    """Score a plan's rows with a model a backend holds, adding its prediction as a column.

    Attributes:
        plan: The rows to score.
        model: The model, a blob held by the backend that runs the operation: the one a training answered, or a
            catalog artifact fetched onto that backend.
        output_column: The column the prediction is written to.
    """

    plan: pl.LazyFrame
    model: ResultRef
    output_column: str


@dataclass(frozen=True, kw_only=True)
class VizSessionPayload(OperationPayload):
    """Answer one Graphic Walker request over a plan a visualisation session holds.

    The session holds the plan lazily under its key, so requests with the same key reuse it; each chart runs
    against the plan, which nothing materialises first. The session's answer is the operation's ``OpResult.metadata``:
    for ``"fields"`` the ``fields`` with ``cache_hit``, for ``"query"`` the chart's ``rows`` with ``total_rows``,
    ``truncated``, ``elapsed_ms`` and ``cache_hit``, either with an ``error`` when it could not be computed.

    Attributes:
        plan: The rows the charts read.
        session_key: Names the session that holds ``plan``; a request with another key opens another session.
        request: ``"fields"`` asks for the field schema, ``"query"`` for one chart's rows.
        query: The Graphic Walker ``IDataQueryPayload`` of a ``"query"`` request, passed through as it is; ``None``
            for ``"fields"``.
        max_rows: The most rows a ``"query"`` request returns; ``None`` for ``"fields"``.

    Raises:
        ValueError: A ``"query"`` request without its query or ``max_rows``, or a ``"fields"`` request with either.
    """

    plan: pl.LazyFrame
    session_key: str
    request: Literal["fields", "query"]
    query: dict[str, Any] | None = None
    max_rows: int | None = None

    def __post_init__(self) -> None:
        if self.request not in ("fields", "query"):
            raise ValueError(f"A Graphic Walker request is 'fields' or 'query', not {self.request!r}")
        if self.request == "query" and (self.query is None or self.max_rows is None):
            raise ValueError("A Graphic Walker query request carries its query and max_rows")
        if self.request == "fields" and (self.query is not None or self.max_rows is not None):
            raise ValueError("A Graphic Walker fields request carries no query and no max_rows")


P = TypeVar("P", bound=OperationPayload)


def op_payload(payload: OperationPayload) -> OpPayload:
    """The ``OpPayload`` that carries ``payload``."""
    return {PAYLOAD_KEY: payload}


def unpack(payload: OpPayload, expected: type[P]) -> P:
    """The ``expected`` dataclass an ``OpPayload`` carries under ``"payload"``.

    Raises:
        TypeError: ``payload`` is not a mapping, or what it holds under ``"payload"`` is not an ``expected``.
        ValueError: ``payload`` has no ``"payload"`` key, or has keys besides it.
    """
    if not isinstance(payload, Mapping):
        raise TypeError(f"An operation payload is a mapping with a {PAYLOAD_KEY!r} key, not {type(payload).__name__}")
    if PAYLOAD_KEY not in payload:
        raise ValueError(f"The operation payload has no {PAYLOAD_KEY!r} key; it carries a {expected.__name__}")
    extra = sorted(str(key) for key in payload if key != PAYLOAD_KEY)
    if extra:
        raise ValueError(f"The operation payload holds keys besides {PAYLOAD_KEY!r}: {', '.join(extra)}")
    value = payload[PAYLOAD_KEY]
    if not isinstance(value, expected):
        raise TypeError(f"payload[{PAYLOAD_KEY!r}] must be a {expected.__name__}, not {type(value).__name__}")
    return value
