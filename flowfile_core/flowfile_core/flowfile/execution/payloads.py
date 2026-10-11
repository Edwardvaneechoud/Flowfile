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
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from flowfile_core.flowfile.execution.protocol import OpPayload

if TYPE_CHECKING:
    import polars as pl

    from flowfile_core.configs.flow_logger import NodeLogger
    from flowfile_core.schemas import input_schema
    from flowfile_core.schemas.cloud_storage_schemas import (
        CloudStorageWriteSettings,
        FullCloudStorageConnectionWorkerInterface,
    )
    from flowfile_core.schemas.input_schema import (
        DatabaseConnection,
        FullDatabaseConnection,
        NodeDatabaseWriter,
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
class DeltaWritePayload(OperationPayload):
    """Write a plan's rows to a Delta table.

    Attributes:
        plan: The rows to write.
        path: The table's normalised path or URI.
        mode: The Delta write mode (``"overwrite"``, ``"append"`` or ``"error"``).
        partition_by: The partition columns a new table is created with, or ``None``.
        enable_cdf: Turn the table's change data feed on before writing.
        connection: The object-storage connection, its secrets encrypted for the node's user; ``None`` for a table
            on the filesystem the compute writes to.
    """

    plan: pl.LazyFrame
    path: str
    mode: str
    partition_by: list[str] | None
    enable_cdf: bool
    connection: FullCloudStorageConnectionWorkerInterface | None


@dataclass(frozen=True, kw_only=True)
class DeltaMergePayload(OperationPayload):
    """Upsert, update or delete a plan's rows in a Delta table by key.

    Attributes:
        plan: The rows to merge.
        path: The table's normalised path or URI.
        merge_mode: ``"upsert"``, ``"update"`` or ``"delete"``.
        merge_keys: The columns that match a row to the table's.
        partition_by: The partition columns a new table is created with, or ``None``.
        enable_cdf: Turn the table's change data feed on before writing.
        connection: The object-storage connection, its secrets encrypted for the node's user; ``None`` for a table
            on the filesystem the compute writes to.
    """

    plan: pl.LazyFrame
    path: str
    merge_mode: str
    merge_keys: list[str]
    partition_by: list[str] | None
    enable_cdf: bool
    connection: FullCloudStorageConnectionWorkerInterface | None


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
