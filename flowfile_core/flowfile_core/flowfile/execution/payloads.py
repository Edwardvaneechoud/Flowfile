"""What the read operations take: one frozen dataclass per read kind, which every backend accepts.

A read's ``OpPayload`` is ``{"payload": <the kind's dataclass>}`` (``op_payload``); a backend takes it back out with
``unpack`` and builds what its transport needs from it. A payload holds what the node knows: its settings model, the
connection those settings resolve to, the credential as stored (the ``$ffsec$`` ciphertext, never a decrypted
secret), the flow and node the compute files the job under, the ``job_id`` that names the job while it runs, and the
in-process hooks only a backend running in core calls (a cancel check, the node logger, the Kafka consumer).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from flowfile_core.flowfile.execution.protocol import OpPayload

if TYPE_CHECKING:
    from flowfile_core.configs.flow_logger import NodeLogger
    from flowfile_core.schemas import input_schema
    from shared.kafka.models import KafkaReadSettings

PAYLOAD_KEY = "payload"
"""The one key of a read's ``OpPayload``."""


def new_job_id() -> str:
    """A fresh job id."""
    return uuid.uuid4().hex


@dataclass(frozen=True, kw_only=True)
class ReadPayload:
    """What every read payload carries.

    Attributes:
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: Names the job while it runs: ``cancel(JobHandle(backend.id, job_id))`` reaches it.
    """

    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True, kw_only=True)
class DatabaseReadPayload(ReadPayload):
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
class KafkaReadPayload(ReadPayload):
    """A Kafka topic read that consumes without committing; the consumed offsets come back as ``metadata["offsets"]``.

    Attributes:
        settings: The topic, consumer group and limits with the connection's consumer config, its secrets as stored.
        read_kafka_source: The consumer a read in core calls, as the flow graph binds it.
    """

    settings: KafkaReadSettings
    read_kafka_source: Callable[..., tuple[Any, Any]]


@dataclass(frozen=True, kw_only=True)
class GoogleAnalyticsReadPayload(ReadPayload):
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
class RestApiReadPayload(ReadPayload):
    """A REST API read.

    Attributes:
        settings: The node's ``NodeRestApiReader``; its ``rest_api_settings`` name the request and how to page.
        encrypted_secret: The auth secret as stored; ``None`` when the request carries none.
    """

    settings: input_schema.NodeRestApiReader
    encrypted_secret: str | None


@dataclass(frozen=True, kw_only=True)
class FileReadPayload(ReadPayload):
    """A file read by a reader Polars cannot scan lazily (Excel, Avro, an Arrow stream, a CSV that is not UTF-8).

    Attributes:
        settings: The node's ``NodeRead``; its ``received_file``, absolute path set, names the file and how to read it.
        node_logger: The node's logger, which the Excel reader reports the engine it used to.
    """

    settings: input_schema.NodeRead
    node_logger: NodeLogger | None


P = TypeVar("P", bound=ReadPayload)


def op_payload(payload: ReadPayload) -> OpPayload:
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
