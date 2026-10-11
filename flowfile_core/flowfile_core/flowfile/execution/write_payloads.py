"""What the write operations take: one frozen dataclass per write ``OperationKind``.

A caller passes the dataclass to ``ComputeBackend.run_op`` under the single key ``PAYLOAD_KEY`` of the
``OpPayload``, and every backend reads the same one: ``LocalBackend`` runs the write in this process, a backend
that crosses a process builds what its transport needs from it. The input crosses as the ``pl.LazyFrame`` itself,
which such a backend serialises when it sends it. A connection's secrets are only ever ``$ffsec$`` ciphertext, which
the compute decrypts where the write runs, so no field holds a decrypted secret.

Every payload names the flow and node the compute files the job and its logs under, and a ``job_id``:
``cancel(JobHandle(backend.id, job_id))`` stops the write while it runs.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TypeVar

import polars as pl

from flowfile_core.flowfile.execution.protocol import OpPayload
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

PAYLOAD_KEY = "payload"
"""The ``OpPayload`` key a write operation's payload dataclass travels under."""


def new_job_id() -> str:
    """A fresh id for a write job."""
    return uuid.uuid4().hex


@dataclass(frozen=True)
class FileWritePayload:
    """Write a plan's rows to a file.

    Attributes:
        plan: The rows to write.
        settings: Where and how to write them: ``abs_file_path``, ``file_type``, ``write_mode`` and the format's
            sheet name, delimiter and compression.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running write by.
    """

    plan: pl.LazyFrame
    settings: OutputSettings
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True)
class DatabaseWritePayload:
    """Write a plan's rows to a database table.

    Attributes:
        plan: The rows to write.
        settings: The node's settings; ``database_write_settings`` holds the connection mode and ``if_exists``.
        connection: The connection the settings resolve to: the node's inline ``DatabaseConnection``, or the stored
            ``FullDatabaseConnection`` a reference names, whose ``password`` is the stored ciphertext.
        encrypted_password: The connection's password as stored, ``None`` for a file-based database.
        table_name: The table to write, qualified by its schema when the settings name one.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running write by.
    """

    plan: pl.LazyFrame
    settings: NodeDatabaseWriter
    connection: DatabaseConnection | FullDatabaseConnection
    encrypted_password: str | None
    table_name: str
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True)
class CloudWritePayload:
    """Write a plan's rows to object storage through the streaming sink.

    Attributes:
        plan: The rows to write.
        settings: The node's write settings: the resource path, format, mode and format options.
        connection: The connection the node resolves to, its secrets encrypted for the node's user.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running write by.
    """

    plan: pl.LazyFrame
    settings: CloudStorageWriteSettings
    connection: FullCloudStorageConnectionWorkerInterface
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True)
class DeltaWritePayload:
    """Write a plan's rows to a Delta table.

    Attributes:
        plan: The rows to write.
        path: The table's normalised path or URI.
        mode: The Delta write mode (``"overwrite"``, ``"append"`` or ``"error"``).
        partition_by: The partition columns a new table is created with, or ``None``.
        enable_cdf: Turn the table's change data feed on before writing.
        connection: The object-storage connection, its secrets encrypted for the node's user; ``None`` for a table
            on the filesystem the compute writes to.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running write by.
    """

    plan: pl.LazyFrame
    path: str
    mode: str
    partition_by: list[str] | None
    enable_cdf: bool
    connection: FullCloudStorageConnectionWorkerInterface | None
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True)
class DeltaMergePayload:
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
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running write by.
    """

    plan: pl.LazyFrame
    path: str
    merge_mode: str
    merge_keys: list[str]
    partition_by: list[str] | None
    enable_cdf: bool
    connection: FullCloudStorageConnectionWorkerInterface | None
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


WritePayload = FileWritePayload | DatabaseWritePayload | CloudWritePayload | DeltaWritePayload | DeltaMergePayload
PayloadT = TypeVar("PayloadT", bound=WritePayload)


def unpack(payload: OpPayload, expected: type[PayloadT]) -> PayloadT:
    """The ``expected`` dataclass ``payload`` carries under ``PAYLOAD_KEY``.

    Raises:
        TypeError: ``payload`` has no ``PAYLOAD_KEY``, or holds something other than an ``expected`` under it.
    """
    value = payload.get(PAYLOAD_KEY)
    if not isinstance(value, expected):
        found = f"a {type(value).__name__}" if PAYLOAD_KEY in payload else f"no such key (keys: {sorted(payload)})"
        raise TypeError(f"Expected a {expected.__name__} under the payload key {PAYLOAD_KEY!r}, found {found}")
    return value
