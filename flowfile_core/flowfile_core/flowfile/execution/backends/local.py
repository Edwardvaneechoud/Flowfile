"""Core's own process as a compute: plans stay lazy in core and operations run in process.

Results live in an in-process store that maps a result id to its ``pl.LazyFrame`` (a materialised result as its
DataFrame turned ``.lazy()``); a ``ResultRef``'s ``location`` is that id as UTF-8 bytes. ``submit`` holds every plan
lazily under the result id it is given (the class docstring has the policy). ``run_op`` runs the operations a
flow's nodes run whole when they execute in core, one method per ``OperationKind``; each method's docstring names
the payload keys it reads, or for a write the ``write_payloads`` dataclass it takes under ``"payload"``. Nothing runs
asynchronously: every job has ended when its call returns, so ``cancel`` has nothing to stop.
"""

from __future__ import annotations

import io
import threading
import uuid
from collections.abc import Callable
from copy import deepcopy
from typing import Any, ClassVar, get_args

import polars as pl
from pydantic import SecretStr

from flowfile_core.configs import logger
from flowfile_core.flowfile.execution.errors import (
    ForeignRef,
    OperationUnsupported,
    ResultMissing,
)
from flowfile_core.flowfile.execution.protocol import (
    CredentialRef,
    JobHandle,
    JobStatus,
    OperationKind,
    OpPayload,
    OpResult,
    ResultRef,
    Schema,
    SubmitHints,
    SubmitResult,
)
from flowfile_core.flowfile.execution.registry import LOCAL_BACKEND_ID
from flowfile_core.flowfile.execution.write_payloads import (
    CloudWritePayload,
    DatabaseWritePayload,
    DeltaMergePayload,
    DeltaWritePayload,
    FileWritePayload,
    unpack,
)
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import convert_stats_to_column_info
from flowfile_core.flowfile.flow_data_engine.utils import local_write_output
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageWriteSettingsInternal,
    FullCloudStorageConnection,
    FullCloudStorageConnectionWorkerInterface,
)
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities, local_capabilities
from shared.delta_utils import merge_into_delta, write_delta

_CLOUD_SECRET_FIELDS = tuple(
    name for name, field in FullCloudStorageConnection.model_fields.items() if SecretStr in get_args(field.annotation)
)
"""The fields of a cloud connection that hold a secret; a write payload carries them encrypted."""


class LocalBackend:
    """The compute backend for core's own process.

    Materialisation policy: this backend holds plans lazily. ``submit`` keeps every plan deferred, whatever the
    hints say: ``must_materialise`` does not collect, and no sample is returned inline even when
    ``preview_rows`` asks for one, because ``sample`` serves it when something reads it. Nothing in core computes
    a node's rows before a consumer, a write or an opened preview needs them.
    """

    id = LOCAL_BACKEND_ID
    protocol_version = PROTOCOL_VERSION

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._results: dict[str, pl.LazyFrame] = {}

    def capabilities(self) -> Capabilities:
        """The capabilities of this process."""
        return local_capabilities()

    def submit(
        self,
        plan: pl.LazyFrame,
        inputs: list[ResultRef],
        hints: SubmitHints,
        result_id: str,
        *,
        flow_id: int,
        node_id: int | str,
    ) -> SubmitResult:
        """Hold ``plan`` lazily under ``result_id``, replacing what that id held.

        The plan object itself is held, never serialised or copied, so a chain of deferred results composes
        lazily over the frames the first plan read. ``inputs`` are only validated: a plan over results of this
        backend already reads them through its own leaves (``plan_for``). ``hints`` are not read (see the class
        docstring), nor are ``flow_id`` and ``node_id``: a job here has no log or file of its own. The returned
        sample is always ``None``.

        Raises:
            ForeignRef: An input was minted by another backend.
            ResultMissing: This backend no longer holds an input.
        """
        for ref in inputs:
            self._plan(ref)
        ref = self._hold(plan, result_id=result_id)
        return SubmitResult(ref=ref, sample=None, handle=JobHandle(backend_id=self.id, job_id=result_id))

    def run_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        """Run ``op`` in this process with the payload its method documents.

        The refs name the results the operation holds here; no operation of this backend reports metadata.

        Raises:
            OperationUnsupported: This backend does not implement ``op``.
        """
        method = self._OPERATIONS.get(op)
        if method is None:
            raise OperationUnsupported(f"The {self.id!r} backend does not run {op.value!r}")
        return method(self, payload)

    def sample(self, ref: ResultRef, n: int) -> bytes:
        """The first ``n`` rows of ``ref`` as an Arrow IPC stream."""
        return _ipc_stream(self._plan(ref).head(max(n, 0)).collect())

    def count(self, ref: ResultRef) -> int:
        """The row count ``ref`` carries, else the rows its plan yields."""
        plan = self._plan(ref)
        if ref.row_count is not None:
            return ref.row_count
        return plan.select(pl.len()).collect().item()

    def exists(self, ref: ResultRef) -> bool:
        """Whether the store still holds ``ref``."""
        self._check_backend(ref.backend_id)
        with self._lock:
            return _store_key(ref) in self._results

    def release(self, ref: ResultRef) -> None:
        """Drop ``ref`` from the store; releasing it again does nothing."""
        self._check_backend(ref.backend_id)
        with self._lock:
            self._results.pop(_store_key(ref), None)

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        """The LazyFrame the store holds for ``ref``."""
        return self._plan(ref)

    def export(self, ref: ResultRef, uri: str, credential: CredentialRef | None) -> str:
        """Always raises ``OperationUnsupported``: this backend writes no result to a URI."""
        raise OperationUnsupported(f"The {self.id!r} backend does not export results")

    def import_uri(
        self, uri: str, schema: Schema, row_count: int | None, credential: CredentialRef | None
    ) -> ResultRef:
        """Always raises ``OperationUnsupported``: this backend reads no result from a URI."""
        raise OperationUnsupported(f"The {self.id!r} backend does not import results from a URI")

    def status(self, handle: JobHandle) -> JobStatus:
        """Done: a job here has ended, and ``submit`` raised if it could not hold the plan."""
        self._check_backend(handle.backend_id)
        return JobStatus(state="done", progress=100, error=None)

    def cancel(self, handle: JobHandle) -> None:
        """Nothing to stop: a job here has ended by the time its handle exists."""
        self._check_backend(handle.backend_id)

    def _check_backend(self, backend_id: str) -> None:
        if backend_id != self.id:
            raise ForeignRef(f"The {self.id!r} backend cannot read a result of the {backend_id!r} backend")

    def _plan(self, ref: ResultRef) -> pl.LazyFrame:
        self._check_backend(ref.backend_id)
        with self._lock:
            plan = self._results.get(_store_key(ref))
        if plan is None:
            raise ResultMissing(f"The {self.id!r} backend no longer holds result {ref.result_id!r}")
        return plan

    def _hold(
        self,
        frame: pl.LazyFrame | pl.DataFrame,
        row_count: int | None = None,
        schema: Schema | None = None,
        result_id: str | None = None,
    ) -> ResultRef:
        """Store ``frame`` and return its reference.

        A DataFrame is held materialised with its height as the row count; a LazyFrame deferred, with ``row_count``
        when the caller knows it. ``schema`` spares resolving the frame's schema when the caller has it.
        """
        result_id = result_id or _new_result_id()
        materialised = isinstance(frame, pl.DataFrame)
        ref = ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=schema if schema is not None else _flowfile_schema(frame.collect_schema()),
            row_count=frame.height if materialised else row_count,
            state="materialised" if materialised else "deferred",
            location=result_id.encode("utf-8"),
        )
        with self._lock:
            self._results[result_id] = frame.lazy() if materialised else frame
        return ref

    def _hold_engine(self, engine: FlowDataEngine) -> ResultRef:
        """Store an engine's frame with the schema and the row count the engine already has."""
        known_count = engine.number_of_records if engine.number_of_records >= 0 else None
        return self._hold(engine.data_frame, row_count=known_count, schema=engine.schema)

    def _read_database(self, payload: OpPayload) -> OpResult:
        """Read a database table or query; the rows are held materialised.

        Payload keys:
            settings: The node's ``NodeDatabaseReader``: its ``database_settings`` and the ``fields`` it reads.
            connection: The database connection the settings resolve to.
            encrypted_password: The connection's password as stored, ``None`` for a file-based database.
            cancel_check: Called while reading; ``True`` stops the read.

        Returns the refs ``{"result": ref}``.
        """
        node_settings = payload["settings"]
        database_settings = node_settings.database_settings
        connection = payload["connection"]
        source = SqlSource(
            connection_string=_sql_uri(connection, payload["encrypted_password"]),
            query=None if database_settings.query_mode == "table" else database_settings.query,
            table_name=database_settings.table_name,
            schema_name=database_settings.schema_name,
            fields=node_settings.fields,
            cancel_check=payload["cancel_check"],
            database_type=connection.database_type,
        )
        return OpResult(refs={"result": self._hold(source.get_pl_df())})

    def _write_database(self, payload: OpPayload) -> OpResult:
        """Write the rows of the ``DatabaseWritePayload`` under ``"payload"`` to its table.

        The password is decrypted here; ``if_exists`` defaults to append. Returns no refs.
        """
        write = unpack(payload, DatabaseWritePayload)
        connection = write.connection
        FlowDataEngine(write.plan).to_database_obj(
            database_type=connection.database_type,
            uri=_sql_uri(connection, write.encrypted_password),
            table_name=write.table_name,
            if_exists=write.settings.database_write_settings.if_exists or "append",
        )
        return OpResult(refs={})

    def _read_rest_api(self, payload: OpPayload) -> OpResult:
        """Fetch a REST API; the response rows are held materialised.

        Payload keys:
            request: The ``RestApiReadSettings`` the node's settings resolve to.
            encrypted_secret: The auth secret as stored, ``None`` when the request carries none.

        Returns the refs ``{"result": ref}``.
        """
        # Function-local, so importing this backend does not load the HTTP client.
        from shared.rest_api.fetch import fetch_rest_api

        encrypted = payload["encrypted_secret"]
        secret = decrypt_secret(encrypted).get_secret_value() if encrypted else None
        return OpResult(refs={"result": self._hold(fetch_rest_api(payload["request"], secret=secret))})

    def _read_file(self, payload: OpPayload) -> OpResult:
        """Read a local file through the engine's readers.

        Payload keys:
            settings: The node's ``NodeRead``; its ``received_file`` names the file and how to read it.
            node_logger: The node's logger, which the Excel reader reports the engine it used to.

        Returns the refs ``{"result": ref}``, with the row count when the reader knows it without reading the rows.
        """
        engine = FlowDataEngine.create_from_path(payload["settings"].received_file, node_logger=payload["node_logger"])
        return OpResult(refs={"result": self._hold_engine(engine)})

    def _write_file(self, payload: OpPayload) -> OpResult:
        """Write the rows of the ``FileWritePayload`` under ``"payload"`` to its file. Returns no refs."""
        write = unpack(payload, FileWritePayload)
        settings = write.settings
        local_write_output(
            write.plan,
            data_type=settings.file_type,
            path=settings.abs_file_path,
            write_mode=settings.write_mode,
            sheet_name=settings.sheet_name,
            delimiter=settings.delimiter,
            compression=settings.compression,
            flow_id=write.flow_id,
            node_id=write.node_id,
        )
        return OpResult(refs={})

    def _fuzzy_join(self, payload: OpPayload) -> OpResult:
        """Fuzzy-match the left input against the right.

        Payload keys:
            left: The left ``FlowDataEngine``.
            right: The right ``FlowDataEngine``.
            settings: The node's ``NodeFuzzyMatch``; a copy of its ``join_input`` is used.
            node_logger: The node's logger, which the matcher reports its progress to.

        Returns the refs ``{"result": ref}``.
        """
        engine = payload["left"].fuzzy_join(
            fuzzy_match_input=deepcopy(payload["settings"].join_input),
            other=payload["right"],
            node_logger=payload["node_logger"],
        )
        return OpResult(refs={"result": self._hold_engine(engine)})

    def _random_split(self, payload: OpPayload) -> OpResult:
        """Partition the input's rows at random into the node's labelled splits.

        Payload keys:
            input: The ``FlowDataEngine`` to split.
            settings: The node's ``NodeRandomSplit``: its ``splits`` (name and percentage each) and ``seed``.

        Returns one ref per split, keyed by the split's name, in the order the settings list them.
        """
        node_settings = payload["settings"]
        split_pairs = [(split.name, split.percentage) for split in node_settings.splits]
        outputs = payload["input"].random_split(split_pairs, node_settings.seed)
        return OpResult(refs={name: self._hold_engine(engine) for name, engine in outputs.outputs.items()})

    def _run_custom_node(self, payload: OpPayload) -> OpResult:
        """Run a custom node's ``process`` on a fresh instance of its class.

        Payload keys:
            custom_node: The ``CustomNodeBase`` the node was placed with.
            settings: The node's ``UserDefinedNode``: the ``settings`` values, ``user_id`` and ``node_id``.
            inputs: The input ``FlowDataEngine``s, in wiring order.
            output_names: The node's declared outputs, in order.

        Returns the refs ``{"result": ref}`` when ``process`` returns one frame; one ref per declared output, keyed by
        its output handle (``output-0``, ``output-1``, ...), when it returns a dict of them; no refs otherwise.

        Raises:
            ValueError: ``process`` returned a dict without one of the declared outputs.
        """
        node_settings = payload["settings"]
        instance = type(payload["custom_node"]).from_settings(node_settings.settings or {})
        if node_settings.user_id is not None:
            instance.set_execution_context(node_settings.user_id)

        output = instance.process(*(engine.data_frame.lazy() for engine in payload["inputs"]))

        accessed_secrets = instance.get_accessed_secrets()
        if accessed_secrets:
            logger.info(f"Node '{node_settings.node_id}' accessed secrets: {accessed_secrets}")
        if isinstance(output, dict):
            engines: dict[str, FlowDataEngine] = {}
            for i, name in enumerate(payload["output_names"]):
                if name not in output:
                    raise ValueError(f"process() did not return declared output '{name}'")
                engines[output_handle(i)] = FlowDataEngine(output[name])
            return OpResult(refs={handle: self._hold_engine(engine) for handle, engine in engines.items()})
        if isinstance(output, pl.LazyFrame | pl.DataFrame):
            return OpResult(refs={"result": self._hold_engine(FlowDataEngine(output))})
        return OpResult(refs={})

    def _write_cloud_storage(self, payload: OpPayload) -> OpResult:
        """Write the rows of the ``CloudWritePayload`` under ``"payload"`` to object storage.

        The connection's secrets are decrypted here. Returns no refs.
        """
        write = unpack(payload, CloudWritePayload)
        FlowDataEngine(write.plan).to_cloud_storage_obj(
            CloudStorageWriteSettingsInternal(
                connection=_decrypted_connection(write.connection), write_settings=write.settings
            )
        )
        return OpResult(refs={})

    def _write_delta(self, payload: OpPayload) -> OpResult:
        """Write the rows of the ``DeltaWritePayload`` under ``"payload"`` to its Delta table. Returns no refs."""
        write = unpack(payload, DeltaWritePayload)
        write_delta(
            write.plan,
            write.path,
            mode=write.mode,
            partition_by=write.partition_by,
            storage_options=_storage_options(write.connection),
            enable_cdf=write.enable_cdf,
        )
        return OpResult(refs={})

    def _merge_delta(self, payload: OpPayload) -> OpResult:
        """Merge the rows of the ``DeltaMergePayload`` under ``"payload"`` into its Delta table by key.

        The rows are collected first, as the merge needs them in memory. Returns no refs.
        """
        write = unpack(payload, DeltaMergePayload)
        merge_into_delta(
            write.plan.collect(),
            write.path,
            merge_mode=write.merge_mode,
            merge_keys=write.merge_keys,
            partition_by=write.partition_by,
            storage_options=_storage_options(write.connection),
            enable_cdf=write.enable_cdf,
        )
        return OpResult(refs={})

    _OPERATIONS: ClassVar[dict[OperationKind, Callable[[LocalBackend, OpPayload], OpResult]]] = {
        OperationKind.database_read: _read_database,
        OperationKind.database_write: _write_database,
        OperationKind.rest_api_read: _read_rest_api,
        OperationKind.file_read: _read_file,
        OperationKind.file_write: _write_file,
        OperationKind.fuzzy_join: _fuzzy_join,
        OperationKind.random_split: _random_split,
        OperationKind.custom_node: _run_custom_node,
        OperationKind.cloud_write: _write_cloud_storage,
        OperationKind.delta_write: _write_delta,
        OperationKind.delta_merge: _merge_delta,
    }


def _new_result_id() -> str:
    return uuid.uuid4().hex


def _store_key(ref: ResultRef) -> str:
    return ref.location.decode("utf-8")


def _flowfile_schema(schema: pl.Schema) -> Schema:
    """A Polars schema as the ``FlowfileColumn`` list a ``FlowDataEngine`` builds from it."""
    return convert_stats_to_column_info(FlowDataEngine._create_schema_stats_from_pl_schema(schema))


def _ipc_stream(frame: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.write_ipc_stream(buffer)
    return buffer.getvalue()


def _sql_uri(connection: Any, encrypted_password: str | None) -> str:
    """The URI of a database connection, its password decrypted in this process."""
    return sql_utils.construct_sql_uri(
        database_type=connection.database_type,
        host=connection.host,
        port=connection.port,
        database=connection.database,
        username=connection.username,
        password=decrypt_secret(encrypted_password) if encrypted_password else None,
        ssl_enabled=bool(getattr(connection, "ssl_enabled", False)),
        connect_timeout=10,
    )


def _decrypted_connection(connection: FullCloudStorageConnectionWorkerInterface) -> FullCloudStorageConnection:
    """The connection with the secrets it carries as ``$ffsec$`` ciphertext decrypted in this process."""
    values = connection.model_dump()
    for name in _CLOUD_SECRET_FIELDS:
        if values.get(name) is not None:
            values[name] = decrypt_secret(values[name])
    return FullCloudStorageConnection(**values)


def _storage_options(connection: FullCloudStorageConnectionWorkerInterface | None) -> dict[str, Any] | None:
    """The object-storage options of ``connection``; ``None`` for a table on the local filesystem."""
    if connection is None:
        return None
    return CloudStorageReader.get_storage_options(_decrypted_connection(connection))
