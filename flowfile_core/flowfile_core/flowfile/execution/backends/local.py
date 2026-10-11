"""Core's own process as a compute: plans stay lazy in core and operations run in process.

Results live in an in-process store that maps a result id to its ``pl.LazyFrame`` (a materialised result as its
DataFrame turned ``.lazy()``); a ``ResultRef``'s ``location`` is that id as UTF-8 bytes. ``submit`` keeps a plan
lazy and collects it only when ``SubmitHints.must_materialise`` asks for the rows. ``run_op`` runs the operations
a flow's nodes run whole when they execute in core, one method per ``OperationKind``; each method's docstring
names the payload keys it reads. Nothing runs asynchronously: every job has ended when its call returns.
"""

from __future__ import annotations

import io
import threading
import uuid
from collections.abc import Callable
from copy import deepcopy
from typing import Any, ClassVar

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.flowfile.execution.errors import (
    ForeignRef,
    OperationUnsupported,
    PlanRejected,
    ResultMissing,
    TaskFailed,
)
from flowfile_core.flowfile.execution.protocol import (
    CredentialRef,
    JobHandle,
    JobStatus,
    OperationKind,
    OpPayload,
    ResultRef,
    Schema,
    SubmitHints,
    SubmitResult,
)
from flowfile_core.flowfile.execution.registry import LOCAL_BACKEND_ID
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import convert_stats_to_column_info
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageWriteSettingsInternal
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities, local_capabilities
from shared.delta_utils import merge_into_delta, write_delta


class LocalBackend:
    """The compute backend for core's own process."""

    id = LOCAL_BACKEND_ID
    protocol_version = PROTOCOL_VERSION

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._results: dict[str, pl.LazyFrame] = {}
        self._failed_jobs: dict[str, str] = {}

    def capabilities(self) -> Capabilities:
        """The capabilities of this process."""
        return local_capabilities()

    def submit(self, plan: bytes, inputs: list[ResultRef], hints: SubmitHints) -> SubmitResult:
        """Hold a serialised plan, deferred unless ``hints.must_materialise`` asks for its rows.

        ``inputs`` are only validated: a plan over results of this backend already reads them through its own
        leaves (``plan_for``).

        Raises:
            ForeignRef: An input was minted by another backend.
            ResultMissing: This backend no longer holds an input.
            PlanRejected: ``plan`` does not deserialise.
            TaskFailed: Collecting the plan for ``must_materialise`` raised; ``status`` reports the job failed.
        """
        for ref in inputs:
            self._plan(ref)
        try:
            lazy_frame = pl.LazyFrame.deserialize(io.BytesIO(plan))
        except Exception as e:
            raise PlanRejected(f"The {self.id!r} backend could not load the plan: {e}") from e
        result_id = _new_result_id()
        frame: pl.LazyFrame | pl.DataFrame = lazy_frame
        if hints.must_materialise:
            try:
                frame = lazy_frame.collect()
            except Exception as e:
                with self._lock:
                    self._failed_jobs[result_id] = str(e)
                raise TaskFailed(str(e)) from e
        ref = self._hold(frame, result_id=result_id)
        sample = None
        if hints.preview_rows is not None:
            sample = _ipc_stream(frame.lazy().head(hints.preview_rows).collect())
        return SubmitResult(ref=ref, sample=sample, handle=JobHandle(backend_id=self.id, job_id=result_id))

    def run_op(self, op: OperationKind, payload: OpPayload) -> dict[str, ResultRef]:
        """Run ``op`` in this process with the payload its method documents.

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
        """Done, or failed with its message when the job's collect raised."""
        self._check_backend(handle.backend_id)
        with self._lock:
            error = self._failed_jobs.get(handle.job_id)
        if error is not None:
            return JobStatus(state="failed", progress=None, error=error)
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

    def _read_database(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Read a database table or query; the rows are held materialised.

        Payload keys:
            settings: The node's ``NodeDatabaseReader``: its ``database_settings`` and the ``fields`` it reads.
            connection: The database connection the settings resolve to.
            encrypted_password: The connection's password as stored, ``None`` for a file-based database.
            cancel_check: Called while reading; ``True`` stops the read.

        Returns ``{"result": ref}``.
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
        return {"result": self._hold(source.get_pl_df())}

    def _write_database(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Write the input to a database table.

        Payload keys:
            input: The ``FlowDataEngine`` to write.
            settings: The node's ``NodeDatabaseWriter``; ``database_write_settings.if_exists`` defaults to append.
            connection: The database connection the settings resolve to.
            encrypted_password: The connection's password as stored, ``None`` for a file-based database.
            table_name: The table to write, qualified by its schema when the settings name one.

        Returns ``{}``.
        """
        connection = payload["connection"]
        payload["input"].to_database_obj(
            database_type=connection.database_type,
            uri=_sql_uri(connection, payload["encrypted_password"]),
            table_name=payload["table_name"],
            if_exists=payload["settings"].database_write_settings.if_exists or "append",
        )
        return {}

    def _read_rest_api(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Fetch a REST API; the response rows are held materialised.

        Payload keys:
            request: The ``RestApiReadSettings`` the node's settings resolve to.
            encrypted_secret: The auth secret as stored, ``None`` when the request carries none.

        Returns ``{"result": ref}``.
        """
        # Function-local, so importing this backend does not load the HTTP client.
        from shared.rest_api.fetch import fetch_rest_api

        encrypted = payload["encrypted_secret"]
        secret = decrypt_secret(encrypted).get_secret_value() if encrypted else None
        return {"result": self._hold(fetch_rest_api(payload["request"], secret=secret))}

    def _read_file(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Read a local file through the engine's readers.

        Payload keys:
            settings: The node's ``NodeRead``; its ``received_file`` names the file and how to read it.
            node_logger: The node's logger, which the Excel reader reports the engine it used to.

        Returns ``{"result": ref}``, with the row count when the reader knows it without reading the rows.
        """
        engine = FlowDataEngine.create_from_path(payload["settings"].received_file, node_logger=payload["node_logger"])
        return {"result": self._hold_engine(engine)}

    def _write_file(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Write the input to a local file.

        Payload keys:
            input: The ``FlowDataEngine`` to write.
            settings: The node's ``NodeOutput``: its ``output_settings`` and ``node_id``.
            flow_id: The id of the flow the node belongs to.

        Returns ``{}``.
        """
        node_settings = payload["settings"]
        payload["input"].output(
            output_fs=node_settings.output_settings,
            flow_id=payload["flow_id"],
            node_id=node_settings.node_id,
            execute_remote=False,
        )
        return {}

    def _fuzzy_join(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Fuzzy-match the left input against the right.

        Payload keys:
            left: The left ``FlowDataEngine``.
            right: The right ``FlowDataEngine``.
            settings: The node's ``NodeFuzzyMatch``; a copy of its ``join_input`` is used.
            node_logger: The node's logger, which the matcher reports its progress to.

        Returns ``{"result": ref}``.
        """
        engine = payload["left"].fuzzy_join(
            fuzzy_match_input=deepcopy(payload["settings"].join_input),
            other=payload["right"],
            node_logger=payload["node_logger"],
        )
        return {"result": self._hold_engine(engine)}

    def _random_split(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Partition the input's rows at random into the node's labelled splits.

        Payload keys:
            input: The ``FlowDataEngine`` to split.
            settings: The node's ``NodeRandomSplit``: its ``splits`` (name and percentage each) and ``seed``.

        Returns one ref per split, keyed by the split's name, in the order the settings list them.
        """
        node_settings = payload["settings"]
        split_pairs = [(split.name, split.percentage) for split in node_settings.splits]
        outputs = payload["input"].random_split(split_pairs, node_settings.seed)
        return {name: self._hold_engine(engine) for name, engine in outputs.outputs.items()}

    def _run_custom_node(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Run a custom node's ``process`` on a fresh instance of its class.

        Payload keys:
            custom_node: The ``CustomNodeBase`` the node was placed with.
            settings: The node's ``UserDefinedNode``: the ``settings`` values, ``user_id`` and ``node_id``.
            inputs: The input ``FlowDataEngine``s, in wiring order.
            output_names: The node's declared outputs, in order.

        Returns ``{"result": ref}`` when ``process`` returns one frame; one ref per declared output, keyed by its
        output handle (``output-0``, ``output-1``, ...), when it returns a dict of them; ``{}`` otherwise.

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
            return {handle: self._hold_engine(engine) for handle, engine in engines.items()}
        if isinstance(output, pl.LazyFrame | pl.DataFrame):
            return {"result": self._hold_engine(FlowDataEngine(output))}
        return {}

    def _write_cloud_storage(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Write the input to object storage.

        Payload keys:
            input: The ``FlowDataEngine`` to write.
            connection: The ``FullCloudStorageConnection`` the node resolves to.
            settings: The node's ``CloudStorageWriteSettings``.

        Returns ``{}``.
        """
        payload["input"].to_cloud_storage_obj(
            CloudStorageWriteSettingsInternal(connection=payload["connection"], write_settings=payload["settings"])
        )
        return {}

    def _write_delta(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Write the input to a Delta table.

        Payload keys:
            input: The ``FlowDataEngine`` to write.
            output_path: The table's path.
            mode: The Delta write mode.
            partition_by: The partition columns, or ``None``.
            enable_cdf: Turn the table's change data feed on before writing.
            storage_options: The object-storage options, ``None`` for the local filesystem.

        Returns ``{}``.
        """
        write_delta(
            payload["input"].data_frame,
            payload["output_path"],
            mode=payload["mode"],
            partition_by=payload["partition_by"],
            storage_options=payload["storage_options"],
            enable_cdf=payload["enable_cdf"],
        )
        return {}

    def _merge_delta(self, payload: OpPayload) -> dict[str, ResultRef]:
        """Upsert, update or delete the input's rows in a Delta table by key; the input is collected first.

        Payload keys:
            input: The ``FlowDataEngine`` whose rows are merged.
            output_path: The table's path.
            merge_mode: ``"upsert"``, ``"update"`` or ``"delete"``.
            merge_keys: The columns that match a row to the table's.
            partition_by: The partition columns, or ``None``.
            enable_cdf: Turn the table's change data feed on before writing.
            storage_options: The object-storage options, ``None`` for the local filesystem.

        Returns ``{}``.
        """
        merge_into_delta(
            payload["input"].data_frame.collect(),
            payload["output_path"],
            merge_mode=payload["merge_mode"],
            merge_keys=payload["merge_keys"],
            partition_by=payload["partition_by"],
            storage_options=payload["storage_options"],
            enable_cdf=payload["enable_cdf"],
        )
        return {}

    _OPERATIONS: ClassVar[dict[OperationKind, Callable[[LocalBackend, OpPayload], dict[str, ResultRef]]]] = {
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
