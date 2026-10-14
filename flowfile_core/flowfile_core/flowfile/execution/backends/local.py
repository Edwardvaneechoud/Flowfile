"""Core's own process as a compute: plans stay lazy in core and operations run in process.

Results live in an in-process store that maps a result id to its ``pl.LazyFrame`` (a materialised result as its
DataFrame turned ``.lazy()``, a stored result as a scan of its file); a ``ResultRef``'s ``location`` is that id as UTF-8
bytes. ``submit`` holds a plan lazily under the result id it is given, or stores it (the class docstring has the
policy). ``run_op`` runs the operations a
flow's nodes run whole when they execute in core, one method per ``OperationKind``; each method's docstring names
the payload keys it reads; every other kind takes its dataclass from ``execution/payloads.py`` under ``"payload"``.
A read holds what it read as a deferred plan, so the reader keeps the frame through ``plan_for`` and can release the
ref at once. A result crosses to another backend only through ``export``, a streaming sink of its plan to a URI, and
``import_uri``, a scan of a URI held deferred. A blob, a file of bytes that is not rows, is held beside the store as a
copy under core's storage directory, which ``import_uri(kind="blob")`` makes from a ``file://`` URI or a presigned
``https://`` URL, or a training writes for the model it fitted here, and ``export`` copies out the same way. Nothing
runs asynchronously: every job has ended when its call returns, so ``cancel`` has nothing to stop.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import threading
import uuid
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any, ClassVar, Literal, get_args

import polars as pl
from pydantic import SecretStr

from flowfile_core.configs import logger, settings
from flowfile_core.flowfile.execution.errors import (
    ForeignRef,
    OperationUnsupported,
    ResultMissing,
)
from flowfile_core.flowfile.execution.payloads import (
    CloudWritePayload,
    CustomNodePayload,
    DatabaseReadPayload,
    DatabaseWritePayload,
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
    FileReadPayload,
    FileWritePayload,
    FuzzyJoinPayload,
    KafkaReadPayload,
    MlApplyPayload,
    MlTrainPayload,
    RandomSplitPayload,
    RestApiReadPayload,
    unpack,
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
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import convert_stats_to_column_info
from flowfile_core.flowfile.flow_data_engine.utils import local_write_output
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_core.flowfile.sources.external_sources.rest_api_source import build_rest_api_worker_settings
from flowfile_core.flowfile.sources.external_sources.sql_source import utils as sql_utils
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import SqlSource
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageWriteSettingsInternal,
    FullCloudStorageConnection,
    FullCloudStorageConnectionWorkerInterface,
)
from flowfile_core.secret_manager.secret_manager import decrypt_secret
from shared.cloud_credential_provider import CredentialRefProvider
from shared.compute_protocol import (
    CLOUD_SCHEMES,
    PROTOCOL_VERSION,
    Capabilities,
    get_blob,
    local_capabilities,
    put_blob,
)
from shared.delta_utils import (
    get_delta_size_bytes,
    merge_into_delta,
    scd2_into_delta,
    validate_catalog_path,
    validate_catalog_uri,
    write_delta,
)
from shared.ml.trainers import get_trainer
from shared.storage_config import storage

_CLOUD_SECRET_FIELDS = tuple(
    name for name, field in FullCloudStorageConnection.model_fields.items() if SecretStr in get_args(field.annotation)
)
"""The fields of a cloud connection that hold a secret; a write payload carries them encrypted."""


class LocalBackend:
    """The compute backend for core's own process.

    Materialisation policy: with ``settings.FLOWFILE_COMPUTE_PROTOCOL`` on, a submit whose hints say
    ``must_materialise`` is stored: a streaming sink writes its rows to a file under core's own storage directory and
    the store holds a scan of that file, so nothing is collected into memory. Every other plan, and every plan with
    the flag off, is held deferred as the object submitted. No sample is returned inline either way, even when
    ``preview_rows`` asks for one, because ``sample`` serves it when something reads it. Apart from a stored result,
    nothing in core computes a node's rows before a consumer, a write or an opened preview needs them.

    A blob (``ResultRef.kind == "blob"``), an imported one or a model a training fitted here, is held as a copy of its
    bytes at ``cache/local/-1/<result_id>.blob`` under core's storage directory, beside the store, in both flag states.
    It has no rows: ``plan_for``, ``sample`` and ``count`` raise ``OperationUnsupported`` for one, as ``submit`` does
    for one among its inputs, while ``exists``, ``export`` and an apply take it, and ``release`` removes the copy.
    """

    id = LOCAL_BACKEND_ID
    protocol_version = PROTOCOL_VERSION

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._results: dict[str, pl.LazyFrame] = {}
        self._files: dict[str, Path] = {}
        self._blobs: dict[str, Path] = {}

    def capabilities(self) -> Capabilities:
        """The capabilities of this process, with the operations ``run_op`` runs here and the schemes it uses.

        Core's process is on core's host, so it reads and writes local files as well as object storage, and ``https``:
        a presigned URL a blob is copied to or from.
        """
        schemes = CLOUD_SCHEMES | {"file", "https"}
        return local_capabilities().model_copy(
            update={
                "operations": frozenset(op.value for op in self._OPERATIONS),
                "read_schemes": schemes,
                "write_schemes": schemes,
            }
        )

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
        """Hold ``plan`` under ``result_id``, or store it when it must be materialised, replacing what that id held.

        A deferred plan is the object itself, never serialised or copied, so a chain of deferred results composes
        lazily over the frames the first plan read. With the protocol flag on, a plan whose hints say
        ``must_materialise`` is streamed to ``cache/local/<flow_id>/<result_id>.arrow`` under core's storage
        directory and held as a scan of that file; a sink that fails leaves no file at that path and nothing held, and
        its Polars error propagates, while what it wrote beside the path goes with the id's next ``release`` or
        ``submit``. The other hints are not read (see the class docstring), nor is ``node_id``. What the id held is
        dropped first, its files included. ``inputs`` are only validated: a plan over results of this backend already
        reads them through its own leaves (``plan_for``). The returned sample is always ``None``.

        Raises:
            ForeignRef: An input was minted by another backend.
            OperationUnsupported: An input is a blob, which a plan cannot read.
            ResultMissing: This backend no longer holds an input.
        """
        for ref in inputs:
            self._plan(ref)
        self._drop(result_id)
        if hints.must_materialise and settings.FLOWFILE_COMPUTE_PROTOCOL:
            ref = self._store(plan, result_id=result_id, flow_id=flow_id)
        else:
            ref = self._hold(plan, result_id=result_id)
        return SubmitResult(ref=ref, sample=None, handle=JobHandle(backend_id=self.id, job_id=result_id))

    def run_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        """Run ``op`` in this process with the payload its method documents.

        The refs name the results the operation holds here. Only the Kafka read (the offsets to commit), the Delta
        writes (what they wrote, in the keys the ``payloads`` module docstring lists) and a training (its model's
        ``sha256``, ``size_bytes`` and ``model_type``) report metadata.

        Raises:
            OperationUnsupported: This backend does not implement ``op``: ``google_analytics_read`` and
                ``viz_session``, whose visualisation sessions live on the worker.
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
        """Whether the store, or the blobs beside it, still hold ``ref``."""
        self._check_backend(ref.backend_id)
        with self._lock:
            return _store_key(ref) in self._results or _store_key(ref) in self._blobs

    def release(self, ref: ResultRef) -> None:
        """Drop ``ref`` from the store, and a stored result's file or a blob's copy; releasing it again does nothing."""
        self._check_backend(ref.backend_id)
        self._drop(_store_key(ref))

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        """The LazyFrame the store holds for ``ref``."""
        return self._plan(ref)

    def export(self, ref: ResultRef, uri: str, credential: CredentialRef | None) -> str:
        """Write ``ref``'s rows to ``uri`` as parquet by a streaming sink of its plan, and return ``uri``.

        Nothing is collected into memory. ``credential`` names the stored connection the target needs; this process
        resolves it with its own catalog and master key when the sink opens the target. The target is the caller's,
        so a sink that fails leaves it to the caller and its Polars error propagates.

        A blob's copy is copied as it is to a ``file://`` URI or ``PUT`` to a presigned ``https://`` URL
        (``put_blob``), which carries its own authorisation, so a blob takes no credential.

        Raises:
            ForeignRef: Another backend minted ``ref``.
            OSError: A blob's copy or ``PUT`` failed; the message names no URL query.
            ResultMissing: This backend no longer holds ``ref``.
            ValueError: ``ref`` is a blob and ``credential`` is set, or ``uri`` is no ``file://`` or ``https://`` one.
        """
        self._check_backend(ref.backend_id)
        if ref.kind == "blob":
            if credential is not None:
                raise ValueError("A blob is copied to a URI that carries its own authorisation; it takes no credential")
            put_blob(self._blob(ref), uri)
            return uri
        self._plan(ref).sink_parquet(uri, **_credential_kwargs(credential))
        return uri

    def import_uri(
        self,
        uri: str,
        schema: Schema | None,
        row_count: int | None,
        credential: CredentialRef | None,
        kind: Literal["table", "blob"] = "table",
    ) -> ResultRef:
        """Hold a scan of the parquet at ``uri`` as a deferred result with the ``schema`` and ``row_count`` given.

        Nothing is read here: the schema and the count are the caller's, so ``count`` answers the given count (and
        counts the rows only when it is ``None``) and the file need not exist until something reads the rows.
        ``credential`` is resolved in this process when the scan runs. ``release`` forgets the scan and leaves the
        file, which is not this backend's.

        With ``kind="blob"`` the bytes at a ``file://`` URI or a presigned ``https://`` URL are copied now
        (``get_blob``) to ``cache/local/-1/<result_id>.blob`` under core's storage directory and held under a fresh id
        as a materialised blob with no columns and no row count; ``release`` removes that copy and leaves the source.

        Raises:
            OSError: A blob's copy or ``GET`` failed; nothing is held and nothing is left of the copy.
            ValueError: A table without ``schema``, a blob with a ``schema``, a ``row_count`` or a ``credential``, or
                a blob ``uri`` that is no ``file://`` or ``https://`` one; nothing is held.
        """
        if kind == "blob":
            if schema is not None or row_count is not None or credential is not None:
                raise ValueError("A blob import takes no schema, no row count and no credential")
            result_id = _new_result_id()
            path = _result_path(-1, result_id).with_suffix(".blob")
            get_blob(uri, path)
            with self._lock:
                self._blobs[result_id] = path
            return ResultRef(
                backend_id=self.id,
                result_id=result_id,
                schema=[],
                row_count=None,
                state="materialised",
                location=result_id.encode("utf-8"),
                kind="blob",
            )
        if schema is None:
            raise ValueError("A table import needs its schema: it is the caller's, never read from the file")
        return self._hold(pl.scan_parquet(uri, **_credential_kwargs(credential)), row_count=row_count, schema=schema)

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
        if ref.kind == "blob":
            raise OperationUnsupported(
                f"Result {ref.result_id} is a blob: it has no rows to plan over, sample or count"
            )
        with self._lock:
            plan = self._results.get(_store_key(ref))
        if plan is None:
            raise ResultMissing(f"The {self.id!r} backend no longer holds result {ref.result_id!r}")
        return plan

    def _blob(self, ref: ResultRef) -> Path:
        """The copy a blob ``ref`` is held as."""
        with self._lock:
            path = self._blobs.get(_store_key(ref))
        if path is None:
            raise ResultMissing(f"The {self.id!r} backend no longer holds result {ref.result_id!r}")
        return path

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

    def _store(self, plan: pl.LazyFrame, *, result_id: str, flow_id: int) -> ResultRef:
        """Stream ``plan`` to its file under core's storage directory and hold a scan of that file.

        The sink writes to a partial file beside the result's, which is moved into place only once the sink has
        returned, so a sink that fails never leaves a file at the result's path. Polars can still write the partial
        file after its error is raised; the result's path is registered under the id before the sink runs, so the
        id's next ``release`` or ``submit`` removes that file, as does the startup sweep of the cache directory. The
        schema is the plan's, never read back from the file; the row count is left to ``count``.
        """
        schema = _flowfile_schema(plan.collect_schema())
        path = _result_path(flow_id, result_id)
        partial = _partial_path(path)
        with self._lock:
            self._files[result_id] = path
        plan.sink_ipc(partial, mkdir=True)
        partial.replace(path)
        ref = ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=schema,
            row_count=None,
            state="materialised",
            location=result_id.encode("utf-8"),
        )
        with self._lock:
            self._results[result_id] = pl.scan_ipc(path)
        return ref

    def _drop(self, key: str) -> None:
        """Forget what the store holds under ``key`` and remove the files a store under it wrote, or a blob's copy."""
        with self._lock:
            self._results.pop(key, None)
            path = self._files.pop(key, None)
            blob = self._blobs.pop(key, None)
        if path is not None:
            path.unlink(missing_ok=True)
            _partial_path(path).unlink(missing_ok=True)
        if blob is not None:
            blob.unlink(missing_ok=True)

    def _hold_engine(self, engine: FlowDataEngine) -> ResultRef:
        """Store an engine's frame with the schema and the row count the engine already has."""
        known_count = engine.number_of_records if engine.number_of_records >= 0 else None
        return self._hold(engine.data_frame, row_count=known_count, schema=engine.schema)

    def _hold_plan(self, engine: FlowDataEngine) -> ResultRef:
        """Store an engine's frame deferred, as a plan, with the schema and the row count the engine already has.

        A computed frame is held as its ``.lazy()``, so its count still comes along. A caller releases a deferred ref
        as soon as it holds the plan, which keeps the store empty between operations.
        """
        known_count = engine.number_of_records if engine.number_of_records >= 0 else None
        return self._hold(engine.data_frame.lazy(), row_count=known_count, schema=engine.schema)

    def _hold_read(
        self, frame: pl.LazyFrame | pl.DataFrame, row_count: int | None = None, schema: Schema | None = None
    ) -> ResultRef:
        """Hold what a read produced as a deferred plan: a collected frame turned lazy, its height the row count."""
        if isinstance(frame, pl.DataFrame):
            return self._hold(frame.lazy(), row_count=frame.height, schema=schema)
        return self._hold(frame, row_count=row_count, schema=schema)

    def _read_database(self, payload: OpPayload) -> OpResult:
        """Read a database table or query, its password decrypted here.

        Payload: a ``DatabaseReadPayload``; its ``cancel_check`` is called while reading, ``True`` stops the read.

        Returns the refs ``{"result": ref}``.
        """
        read = unpack(payload, DatabaseReadPayload)
        database_settings = read.settings.database_settings
        source = SqlSource(
            connection_string=_sql_uri(read.connection, read.encrypted_password),
            query=None if database_settings.query_mode == "table" else database_settings.query,
            table_name=database_settings.table_name,
            schema_name=database_settings.schema_name,
            fields=read.settings.fields,
            cancel_check=read.cancel_check,
            database_type=read.connection.database_type,
        )
        return OpResult(refs={"result": self._hold_read(source.get_pl_df())})

    def _read_kafka(self, payload: OpPayload) -> OpResult:
        """Consume a Kafka topic without committing, spilling the rows to an Arrow file as they arrive.

        Payload: a ``KafkaReadPayload``; its ``read_kafka_source`` consumes, its secrets decrypted here.

        Returns the refs ``{"result": ref}``, and ``metadata["offsets"]``, the offsets to commit, when the read
        consumed messages.
        """
        read = unpack(payload, KafkaReadPayload)
        fd, spill_file = tempfile.mkstemp(suffix=".arrow", prefix="kafka_")
        os.close(fd)
        rows, consumed = read.read_kafka_source(
            read.settings, commit=False, decrypt_fn=_decrypted, spill_path=spill_file
        )
        ref = self._hold_read(rows)
        if consumed.messages_consumed > 0:
            return OpResult(refs={"result": ref}, metadata={"offsets": consumed.new_offsets})
        return OpResult(refs={"result": ref})

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
        """Fetch a REST API, its auth secret decrypted here.

        Payload: a ``RestApiReadPayload``.

        Returns the refs ``{"result": ref}``.
        """
        # Function-local, so importing this backend does not load the HTTP client.
        from shared.rest_api.fetch import fetch_rest_api

        read = unpack(payload, RestApiReadPayload)
        request = build_rest_api_worker_settings(read.settings, read.encrypted_secret)
        secret = _decrypted(read.encrypted_secret) if read.encrypted_secret else None
        return OpResult(refs={"result": self._hold_read(fetch_rest_api(request, secret=secret))})

    def _read_file(self, payload: OpPayload) -> OpResult:
        """Read a local file through the engine's readers.

        Payload: a ``FileReadPayload``; the Excel reader reports the engine it used to its ``node_logger``.

        Returns the refs ``{"result": ref}``, with the row count when the reader knows it without reading the rows.
        """
        read = unpack(payload, FileReadPayload)
        engine = FlowDataEngine.create_from_path(read.settings.received_file, node_logger=read.node_logger)
        known_count = engine.number_of_records if engine.number_of_records >= 0 else None
        ref = self._hold_read(engine.data_frame, row_count=known_count, schema=engine.schema)
        return OpResult(refs={"result": ref})

    def _write_file(self, payload: OpPayload) -> OpResult:
        """Write the rows of the ``FileWritePayload`` under ``"payload"`` to its file. Returns no refs.

        A parquet file is flushed to disk (``os.fsync``) before this returns, so a reader in another process, such as
        a kernel container reading its inputs, finds it complete.
        """
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
        if settings.file_type == "parquet":
            _fsync(settings.abs_file_path)
        return OpResult(refs={})

    def _fuzzy_join(self, payload: OpPayload) -> OpResult:
        """Fuzzy-match the left rows of the ``FuzzyJoinPayload`` under ``"payload"`` against its right rows.

        A copy of the settings' ``join_input`` is used, and the matcher reports its progress to the payload's node
        logger. Returns the refs ``{"result": ref}``, a deferred result.
        """
        join = unpack(payload, FuzzyJoinPayload)
        engine = FlowDataEngine(join.left).fuzzy_join(
            fuzzy_match_input=deepcopy(join.settings.join_input),
            other=FlowDataEngine(join.right),
            node_logger=join.node_logger,
        )
        return OpResult(refs={"result": self._hold_engine(engine)})

    def _random_split(self, payload: OpPayload) -> OpResult:
        """Partition the rows of the ``RandomSplitPayload`` under ``"payload"`` at random into its labelled splits.

        The shuffled rows are computed once, so every split slices the same shuffle. Returns one deferred ref per split,
        keyed by the split's name, in the order the settings list them.
        """
        split = unpack(payload, RandomSplitPayload)
        split_pairs = [(group.name, group.percentage) for group in split.settings.splits]
        outputs = FlowDataEngine(split.plan).random_split(split_pairs, split.settings.seed)
        return OpResult(refs={name: self._hold_engine(engine) for name, engine in outputs.outputs.items()})

    def _run_custom_node(self, payload: OpPayload) -> OpResult:
        """Run ``process`` of the ``CustomNodePayload`` under ``"payload"`` on a fresh instance of its node's class.

        The instance is built from the payload's ``settings_values`` and, when the payload names a user, reads that
        user's secrets through core's secret store as it accesses them. The source and secret fields a backend in
        another process needs are not read.

        Returns the refs ``{"result": ref}`` when ``process`` returns one frame; one ref per declared output, keyed by
        its output handle (``output-0``, ``output-1``, ...), when it returns a dict of them; no refs otherwise. Every
        ref is deferred, holding the frame ``process`` returned as a plan, with its row count when the frame was
        computed already.

        Raises:
            ValueError: ``process`` returned a dict without one of the declared outputs.
        """
        job = unpack(payload, CustomNodePayload)
        instance = type(job.custom_node).from_settings(job.settings_values)
        if job.user_id is not None:
            instance.set_execution_context(job.user_id)

        output = instance.process(*job.inputs)

        accessed_secrets = instance.get_accessed_secrets()
        if accessed_secrets:
            logger.info(f"Node '{job.node_id}' accessed secrets: {accessed_secrets}")
        if isinstance(output, dict):
            engines: dict[str, FlowDataEngine] = {}
            for i, name in enumerate(job.output_names):
                if name not in output:
                    raise ValueError(f"process() did not return declared output '{name}'")
                engines[output_handle(i)] = FlowDataEngine(output[name])
            return OpResult(refs={handle: self._hold_plan(engine) for handle, engine in engines.items()})
        if isinstance(output, pl.LazyFrame | pl.DataFrame):
            return OpResult(refs={"result": self._hold_plan(FlowDataEngine(output))})
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
        """Write the rows of the ``DeltaWritePayload`` under ``"payload"`` to its Delta table.

        The plan streams into the table. Returns no refs; the metadata is the written rows' report
        (``_written_report``), or ``{"skipped": True}`` for an append of no rows to a table with the same columns.
        """
        write = unpack(payload, DeltaWritePayload)
        path = _delta_path(write)
        storage_options = _storage_options(write.connection)
        wrote = write_delta(
            write.plan,
            path,
            mode=write.mode,
            partition_by=write.partition_by,
            storage_options=storage_options,
            enable_cdf=write.enable_cdf,
        )
        if not wrote:
            return OpResult(refs={}, metadata={"skipped": True})
        return OpResult(refs={}, metadata=_written_report(FlowDataEngine(write.plan), path, storage_options))

    def _merge_delta(self, payload: OpPayload) -> OpResult:
        """Merge the rows of the ``DeltaMergePayload`` under ``"payload"`` into its Delta table by key.

        The rows are collected first, as the merge needs them in memory. Returns no refs; the metadata is the merged
        rows' report (``_written_report``), or ``{"skipped": True}`` for an update of no rows on a table with the same
        columns.
        """
        write = unpack(payload, DeltaMergePayload)
        path = _delta_path(write)
        storage_options = _storage_options(write.connection)
        rows = write.plan.collect()
        wrote = merge_into_delta(
            rows,
            path,
            merge_mode=write.merge_mode,
            merge_keys=write.merge_keys,
            partition_by=write.partition_by,
            storage_options=storage_options,
            enable_cdf=write.enable_cdf,
        )
        if not wrote:
            return OpResult(refs={}, metadata={"skipped": True})
        return OpResult(refs={}, metadata=_written_report(FlowDataEngine(rows), path, storage_options))

    def _scd2_delta(self, payload: OpPayload) -> OpResult:
        """Apply the rows of the ``DeltaScd2Payload`` under ``"payload"`` to its Delta table as an SCD2 write.

        The rows are collected first, as classifying them against the table needs them in memory. Returns no refs.
        The metadata is the table's own shape after the write (an SCD2 table's rows are its whole history): its
        ``schema``, ``row_count`` and ``column_count`` with the ``table_path``, ``size_bytes``, the Delta ``version``
        committed and the ``scd2_metrics``; a batch that changes nothing commits nothing and reports
        ``{"skipped": True, "version": ...}``, the version it was classified against.
        """
        write = unpack(payload, DeltaScd2Payload)
        path = _delta_path(write)
        storage_options = _storage_options(write.connection)
        result = scd2_into_delta(
            write.plan.collect(),
            path,
            business_keys=write.business_keys,
            valid_from_iso=write.run_timestamp,
            compare_columns=write.compare_columns,
            full_snapshot=write.full_snapshot,
            surrogate_key_column=write.surrogate_key_column,
            valid_from_column=write.valid_from_column,
            valid_to_column=write.valid_to_column,
            is_current_column=write.is_current_column,
            partition_by=write.partition_by,
            partition_on_current=write.partition_on_current,
            storage_options=storage_options,
        )
        if result.skipped:
            return OpResult(refs={}, metadata={"skipped": True, "version": result.version})
        table_schema = pl.scan_delta(
            path, **({} if storage_options is None else {"storage_options": storage_options})
        ).collect_schema()
        return OpResult(
            refs={},
            metadata={
                "table_path": path,
                "schema": [{"name": name, "dtype": str(dtype)} for name, dtype in table_schema.items()],
                "row_count": result.rows_total,
                "column_count": len(table_schema),
                "size_bytes": get_delta_size_bytes(path, storage_options=storage_options),
                "version": result.version,
                "scd2_metrics": {
                    "rows_inserted": result.rows_inserted,
                    "rows_closed": result.rows_closed,
                    "rows_total": result.rows_total,
                    "rows_current": result.rows_current,
                    "created": result.created,
                },
            },
        )

    def _ml_train(self, payload: OpPayload) -> OpResult:
        """Fit the model of the ``MlTrainPayload`` under ``"payload"`` to its rows in this process, and hold it.

        The trainer is the one the worker runs (``shared.ml.trainers``). Its bytes are written under a fresh id to
        ``cache/local/-1/<result_id>.blob`` under core's storage directory and held as a blob, as an imported one is,
        so each fit is a model of its own. Returns the refs ``{"model": ref}`` and, as ``metadata``, the model's
        ``sha256``, ``size_bytes`` and ``model_type``.
        """
        train = unpack(payload, MlTrainPayload)
        model = get_trainer(train.model_type).train(
            train.plan, target=train.target_column, features=train.feature_columns, params=train.params or {}
        )
        result_id = _new_result_id()
        path = _result_path(-1, result_id).with_suffix(".blob")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(model)
        with self._lock:
            self._blobs[result_id] = path
        ref = ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=[],
            row_count=None,
            state="materialised",
            location=result_id.encode("utf-8"),
            kind="blob",
        )
        report = {"sha256": hashlib.sha256(model).hexdigest(), "size_bytes": len(model), "model_type": train.model_type}
        return OpResult(refs={"model": ref}, metadata=report)

    def _ml_apply(self, payload: OpPayload) -> OpResult:
        """Score the rows of the ``MlApplyPayload`` under ``"payload"`` with the model blob this backend holds.

        The model's copy is read as the JSON its trainer wrote, and the trainer adds the prediction as the payload's
        output column. Returns the refs ``{"result": ref}``, the scored plan held deferred.

        Raises:
            ForeignRef: Another backend holds the model.
            ResultMissing: This backend no longer holds the model.
        """
        apply = unpack(payload, MlApplyPayload)
        self._check_backend(apply.model.backend_id)
        model = json.loads(self._blob(apply.model).read_bytes())
        scored = get_trainer(model["model_type"]).apply(apply.plan, model, apply.output_column)
        return OpResult(refs={"result": self._hold(scored)})

    _OPERATIONS: ClassVar[dict[OperationKind, Callable[[LocalBackend, OpPayload], OpResult]]] = {
        OperationKind.database_read: _read_database,
        OperationKind.kafka_read: _read_kafka,
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
        OperationKind.delta_scd2: _scd2_delta,
        OperationKind.ml_train: _ml_train,
        OperationKind.ml_apply: _ml_apply,
    }


def _new_result_id() -> str:
    return uuid.uuid4().hex


def _store_key(ref: ResultRef) -> str:
    return ref.location.decode("utf-8")


def _result_path(flow_id: int, result_id: str) -> Path:
    """Where a stored result's file is written: ``cache/local/<flow_id>/<result_id>.arrow`` in core's storage."""
    return storage.cache_directory / "local" / str(flow_id) / f"{result_id}.arrow"


def _partial_path(path: Path) -> Path:
    """The file a stored result's sink writes beside ``path`` before the result is moved there."""
    return path.with_name(f"{path.name}.part")


def _credential_kwargs(credential: CredentialRef | None) -> dict[str, Any]:
    """The ``credential_provider`` a sink or scan of a URI takes for ``credential``; nothing without one.

    The provider carries the stored connection's id and user only, and resolves it where the plan runs. Without a
    credential Polars resolves the target as it does for any scan.
    """
    if credential is None:
        return {}
    return {"credential_provider": CredentialRefProvider(credential.connection_id, credential.user_id)}


def _flowfile_schema(schema: pl.Schema) -> Schema:
    """A Polars schema as the ``FlowfileColumn`` list a ``FlowDataEngine`` builds from it."""
    return convert_stats_to_column_info(FlowDataEngine._create_schema_stats_from_pl_schema(schema))


def _fsync(path: str) -> None:
    """Flush the file at ``path`` to disk; ``"rb+"`` because ``os.fsync`` needs a writable descriptor on Windows."""
    with open(path, "rb+") as file:
        os.fsync(file.fileno())


def _ipc_stream(frame: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.write_ipc_stream(buffer)
    return buffer.getvalue()


def _decrypted(encrypted: str) -> str:
    return decrypt_secret(encrypted).get_secret_value()


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


def _delta_path(write: DeltaWritePayload | DeltaMergePayload | DeltaScd2Payload) -> str:
    """The Delta table a write names: its ``path``, or its catalog table under the catalog's storage root.

    A catalog table's name is a bare directory name, joined onto the identity's object-storage root or onto this
    process's catalog tables directory the way core's catalog joins a table's directory; a name with a path separator
    or ``..`` raises ``ValueError``.
    """
    table = write.table
    if table is None:
        return write.path
    if table.base_uri is not None:
        return validate_catalog_uri(table.table_name, table.base_uri)
    validate_catalog_path(table.table_name, storage.catalog_tables_directory)
    return str(storage.catalog_tables_directory / table.table_name)


def _storage_options(connection: FullCloudStorageConnectionWorkerInterface | None) -> dict[str, Any] | None:
    """The object-storage options of ``connection``; ``None`` for a table on the local filesystem."""
    if connection is None:
        return None
    return CloudStorageReader.get_storage_options(_decrypted_connection(connection))


def _written_report(rows: FlowDataEngine, path: str, storage_options: dict[str, Any] | None) -> dict[str, Any]:
    """What a Delta write or merge of ``rows`` into the table at ``path`` reports.

    The schema and the counts are those of the rows written, the size that of the whole table afterwards.
    """
    return {
        "table_path": path,
        "schema": [{"name": column.column_name, "dtype": column.data_type} for column in rows.schema],
        "row_count": rows.count(),
        "column_count": rows.number_of_fields,
        "size_bytes": get_delta_size_bytes(path, storage_options=storage_options),
    }
