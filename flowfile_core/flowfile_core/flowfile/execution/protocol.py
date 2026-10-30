"""The interface between core and a compute, and the values that cross it.

A compute (core's own process, a worker, a cluster) holds results; core holds ``ResultRef``s to them.
Rows reach core only through ``ComputeBackend.sample`` and the sample ``submit`` returns inline. A plan crosses
this interface as the ``pl.LazyFrame`` itself; a backend that hands it to another process serialises it there,
so only its wire carries bytes.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, TypeAlias, runtime_checkable

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from shared.compute_protocol import Capabilities

Schema: TypeAlias = list[FlowfileColumn]
"""The columns of a result, in order."""

OpPayload: TypeAlias = dict[str, Any]
"""What one operation needs: ``{"payload": <the kind's dataclass from execution/payloads.py>}``, for every backend.

``payloads.unpack`` takes the dataclass out and raises ``TypeError``/``ValueError`` naming the key when it is wrong."""


@dataclass(frozen=True)
class ResultRef:
    """A result one backend holds, named without its rows.

    Attributes:
        backend_id: The backend that minted the reference; no other backend can read it.
        result_id: The producing node's hash, plus a suffix for a derived result (one output of several).
        schema: The result's columns.
        row_count: The number of rows, when the backend knows it without computing the result.
        state: ``"materialised"`` when the backend holds the rows, ``"deferred"`` when it holds a plan
            that computes them on read.
        location: Where the backend keeps the result; opaque to core, only the minting backend reads it.
        kind: ``"table"`` for rows. ``"blob"`` for bytes that are not rows (a fitted model): materialised, with no
            columns and no row count, and never planned over, sampled or counted.
    """

    backend_id: str
    result_id: str
    schema: Schema
    row_count: int | None
    state: Literal["materialised", "deferred"]
    location: bytes
    kind: Literal["table", "blob"] = "table"


@dataclass(frozen=True)
class SubmitHints:
    """What core knows about a plan that a backend may use to run it.

    Each backend's class docstring says which hints its materialisation policy reads.

    Attributes:
        must_materialise: Compute and hold the rows rather than keep a deferred plan.
        wide: The plan reshapes data across rows (join, group by, sort, pivot) rather than mapping each row.
        fanout: How many distinct plans of this run execute the result; a backend may store a wide result
            several plans would otherwise each compute again.
        preview_rows: The most rows the inline sample may hold; ``None`` asks for no sample.
    """

    must_materialise: bool
    wide: bool
    fanout: int
    preview_rows: int | None


def worker_stores(hints: SubmitHints) -> bool:
    """Whether the worker's policy (``flowfile_worker.service._materialise``) stores a plan submitted with ``hints``.

    Both worker backends apply it, and ``hints_for`` reads it to tell which consumers store their result.
    """
    return hints.must_materialise or (hints.wide and (hints.preview_rows is not None or hints.fanout > 1))


@dataclass(frozen=True)
class JobHandle:
    """Names one job on one backend."""

    backend_id: str
    job_id: str


@dataclass(frozen=True)
class JobStatus:
    """The state of a job.

    Attributes:
        state: ``"running"`` until the job ends as ``"done"``, ``"failed"`` or ``"cancelled"``.
        progress: Percent complete, when the backend reports it.
        error: Why the job failed; ``None`` unless ``state`` is ``"failed"``.
    """

    state: Literal["running", "done", "failed", "cancelled"]
    progress: int | None
    error: str | None


@dataclass(frozen=True)
class SubmitResult:
    """What ``ComputeBackend.submit`` returns.

    Attributes:
        ref: The result the backend now holds.
        sample: At most ``SubmitHints.preview_rows`` rows of it as an Arrow IPC stream; ``None`` when no
            sample was asked for.
        handle: The job that produced it.
    """

    ref: ResultRef
    sample: bytes | None
    handle: JobHandle


@dataclass(frozen=True)
class OpResult:
    """What ``ComputeBackend.run_op`` returns.

    Attributes:
        refs: The results the operation produced, by name; empty for an operation that only writes.
        metadata: What the operation reports besides rows, such as a trained model's digest or a Delta write's
            version and counts.
    """

    refs: dict[str, ResultRef]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CredentialRef:
    """Names a stored connection a plan or operation may use, by its owner and id.

    The executor resolves it to the secret where the work runs; the reference never carries the secret.
    """

    connection_id: int
    user_id: int


class OperationKind(enum.Enum):
    """The operations a backend runs whole rather than as a Polars plan.

    Each reads or writes an external system, or runs code a serialised plan cannot carry. The set is
    closed; a backend that does not implement one raises ``OperationUnsupported``.
    """

    database_read = "database_read"
    kafka_read = "kafka_read"
    google_analytics_read = "google_analytics_read"
    rest_api_read = "rest_api_read"
    database_write = "database_write"
    cloud_write = "cloud_write"
    file_write = "file_write"
    file_read = "file_read"
    fuzzy_join = "fuzzy_join"
    custom_node = "custom_node"
    random_split = "random_split"
    ml_train = "ml_train"
    ml_apply = "ml_apply"
    polars_code = "polars_code"
    sql_query = "sql_query"
    delta_write = "delta_write"
    delta_merge = "delta_merge"
    delta_scd2 = "delta_scd2"
    viz_session = "viz_session"


@runtime_checkable
class ComputeBackend(Protocol):
    """A compute core runs work on and reads results from.

    A ``ResultRef`` is valid only on the backend whose ``id`` it carries: every method that takes one raises
    ``ForeignRef`` for another backend's reference and ``ResultMissing`` for one this backend no longer holds. A blob
    (``kind="blob"``) has no rows, so ``plan_for``, ``sample``, ``count`` and ``submit`` (among its inputs) refuse it
    with ``OperationUnsupported``. Its bytes cross between backends through ``export`` and ``import_uri``.

    Attributes:
        id: The id the registry knows this backend by.
        protocol_version: The protocol version this backend speaks (``shared.compute_protocol.PROTOCOL_VERSION``).
    """

    id: str
    protocol_version: int

    def capabilities(self) -> Capabilities:
        """What this compute runs (versions, platform, plugins), in the fields a handshake compares."""
        ...

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
        """Run a Polars plan over ``inputs``, hold its result and return a reference to it.

        Args:
            plan: The plan; a backend that runs it in another process serialises it when it sends it.
            inputs: The results of this backend the plan reads.
            hints: What core knows about the plan, for the backend's materialisation policy.
            result_id: The id to mint the result under, the producing node's hash. Reusing a held id replaces that
                result.
            flow_id: The flow the compute files the job and its logs under.
            node_id: The node the compute files the job and its logs under.
        """
        ...

    def run_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        """Run one operation and return the results it produced by name, with what it reports besides them."""
        ...

    def sample(self, ref: ResultRef, n: int) -> bytes:
        """At most ``n`` rows of ``ref`` as an Arrow IPC stream; with ``submit``'s sample, the only rows core sees.

        A blob is refused with ``OperationUnsupported``.
        """
        ...

    def count(self, ref: ResultRef) -> int:
        """The number of rows in ``ref``; a blob is refused with ``OperationUnsupported``."""
        ...

    def exists(self, ref: ResultRef) -> bool:
        """Whether this backend still holds ``ref``; ``False`` only when it answered that it does not."""
        ...

    def release(self, ref: ResultRef) -> None:
        """Let this backend drop ``ref``; a later read of it raises ``ResultMissing``."""
        ...

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        """A LazyFrame leaf that reads ``ref``, valid in plans this backend runs and nowhere else.

        A blob is refused with ``OperationUnsupported``: it has no rows to plan over.
        """
        ...

    def export(self, ref: ResultRef, uri: str, credential: CredentialRef | None) -> str:
        """Write ``ref`` to ``uri``, through ``credential`` when the target needs one, and return the URI written.

        A table is written as parquet. A blob's bytes are copied as they are to a ``file://`` URI or ``PUT`` to a
        presigned ``https://`` URL, which carries its own authorisation, so a blob takes no ``credential``.
        """
        ...

    def import_uri(
        self,
        uri: str,
        schema: Schema | None,
        row_count: int | None,
        credential: CredentialRef | None,
        kind: Literal["table", "blob"] = "table",
    ) -> ResultRef:
        """A reference on this backend to the data at ``uri``, which has ``schema`` and ``row_count`` rows.

        A table's schema is the caller's and required. With ``kind="blob"`` the bytes at a ``file://`` URI or a
        presigned ``https://`` URL are copied into a blob this backend holds; a blob import has no schema, no row
        count and no credential.
        """
        ...

    def status(self, handle: JobHandle) -> JobStatus:
        """The state of the job ``handle`` names."""
        ...

    def cancel(self, handle: JobHandle) -> None:
        """Stop the job ``handle`` names; a job that already ended is left as it is."""
        ...
