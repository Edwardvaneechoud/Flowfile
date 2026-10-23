"""The Polars backend: core's client of a Polars cluster the operator runs, reached at its scheduler's address.

A ``compute_backends`` row of kind ``polars-cloud``, id ``polars-cloud:<name>``, names in its JSON settings the
scheduler's URI and, optionally, its dashboard (Polars' observatory, which polars-cloud otherwise looks for on the
scheduler's host at port 3001); the registry makes a ``PolarsCloudBackend`` from those settings on the first lookup of
its id. Core hands the cluster plain Polars plans through ``polars_cloud.ClusterContext`` and ``LazyFrame.remote``; the
cluster runs no operation, so ``run_op`` raises ``OperationUnsupported`` and placement refuses every node that runs one.
The cluster reads and writes ``s3`` with its pods' own identity (``credential_resolution="ambient"``), so no plan it
gets carries a credential: the storage options of a sink or scan the backend adds are the exchange store connection's
plain ones (region, endpoint, plain HTTP, TLS; ``credential_ref_kwargs`` with every key and the provider dropped), and a
plan core built names its reads' connections only as placement allows on an ambient backend. Neither carries a Python
credential provider (``credential_provider=None``, and ``native_credentials`` for the reads core builds): a provider
pickled under core's Python does not load on a cluster of another, and the cluster's native chain finds its own
identity. A self-run scheduler has no client authentication and its plans carry user code, so a row's addresses must be
loopback or private (``address_refusal``).

The capabilities are constants of the Polars On-Prem 0.7.1 image, not asked of the cluster: Polars 1.43.2 on the 64-bit
index runtime, Python 3.11.12, linux-x86_64, the plugins ``polars_ds`` 0.13.0, ``polars_grouper`` 0.6.1 and
``polars_expr_transformer`` 0.6.4 with the library paths their plans carry there. The handshake compares them with
core's and runs its probe plans on the cluster like on any backend.

Nothing connects until a call needs the cluster (a submit, a handshake probe, a sample, a count, an export): the
cluster context is made then, once, after a 3 s TCP probe of the scheduler's port. polars-cloud 0.10.0's
``ClusterContext`` connects in its constructor and retries an unreachable scheduler for 60 s holding the GIL, which
stalls every route of core; the probe turns a closed port into an immediate ``ComputeUnavailable``. A port that still
accepts while no scheduler answers behind it (a port-forward to a gone pod) passes the probe and meets that stall. A
call whose error reads as the transport's (``_UNREACHABLE``) is a ``ComputeUnavailable`` too, and the context is made
again on the next call; any other failure of a query is a ``TaskFailed`` with the cluster's message.

Materialisation policy: a plan is held deferred, as the plan itself; its schema and the inline sample come from one
remote ``show(preview_rows)``, or ``show(0)`` when no preview is asked. With ``must_materialise`` set it is first sunk
by a single node to ``<exchange uri>/<flow id>/results/<result id>.parquet`` and held as a scan of that object with its
schema and the sink's row count, which needs an exchange store (``TaskFailed`` without one, before any call).
``release``, and a submit under an id already held, deletes that object through the exchange store, with core's
resolution of the store's connection (``exchange.remove_prefix``); a deletion that fails is logged and left to the
bucket's lifecycle rule. ``exists`` answers from what this instance holds, without a call. ``sample`` and ``count``
are one remote ``show`` each (``count`` none when the row count is known), ``export`` sinks a held plan to the URI it
is given, ``import_uri`` holds a scan of one as a deferred result under a fresh id; a blob is refused. ``status``
answers done and ``cancel`` does nothing: every call blocks until the cluster answers.
"""

from __future__ import annotations

import io
import ipaddress
import json
import socket
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from typing import Any, Literal
from urllib.parse import urlsplit

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.flowfile.execution import exchange
from flowfile_core.flowfile.execution.backends.worker import _flowfile_schema
from flowfile_core.flowfile.execution.errors import (
    ComputeUnavailable,
    ForeignRef,
    OperationUnsupported,
    ResultMissing,
    TaskFailed,
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
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import safe_eval_pl_type
from shared._version import get_version
from shared.cloud_credential_provider import credential_ref_kwargs
from shared.cloud_storage.storage_options import ALLOW_INVALID_CERTIFICATES
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities

DEFAULT_SCHEDULER_PORT = 5051
PROBE_TIMEOUT = 3.0
"""Seconds the TCP probe of the scheduler's port waits before the backend counts the cluster unreachable."""

IMAGE = "Polars On-Prem 0.7.1"
IMAGE_CAPABILITIES = {
    "polars_version": "1.43.2",
    "polars_runtime": "64",
    "python_version": "3.11.12",
    "platform": "linux-x86_64",
    "plugins": {"polars_ds": "0.13.0", "polars_grouper": "0.6.1", "polars_expr_transformer": "0.6.4"},
    "plugin_libs": {
        "polars_grouper": "lib/python3.11/site-packages/polars_grouper/_internal.abi3.so",
        "polars_ds": "lib/python3.11/site-packages/polars_ds/_polars_ds.abi3.so",
    },
}
"""What the ``IMAGE`` runs, as measured on a cluster of it; a cluster of another image needs these changed."""

PRIVATE_NETWORKS = tuple(
    ipaddress.ip_network(network)
    for network in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7", "fe80::/10")
)
_PLAIN_OPTIONS = frozenset(
    {"aws_region", "endpoint_url", "aws_endpoint_url", "aws_allow_http", ALLOW_INVALID_CERTIFICATES}
)
_UNREACHABLE = ("transport error", "tcp connect error", "connection refused", "error trying to connect", "unavailable")

ContextFactory = Callable[[str, "str | None"], Any]


def address_refusal(url: str) -> str | None:
    """Why a self-run Polars address *url* is refused, or ``None`` when its host is loopback or private.

    The host is resolved, and every address it resolves to must be loopback or in ``PRIVATE_NETWORKS``; TLS changes
    nothing, as the scheduler lets in whoever reaches it.
    """
    host = urlsplit(url).hostname
    if not host:
        return f"{url} names no host"
    try:
        found = {info[4][0] for info in socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)}
    except OSError as e:
        return f"its host {host} does not resolve ({e})"
    for address in sorted(found):
        ip = ipaddress.ip_address(address.partition("%")[0])
        if getattr(ip, "ipv4_mapped", None) is not None:
            ip = ip.ipv4_mapped
        if not ip.is_loopback and not any(ip in network for network in PRIVATE_NETWORKS):
            named = host if host == address else f"{host} ({address})"
            return (
                f"{named} is neither loopback nor in a private range (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, "
                "fc00::/7, fe80::/10): a self-run Polars scheduler has no client authentication, so anyone who "
                "reaches it runs code on the cluster, TLS or not"
            )
    return None


def _cluster_context(scheduler_url: str, dashboard_url: str | None) -> Any:
    """``polars_cloud.ClusterContext`` on *scheduler_url*, its observatory at *dashboard_url* when one is given."""
    import polars_cloud as pc

    observatory = pc.ClientOptions(uri=dashboard_url) if dashboard_url else None
    return pc.ClusterContext(uri=scheduler_url, observatory=observatory)


def _ipc_stream(frame: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.write_ipc_stream(buffer)
    return buffer.getvalue()


def _plain_options(credential: CredentialRef | None) -> dict[str, str]:
    """The storage options of the connection *credential* names that carry no secret; none without one."""
    if credential is None:
        return {}
    options = credential_ref_kwargs(credential.connection_id, credential.user_id).get("storage_options", {})
    return {key: value for key, value in options.items() if key in _PLAIN_OPTIONS}


class PolarsCloudBackend:
    """``ComputeBackend`` for the Polars compute ``settings`` describe, a ``polars-cloud`` row's settings; the module
    docstring has the policy.

    The settings are read in one place, ``_make_context``, which makes the client context every call runs in, on the
    first call that needs it and never in the constructor. Their one shape today is a self-run cluster's
    ``scheduler_url`` and optional ``dashboard_url``, the context made by ``connect`` (``_cluster_context`` unless
    given) after the TCP probe; nothing else in the class knows how the compute is reached, so another shape is another
    branch there. ``url`` is the scheduler URL, by which the handshake forgets this backend's verdicts.

    Raises:
        ValueError: ``settings`` name no ``scheduler_url``.
    """

    protocol_version = PROTOCOL_VERSION
    native_credentials = True
    """A read core builds for this backend names no Python credential provider (``NATIVE_CREDENTIALS``): the cluster's
    native chain finds its own identity, and a provider pickled under core's Python would not load on the cluster's."""

    def __init__(self, backend_id: str, settings: Mapping[str, Any], *, connect: ContextFactory | None = None) -> None:
        if not settings.get("scheduler_url"):
            raise ValueError(f"The settings of the {backend_id!r} backend name no scheduler_url")
        self.id = backend_id
        self._settings = dict(settings)
        self._connect = connect or _cluster_context
        self._context: Any = None
        self._lock = threading.Lock()
        self._results: dict[str, pl.LazyFrame] = {}
        self._staged: dict[str, tuple[exchange.ExchangeStore, str]] = {}

    @classmethod
    def from_row(cls, backend_id: str, settings: str | None) -> PolarsCloudBackend:
        """The backend a ``compute_backends`` row configures from its JSON ``settings``.

        Raises:
            ValueError: The settings are no JSON object naming a ``scheduler_url``.
        """
        parsed = json.loads(settings or "{}")
        if not isinstance(parsed, dict):
            raise ValueError(f"The settings of the {backend_id!r} backend are no JSON object")
        return cls(backend_id, parsed)

    @property
    def url(self) -> str:
        return self._settings["scheduler_url"]

    def capabilities(self) -> Capabilities:
        """The ``IMAGE``'s constants: no operation, ``s3`` read and written with the pods' own identity."""
        return Capabilities(
            app_version=get_version(),
            protocol_version=PROTOCOL_VERSION,
            operations=frozenset(),
            read_schemes=frozenset({"s3"}),
            write_schemes=frozenset({"s3"}),
            credential_resolution="ambient",
            **IMAGE_CAPABILITIES,
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
        """Hold ``plan`` under ``result_id``, deferred or sunk to the exchange store by ``hints.must_materialise``.

        Raises:
            ComputeUnavailable: The cluster cannot be reached.
            ForeignRef: An input another backend minted.
            ResultMissing: This backend no longer holds an input.
            TaskFailed: The cluster failed the query, or the result must be materialised and no exchange store is
                configured (or it is misconfigured).
        """
        for ref in inputs:
            self._plan(ref)
        store = None
        if hints.must_materialise:
            try:
                store = exchange.exchange_store()
            except ValueError as e:
                raise TaskFailed(str(e)) from e
            if store is None:
                raise TaskFailed(
                    f"The {self.id!r} backend keeps a cached result in the exchange store, and none is configured: "
                    "set FLOWFILE_EXCHANGE_URI and FLOWFILE_EXCHANGE_CONNECTION, or turn off caching on node "
                    f"{node_id}"
                )
        self._drop(result_id)
        row_count = None
        held = plan
        if store is not None:
            uri = f"{store.uri}/{flow_id}/results/{result_id}.parquet"
            options = _plain_options(store.credential)
            with self._lock:
                self._staged[result_id] = (store, uri)
            row_count = self._sink(plan, uri, options, "sink")
        if hints.preview_rows is not None:
            frame = self._show(plan, hints.preview_rows)
            schema, sample = frame.schema, _ipc_stream(frame)
        else:
            schema, sample = self._show(plan, 0).schema, None
        if store is not None:
            held = pl.scan_parquet(uri, schema=schema, storage_options=options, credential_provider=None)
        with self._lock:
            self._results[result_id] = held
        ref = ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=_flowfile_schema(schema),
            row_count=row_count,
            state="materialised" if store is not None else "deferred",
            location=result_id.encode("utf-8"),
        )
        return SubmitResult(ref=ref, sample=sample, handle=JobHandle(backend_id=self.id, job_id=result_id))

    def run_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        raise OperationUnsupported(f"The {self.id!r} backend runs plain Polars plans only, not {op.value!r}")

    def sample(self, ref: ResultRef, n: int) -> bytes:
        """The first ``n`` rows of ``ref`` as an Arrow IPC stream, from one remote ``show``."""
        return _ipc_stream(self._show(self._plan(ref), max(n, 0)))

    def count(self, ref: ResultRef) -> int:
        """``ref``'s row count when it is known, else one remote ``show`` of ``pl.len()``."""
        plan = self._plan(ref)
        if ref.row_count is not None:
            return ref.row_count
        return self._show(plan.select(pl.len()), 1).item()

    def exists(self, ref: ResultRef) -> bool:
        """Whether this instance holds ``ref``; the cluster is not asked."""
        self._check_backend(ref.backend_id)
        with self._lock:
            return ref.result_id in self._results

    def release(self, ref: ResultRef) -> None:
        """Forget ``ref``, and delete its object from the exchange store when it was materialised."""
        self._check_backend(ref.backend_id)
        self._drop(ref.result_id)

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        """The held plan, or the scan of the materialised object; valid only in a plan sent to this cluster."""
        return self._plan(ref)

    def export(self, ref: ResultRef, uri: str, credential: CredentialRef | None) -> str:
        """Sink ``ref`` by one node to ``uri`` as parquet, with the plain options of ``credential``'s connection."""
        self._sink(self._plan(ref), uri, _plain_options(credential), "export")
        return uri

    def import_uri(
        self,
        uri: str,
        schema: Schema | None,
        row_count: int | None,
        credential: CredentialRef | None,
        kind: Literal["table", "blob"] = "table",
    ) -> ResultRef:
        """Hold a scan of the parquet at ``uri`` with ``schema`` as a deferred result under a fresh id.

        Raises:
            OperationUnsupported: A blob; the cluster holds tables only.
            ValueError: A table without ``schema``.
        """
        if kind == "blob":
            raise OperationUnsupported(f"The {self.id!r} backend does not hold blobs")
        if schema is None:
            raise ValueError("A table import needs its schema: it is the caller's, never read from the file")
        polars_schema = pl.Schema({column.column_name: safe_eval_pl_type(column.data_type) for column in schema})
        result_id = f"import_{uuid.uuid4().hex}"
        scan = pl.scan_parquet(
            uri, schema=polars_schema, storage_options=_plain_options(credential), credential_provider=None
        )
        with self._lock:
            self._results[result_id] = scan
        return ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=schema,
            row_count=row_count,
            state="deferred",
            location=result_id.encode("utf-8"),
        )

    def status(self, handle: JobHandle) -> JobStatus:
        return JobStatus(state="done", progress=100, error=None)

    def cancel(self, handle: JobHandle) -> None:
        return None

    def _ctx(self) -> Any:
        """The client context, made once, on first use (``_make_context``), and again after the cluster was lost."""
        with self._lock:
            if self._context is None:
                self._context = self._make_context()
            return self._context

    def _make_context(self) -> Any:
        """The client context the settings describe: a self-run cluster's, after a TCP probe of its scheduler's port.

        Raises:
            ComputeUnavailable: The port does not accept within ``PROBE_TIMEOUT``; no context was made.
        """
        scheduler_url = self._settings["scheduler_url"]
        address = urlsplit(scheduler_url)
        try:
            probe = socket.create_connection(
                (address.hostname, address.port or DEFAULT_SCHEDULER_PORT), timeout=PROBE_TIMEOUT
            )
        except OSError as e:
            raise ComputeUnavailable(
                f"Could not reach the Polars cluster at {scheduler_url}: {e}", backend_id=self.id
            ) from e
        probe.close()
        return self._connect(scheduler_url, self._settings.get("dashboard_url"))

    def _show(self, plan: pl.LazyFrame, n: int) -> pl.DataFrame:
        return self._run("show", lambda: plan.remote(self._ctx()).show(n))

    def _sink(self, plan: pl.LazyFrame, uri: str, options: dict[str, str], kind: str) -> int | None:
        def sink() -> int | None:
            query = (
                plan.remote(self._ctx())
                .single_node()
                .sink_parquet(uri, storage_options=options or None, credential_provider=None)
            )
            return query.await_result(silent=True).n_rows_total

        return self._run(kind, sink)

    def _run(self, kind: str, call: Callable[[], Any]) -> Any:
        started = time.monotonic()
        try:
            result = call()
        except ComputeUnavailable:
            raise
        except Exception as e:
            text = str(e)
            if any(marker in text.lower() for marker in _UNREACHABLE):
                with self._lock:
                    self._context = None
                raise ComputeUnavailable(
                    f"Could not reach the Polars cluster at {self.url}: {text}", backend_id=self.id
                ) from e
            raise TaskFailed(f"The Polars cluster failed the query: {type(e).__name__}: {text}") from e
        logger.debug("Polars cluster %s: %s took %.2f s", self.id, kind, time.monotonic() - started)
        return result

    def _check_backend(self, backend_id: str) -> None:
        if backend_id != self.id:
            raise ForeignRef(f"The {self.id!r} backend cannot read a result of the {backend_id!r} backend")

    def _plan(self, ref: ResultRef) -> pl.LazyFrame:
        self._check_backend(ref.backend_id)
        with self._lock:
            plan = self._results.get(ref.result_id)
        if plan is None:
            raise ResultMissing(f"The {self.id!r} backend no longer holds result {ref.result_id!r}")
        return plan

    def _drop(self, result_id: str) -> None:
        with self._lock:
            self._results.pop(result_id, None)
            staged = self._staged.pop(result_id, None)
        if staged is None:
            return
        store, uri = staged
        try:
            exchange.remove_prefix(store, uri)
        except Exception as e:
            logger.warning("Could not delete %s from the exchange store; its lifecycle rule will: %s", uri, e)
