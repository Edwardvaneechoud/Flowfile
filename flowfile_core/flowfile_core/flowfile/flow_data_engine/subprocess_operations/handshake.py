"""Refuse a worker that runs other versions than core, before it is sent any work.

Core ships the worker serialized Polars plans and reads back what the worker wrote, so the
two processes must agree on everything a plan's bytes depend on. ``ensure_handshake`` runs
before core's first worker call, in the REST session and before a websocket submit, each
passing the worker URL it is about to call (and a caller that knows which backend that worker
is, its backend id), and checks that once per process.

First, ``GET /capabilities`` returns the worker's ``Capabilities``, which are compared field
by field with ``local_capabilities()`` of this process. The request goes through the worker
session around its own gate, so a worker core trusts by its CA certificate (``trust_ca``) is
verified against it here too. Every field must match exactly:

- ``app_version``: core and the worker are released together and their routes, payloads
  and on-disk results change between releases.
- ``protocol_version``: the version of the contract between them.
- ``polars_version``: a serialized plan only deserializes on the Polars that wrote it.
- ``python_version``: plans can carry pickled Python callables.
- ``platform``: the plugin libraries are native binaries for one OS and architecture.
- ``plugins``: the version of each plugin package; a plugin's native library and the Python
  side that builds its expressions must come from the same release.
- ``plugin_libs``: the path of each expression plugin's library as a plan records it,
  relative to ``sys.prefix``; a worker that resolves another path cannot load the plan.

A plugin present on only one side is a difference too.

Then one empty-frame plan per expression plugin runs on the worker. Equal capabilities say
the worker should load core's plans; the probe proves it does, by making the worker load
each plugin's library from the path the plan carries.

A difference in either check is a ``VersionMismatch`` that names it with both sides' values;
no work goes to that worker and nothing runs in core instead. An unreachable worker is a
``ComputeUnavailable``, naming the backend id the gate was given. An answer that is no
``Capabilities`` is a ``VersionMismatch`` naming both app versions when it carries an
``app_version`` (a worker of another release), else a ``ComputeUnavailable``: the URL is no
Flowfile worker (``capabilities_from_answer``).
"""

import polars as pl
import requests
from polars_grouper import graph_solver
from pydantic import ValidationError

from flowfile_core.flowfile.execution.errors import ComputeError, ComputeUnavailable, VersionMismatch
from shared._version import get_version
from shared.compute_protocol import EXPRESSION_PLUGINS, Capabilities, local_capabilities

_COMPARED_FIELDS = ("app_version", "protocol_version", "polars_version", "python_version", "platform")
_PER_PLUGIN_FIELDS = ("plugins", "plugin_libs")
_ABSENT = "not installed"

# True once the worker passed; also True while the check runs (see ensure_handshake).
_handshake_done = False


def fetch_worker_capabilities(worker_url: str) -> Capabilities:
    """``GET /capabilities`` from the worker at *worker_url*, signed with the internal token like every worker call.

    Raises:
        ComputeUnavailable: the worker cannot be reached, or its answer is no ``Capabilities`` and carries no
            ``app_version``, so the URL is no Flowfile worker.
        VersionMismatch: the worker answers but has no ``/capabilities`` route, so it predates core; or its answer
            carries an ``app_version`` but is no ``Capabilities``, so it is of another release.
    """
    # imported here: subprocess_operations imports this module
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
        _WORKER_TIMEOUT,
        _worker_session,
    )

    try:
        response = _worker_session.get("/capabilities", worker_url=worker_url, gated=False, timeout=_WORKER_TIMEOUT)
    except (requests.ConnectionError, requests.Timeout) as e:
        raise ComputeUnavailable(f"Could not reach the worker at {worker_url}: {e}") from e
    if response.status_code == 404:
        raise VersionMismatch(
            f"The worker at {worker_url} does not report its capabilities (GET /capabilities answered 404), "
            "so it is an older release than core"
        )
    response.raise_for_status()
    return capabilities_from_answer(worker_url, response)


def capabilities_from_answer(
    worker_url: str, response: requests.Response, *, backend_id: str | None = None
) -> Capabilities:
    """The ``Capabilities`` a 2xx answer to ``GET /capabilities`` from *worker_url* carries.

    An answer that does not validate is a worker of another release when it carries an ``app_version`` (a field
    added since without a default is missing from it), and otherwise a server that is no Flowfile worker (other
    JSON, or a body that is no JSON). Either error is raised from pydantic's ``ValidationError``.

    Raises:
        VersionMismatch: the answer carries an ``app_version`` but is no ``Capabilities``; the message names both
            app versions and each field this release could not read.
        ComputeUnavailable: the answer carries no ``app_version``; its ``backend_id`` is *backend_id*.
    """
    try:
        answer = response.json()
    except ValueError:
        answer = None
    try:
        return Capabilities.model_validate(answer)
    except ValidationError as e:
        app_version = answer.get("app_version") if isinstance(answer, dict) else None
        if isinstance(app_version, str):
            unread = "; ".join(f"{'.'.join(map(str, error['loc']))}: {error['msg']}" for error in e.errors())
            raise VersionMismatch(
                f"The worker at {worker_url} does not match core (app_version: core {get_version()}, worker "
                f"{app_version}): its capabilities answer is not one this release reads ({unread})"
            ) from e
        raise ComputeUnavailable(
            f"The server at {worker_url} is not a Flowfile worker: its capabilities answer carries no app_version",
            backend_id=backend_id,
        ) from e


def capability_differences(core: Capabilities, worker: Capabilities) -> list[str]:
    """Every field, or plugin within a per-plugin field, where the worker differs from core.

    Each entry names the field and both values, e.g. ``polars_version: core 1.39.3, worker 0.20.31``
    or ``plugin_libs[polars_ds]: core lib/.../_polars_ds.abi3.so, worker not installed``.
    """
    differences = []
    for field in _COMPARED_FIELDS:
        ours, theirs = getattr(core, field), getattr(worker, field)
        if ours != theirs:
            differences.append(f"{field}: core {ours}, worker {theirs}")
    for field in _PER_PLUGIN_FIELDS:
        ours, theirs = getattr(core, field), getattr(worker, field)
        for name in sorted(ours.keys() | theirs.keys()):
            if ours.get(name) != theirs.get(name):
                differences.append(
                    f"{field}[{name}]: core {ours.get(name, _ABSENT)}, worker {theirs.get(name, _ABSENT)}"
                )
    return differences


def _probe_plans() -> dict[str, pl.LazyFrame]:
    """One empty-frame plan per expression plugin, each calling one cheap expression of that plugin."""
    import polars_ds  # imported here: core does not need it at startup

    return {
        "polars_grouper": pl.LazyFrame(schema={"a": pl.String, "b": pl.String}).select(
            graph_solver(pl.col("a"), pl.col("b"))
        ),
        "polars_ds": pl.LazyFrame(schema={"x": pl.Int32}).select(polars_ds.gcd(pl.col("x"), 1)),
    }


def probe_plugins(worker_url: str, core: Capabilities, worker: Capabilities) -> None:
    """Run each probe plan on the worker at *worker_url* through core's own submit path.

    Raises:
        VersionMismatch: the worker could not run a plugin's plan; names the plugin and the
            library path core's plan carries next to the one the worker reported.
    """
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
        ExternalDfFetcher,
        clear_task_from_worker,
    )

    plans = _probe_plans()
    for name in EXPRESSION_PLUGINS:
        try:
            fetcher = ExternalDfFetcher(
                lf=plans[name], flow_id=-1, node_id=-1, wait_on_completion=True, worker_url=worker_url
            )
        except ComputeError:
            raise
        except Exception as e:
            raise VersionMismatch(
                f"The worker at {worker_url} could not run a {name} plan "
                f"(library: core {core.plugin_libs.get(name, _ABSENT)}, "
                f"worker {worker.plugin_libs.get(name, _ABSENT)}): {e}"
            ) from e
        clear_task_from_worker(fetcher.file_ref, worker_url=worker_url)


def ensure_handshake(worker_url: str, *, backend_id: str | None = None) -> None:
    """Check the worker at *worker_url* before core's first worker call in this process; afterwards return at once.

    ``_handshake_done`` is set before the checks start, so the probe's own worker calls pass
    this gate instead of recursing into it (a REST fallback polls from another thread, which
    therefore must not wait on the check either). Any failure clears it again, so the next
    worker call checks again; the worker session also clears it when a call cannot reach the
    worker, because the worker that comes back may be another one.

    A caller that knows which backend the worker is passes its id as *backend_id*; a
    ``ComputeUnavailable`` the check raises then names that backend in its hint.

    Raises:
        ComputeUnavailable: the worker cannot be reached; its ``backend_id`` is *backend_id*.
        VersionMismatch: the worker differs from core; the message names every difference.
    """
    global _handshake_done
    if _handshake_done:
        return
    _handshake_done = True
    try:
        core, worker = local_capabilities(), fetch_worker_capabilities(worker_url)
        differences = capability_differences(core, worker)
        if differences:
            raise VersionMismatch(f"The worker at {worker_url} does not match core ({'; '.join(differences)})")
        probe_plugins(worker_url, core, worker)
    except BaseException as e:
        _handshake_done = False
        if isinstance(e, ComputeUnavailable) and e.backend_id is None:
            e.backend_id = backend_id
        raise


def reset_handshake() -> None:
    """Make the next worker call check the worker again."""
    global _handshake_done
    _handshake_done = False
