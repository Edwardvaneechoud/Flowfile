"""Refuse a worker that runs other versions than core, before it is sent any work.

Core ships the worker serialized Polars plans and reads back what the worker wrote, so the
two processes must agree on everything a plan's bytes depend on. ``ensure_handshake`` runs
before core's first worker call, in the REST session and before a websocket submit, each
passing the worker URL it is about to call, and checks that once per process.

First, ``GET /capabilities`` returns the worker's ``Capabilities``, which are compared field
by field with ``local_capabilities()`` of this process. Every field must match exactly:

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
``ComputeUnavailable``.
"""

import polars as pl
import requests
from polars_grouper import graph_solver

from flowfile_core.flowfile.execution.errors import ComputeError, ComputeUnavailable, VersionMismatch
from shared.compute_protocol import EXPRESSION_PLUGINS, Capabilities, local_capabilities

_COMPARED_FIELDS = ("app_version", "protocol_version", "polars_version", "python_version", "platform")
_PER_PLUGIN_FIELDS = ("plugins", "plugin_libs")
_ABSENT = "not installed"

# True once the worker passed; also True while the check runs (see ensure_handshake).
_handshake_done = False


def fetch_worker_capabilities(worker_url: str) -> Capabilities:
    """``GET /capabilities`` from the worker at *worker_url*, signed with the internal token like every worker call.

    Raises:
        ComputeUnavailable: the worker cannot be reached.
        VersionMismatch: the worker answers but has no ``/capabilities`` route, so it predates core.
    """
    # imported here: subprocess_operations imports this module
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
        _WORKER_TIMEOUT,
        _WorkerAuth,
    )

    try:
        response = requests.get(f"{worker_url}/capabilities", auth=_WorkerAuth(), timeout=_WORKER_TIMEOUT)
    except (requests.ConnectionError, requests.Timeout) as e:
        raise ComputeUnavailable(f"Could not reach the worker at {worker_url}: {e}") from e
    if response.status_code == 404:
        raise VersionMismatch(
            f"The worker at {worker_url} does not report its capabilities (GET /capabilities answered 404), "
            "so it is an older release than core"
        )
    response.raise_for_status()
    return Capabilities.model_validate(response.json())


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


def ensure_handshake(worker_url: str) -> None:
    """Check the worker at *worker_url* before core's first worker call in this process; afterwards return at once.

    ``_handshake_done`` is set before the checks start, so the probe's own worker calls pass
    this gate instead of recursing into it (a REST fallback polls from another thread, which
    therefore must not wait on the check either). Any failure clears it again, so the next
    worker call checks again; the worker session also clears it when a call cannot reach the
    worker, because the worker that comes back may be another one.

    Raises:
        ComputeUnavailable: the worker cannot be reached.
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
    except BaseException:
        _handshake_done = False
        raise


def reset_handshake() -> None:
    """Make the next worker call check the worker again."""
    global _handshake_done
    _handshake_done = False
