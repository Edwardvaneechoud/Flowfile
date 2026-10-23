"""Compute backends, known by id.

A backend is made by a factory. Core registers its own in process with ``register_backend``; an installed
package adds one through the ``flowfile.compute_backends`` entry-point group, where the entry point's name
is the backend id and it loads to the factory. Entry points are scanned once, on the first lookup, and an
in-process registration wins over an entry point of the same name. Core's own backends are built in: ``_BUILTIN``
holds the factory of each, which imports the backend's module when it first makes one and is registered on the
first lookup of an id nothing registered; an entry point never shadows a built-in id. ``backend`` calls a factory
once per process and hands out that instance from then on.

A configured backend is a row of core's ``compute_backends`` table (``ConfiguredBackend``), built by the row's kind on
the first lookup of an id with a configured prefix, ``worker:`` or ``polars-cloud:``, that nothing registered and no
entry point names. A ``worker`` row, ``worker:<name>``, holds its URL, its bearer token, stored as secret-manager
ciphertext, and the CA certificate to trust for it, if any: the lookup decrypts the token and makes a ``WorkerBackend``
on that URL with that id, token and CA certificate. A ``polars-cloud`` row, ``polars-cloud:<name>``, holds a Polars
cluster's scheduler URL as its ``url`` and, in its JSON ``settings``, how the backend reaches the compute (the
scheduler's and the dashboard's URLs): the lookup makes a ``PolarsCloudBackend`` from those settings, which connects to
nothing until it is first used. The instance is cached like every other. No row, or a row of a
kind this core does not know, is a ``LookupError``. ``forget`` drops a cached instance, so the next lookup reads the row
again, or fails once the row is gone.

``default_backend_id`` names the default backend, the one a ``"remote"`` flow and a call without a graph use:
the process-wide override a headless runner sets (``set_default_override``) when there is one, else
``FLOWFILE_COMPUTE_BACKEND`` when set, else ``"worker"`` while the ``OFFLOAD_TO_WORKER`` setting is on and
``"local"`` when it is off; never ``"local"`` in a process that serves.

A process that serves (the desktop sidecar, the Docker image, the pip server) is marked by its entry point
(``mark_serving``): its compute is the worker beside it, so its default backend is never the worker embedded in it,
a run placed on ``local`` there is refused, and the ``local`` factory leaves the embedded worker's jobs to its pool.
The pip server has no worker beside it: in single-file mode (``settings.SINGLE_FILE_MODE``, which its ``start_server``
sets) the built-in ``worker`` is the worker embedded in this process under that id, its jobs in its pool, while a
``worker:<name>`` row stays a ``WorkerBackend``.

This module is the one place outside ``configs/settings.py`` that reads the worker settings: the offload setting
above, and ``WORKER_URL``, the URL the built-in worker backend is made with. Every other module asks this registry,
or the backend it holds, where compute goes and which worker it reaches (``worker_url``); ``child_env`` names the
variables a child process needs to run on the worker embedded in its own process.
"""

from __future__ import annotations

import importlib.metadata
import os
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from flowfile_core.configs import settings

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
    from flowfile_core.flowfile.execution.protocol import ComputeBackend

BackendFactory = Callable[[], "ComputeBackend"]

ENTRY_POINT_GROUP = "flowfile.compute_backends"
BACKEND_ENV_VAR = "FLOWFILE_COMPUTE_BACKEND"
LEGACY_OFFLOAD_ENV_VAR = "FLOWFILE_OFFLOAD_TO_WORKER"
LOCAL_BACKEND_ID = "local"
WORKER_BACKEND_ID = "worker"
CONFIGURED_WORKER_PREFIX = "worker:"
WORKER_KIND = "worker"
POLARS_CLOUD_KIND = "polars-cloud"
POLARS_CLOUD_PREFIX = "polars-cloud:"
CONFIGURED_PREFIXES = {WORKER_KIND: CONFIGURED_WORKER_PREFIX, POLARS_CLOUD_KIND: POLARS_CLOUD_PREFIX}
"""Backend kind -> the prefix of the ids of its ``compute_backends`` rows."""


def _local_backend() -> ComputeBackend:
    """The worker embedded in this process.

    A process that does not serve (a headless run, a flow built in code, a test) computes in its own process, so the
    factory sets the worker's in-process mode before it makes the adapter; in a process that serves
    (``mark_serving``) it leaves the mode unset, so the embedded worker's jobs run in its pool or a child, never in the
    serving process.
    """
    from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
    from flowfile_worker import spawner

    if not _serving:
        spawner.in_process = True
    return EmbeddedWorkerBackend()


def _worker_backend() -> ComputeBackend:
    """The worker at ``settings.WORKER_URL``, which ``FLOWFILE_WORKER_URL`` sets, else ``WORKER_HOST`` and the port.

    In single-file mode it is the worker embedded in this process, made as ``local`` is and answering to ``worker``.
    """
    if settings.SINGLE_FILE_MODE:
        embedded = _local_backend()
        embedded.id = WORKER_BACKEND_ID
        return embedded
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend

    return WorkerBackend(url=settings.WORKER_URL)


def _configured_backend(backend_id: str) -> ComputeBackend:
    """The backend the ``compute_backends`` row of ``backend_id`` configures, built by the row's kind.

    A worker signs its calls with the row's token and trusts the row's CA certificate for its TLS, when the row has
    one; a Polars cluster is reached at the row's scheduler URL, its dashboard the one the row's settings name.

    Raises:
        LookupError: No row configures ``backend_id``, or the row's kind is none this core knows.
    """
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import ConfiguredBackend

    with get_db_context() as db:
        row = db.get(ConfiguredBackend, backend_id)
        if row is None:
            raise LookupError(f"Unknown compute backend {backend_id!r}; no compute_backends row has that id")
        kind, url, ciphertext, ca_cert, row_settings = row.kind, row.url, row.token, row.ca_cert, row.settings
    if kind == POLARS_CLOUD_KIND:
        from flowfile_core.flowfile.execution.backends.polars_cloud import PolarsCloudBackend

        try:
            return PolarsCloudBackend.from_row(backend_id, row_settings)
        except ValueError as e:
            raise LookupError(f"Compute backend {backend_id!r} cannot be made from its row: {e}") from e
    if kind != WORKER_KIND:
        raise LookupError(f"Compute backend {backend_id!r} is of kind {kind!r}, which this core does not know")
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
    from flowfile_core.secret_manager.secret_manager import decrypt_secret

    return WorkerBackend(
        url=url, backend_id=backend_id, token=decrypt_secret(ciphertext).get_secret_value(), ca_cert=ca_cert
    )


_BUILTIN: dict[str, BackendFactory] = {
    LOCAL_BACKEND_ID: _local_backend,
    WORKER_BACKEND_ID: _worker_backend,
}
"""Backend id -> factory of the backends core ships; each imports its backend's module on first use."""

_lock = threading.RLock()
_factories: dict[str, BackendFactory] = {}
_instances: dict[str, ComputeBackend] = {}
_entry_points_scanned = False
_default_override: str | None = None
_serving = False


def register_backend(backend_id: str, factory: BackendFactory) -> None:
    """Make ``backend_id`` resolve through ``factory``.

    Registering an id again replaces its factory and drops the instance the previous one made.
    """
    with _lock:
        _factories[backend_id] = factory
        _instances.pop(backend_id, None)


def backend(backend_id: str) -> ComputeBackend:
    """The process's instance of ``backend_id``, made by its factory on first use.

    An id that nothing registered and no entry point names, starting with ``worker:`` or ``polars-cloud:``, is made
    from its ``compute_backends`` row by the row's kind.

    Raises:
        LookupError: No factory is registered, built in or installed under ``backend_id``; the message lists the
            known ids, built-in ones included. For a configured id, no row configures it or its kind is unknown.
    """
    with _lock:
        instance = _instances.get(backend_id)
        if instance is not None:
            return instance
        if backend_id not in _factories and backend_id in _BUILTIN:
            _factories[backend_id] = _BUILTIN[backend_id]
        _scan_entry_points()
        factory = _factories.get(backend_id)
        if factory is not None:
            instance = factory()
        elif backend_id.startswith(tuple(CONFIGURED_PREFIXES.values())):
            instance = _configured_backend(backend_id)
        else:
            known = ", ".join(repr(known_id) for known_id in sorted(_factories.keys() | _BUILTIN.keys())) or "none"
            raise LookupError(f"Unknown compute backend {backend_id!r}; known backends: {known}")
        _instances[backend_id] = instance
        return instance


def forget(backend_id: str) -> None:
    """Drop the process's instance of ``backend_id``; the next lookup makes it again, or fails once its row is gone."""
    with _lock:
        _instances.pop(backend_id, None)


def default_backend_id() -> str:
    """The id of the default backend, the one a ``"remote"`` flow and a call without a graph use.

    The override ``set_default_override`` set, when there is one; else ``FLOWFILE_COMPUTE_BACKEND`` when set to a
    non-empty value; else ``"worker"`` while ``OFFLOAD_TO_WORKER`` is on and ``"local"`` when it is off. In a process
    that serves (``mark_serving``) it is ``"worker"`` wherever that order gives ``"local"``.
    """
    backend_id = _default_override
    if backend_id is None:
        backend_id = os.environ.get(BACKEND_ENV_VAR, "").strip() or (
            WORKER_BACKEND_ID if settings.OFFLOAD_TO_WORKER else LOCAL_BACKEND_ID
        )
    if _serving and backend_id == LOCAL_BACKEND_ID:
        return WORKER_BACKEND_ID
    return backend_id


def default_backend() -> ComputeBackend:
    """The instance of the default backend."""
    return backend(default_backend_id())


def set_default_override(backend_id: str | None) -> str | None:
    """Make ``backend_id`` the default backend of this process, ahead of ``FLOWFILE_COMPUTE_BACKEND`` and the flag.

    The headless flow runners set it to the worker embedded in their process for the run they make. ``None`` removes
    the override, so
    the default follows the environment variable and the flag again. Returns the override this call replaced
    (``None`` when there was none); passing it back here undoes the call.
    """
    global _default_override
    with _lock:
        previous, _default_override = _default_override, backend_id
    return previous


def default_override() -> str | None:
    """The override ``set_default_override`` set, or ``None`` when the default follows the environment and the flag."""
    return _default_override


def mark_serving() -> None:
    """Mark this process as one that serves: the desktop sidecar, the Docker image, the pip server.

    Its entry point calls it before the server starts. From then on the default backend is never ``"local"``, a run
    placed on ``local`` is refused (``FlowGraph._place_run``), and the ``local`` factory leaves the embedded worker's
    jobs to its pool. Nothing unmarks a process.
    """
    global _serving
    _serving = True


def serving() -> bool:
    """Whether this process serves (``mark_serving``)."""
    return _serving


def child_env() -> dict[str, str]:
    """The environment variables a child process needs to run on the worker embedded in its own process.

    ``FLOWFILE_COMPUTE_BACKEND`` names it; the legacy ``FLOWFILE_OFFLOAD_TO_WORKER=0`` says the same to a release
    that only knows that flag.
    """
    return {BACKEND_ENV_VAR: LOCAL_BACKEND_ID, LEGACY_OFFLOAD_ENV_VAR: "0"}


def worker_url() -> str:
    """The base URL of the worker the registry's worker backend runs on, for a caller that sends work there itself.

    In single-file mode that worker is embedded in this process and the app mounts its routes at
    ``settings.WORKER_URL``, which ``start_server`` sets; no backend is made for the answer.
    """
    if settings.SINGLE_FILE_MODE:
        return settings.WORKER_URL
    return cast("WorkerBackend", backend(WORKER_BACKEND_ID)).url


def reset() -> None:
    """Forget every registration, every instance and the entry-point scan, for tests; the default override stays."""
    global _entry_points_scanned
    with _lock:
        _factories.clear()
        _instances.clear()
        _entry_points_scanned = False


def _scan_entry_points() -> None:
    global _entry_points_scanned
    if _entry_points_scanned:
        return
    for entry_point in importlib.metadata.entry_points(group=ENTRY_POINT_GROUP):
        if entry_point.name not in _BUILTIN:
            _factories.setdefault(entry_point.name, _entry_point_factory(entry_point))
    _entry_points_scanned = True


def _entry_point_factory(entry_point: importlib.metadata.EntryPoint) -> BackendFactory:
    """A factory that imports the entry point's target only when the backend is first made."""

    def make() -> ComputeBackend:
        return entry_point.load()()

    return make
