"""Compute backends, known by id.

A backend is made by a factory. Core registers its own in process with ``register_backend``; an installed
package adds one through the ``flowfile.compute_backends`` entry-point group, where the entry point's name
is the backend id and it loads to the factory. Entry points are scanned once, on the first lookup, and an
in-process registration wins over an entry point of the same name. Core's own backends are built in: ``_BUILTIN``
holds the factory of each, which imports the backend's module when it first makes one and is registered on the
first lookup of an id nothing registered; an entry point never shadows a built-in id. ``backend`` calls a factory
once per process and hands out that instance from then on.

``default_backend_id`` names the default backend, the one a ``"remote"`` flow and a call without a graph use:
the process-wide override a headless runner sets (``set_default_override``) when there is one, else
``FLOWFILE_COMPUTE_BACKEND`` when set, else ``"worker"`` while the ``OFFLOAD_TO_WORKER`` setting is on and
``"local"`` when it is off.

This module is the one place outside ``configs/settings.py`` that reads the worker settings: the offload setting
above, and ``WORKER_URL``, the URL the built-in worker backend is made with. Every other module asks this registry,
or the backend it holds, where compute goes and which worker it reaches; ``child_env`` names the variables a child
process needs to run on core's own backend.
"""

from __future__ import annotations

import importlib.metadata
import os
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from flowfile_core.configs import settings

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.protocol import ComputeBackend

BackendFactory = Callable[[], "ComputeBackend"]

ENTRY_POINT_GROUP = "flowfile.compute_backends"
BACKEND_ENV_VAR = "FLOWFILE_COMPUTE_BACKEND"
LEGACY_OFFLOAD_ENV_VAR = "FLOWFILE_OFFLOAD_TO_WORKER"
LOCAL_BACKEND_ID = "local"
WORKER_BACKEND_ID = "worker"


def _local_backend() -> ComputeBackend:
    """Core's own process as a compute."""
    from flowfile_core.flowfile.execution.backends.local import LocalBackend

    return LocalBackend()


def _worker_backend() -> ComputeBackend:
    """The worker at ``settings.WORKER_URL``, which ``FLOWFILE_WORKER_URL`` sets, else ``WORKER_HOST`` and the port."""
    from flowfile_core.flowfile.execution.backends.worker import WorkerBackend

    return WorkerBackend(url=settings.WORKER_URL)


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


def register_backend(backend_id: str, factory: BackendFactory) -> None:
    """Make ``backend_id`` resolve through ``factory``.

    Registering an id again replaces its factory and drops the instance the previous one made.
    """
    with _lock:
        _factories[backend_id] = factory
        _instances.pop(backend_id, None)


def backend(backend_id: str) -> ComputeBackend:
    """The process's instance of ``backend_id``, made by its factory on first use.

    Raises:
        LookupError: No factory is registered, built in or installed under ``backend_id``; the message lists the
            known ids, built-in ones included.
    """
    with _lock:
        instance = _instances.get(backend_id)
        if instance is not None:
            return instance
        if backend_id not in _factories and backend_id in _BUILTIN:
            _factories[backend_id] = _BUILTIN[backend_id]
        _scan_entry_points()
        factory = _factories.get(backend_id)
        if factory is None:
            known = ", ".join(repr(known_id) for known_id in sorted(_factories.keys() | _BUILTIN.keys())) or "none"
            raise LookupError(f"Unknown compute backend {backend_id!r}; known backends: {known}")
        instance = _instances[backend_id] = factory()
        return instance


def default_backend_id() -> str:
    """The id of the default backend, the one a ``"remote"`` flow and a call without a graph use.

    The override ``set_default_override`` set, when there is one; else ``FLOWFILE_COMPUTE_BACKEND`` when set to a
    non-empty value; else ``"worker"`` while ``OFFLOAD_TO_WORKER`` is on and ``"local"`` when it is off.
    """
    override = _default_override
    if override is not None:
        return override
    configured = os.environ.get(BACKEND_ENV_VAR, "").strip()
    if configured:
        return configured
    return WORKER_BACKEND_ID if settings.OFFLOAD_TO_WORKER else LOCAL_BACKEND_ID


def default_backend() -> ComputeBackend:
    """The instance of the default backend."""
    return backend(default_backend_id())


def set_default_override(backend_id: str | None) -> str | None:
    """Make ``backend_id`` the default backend of this process, ahead of ``FLOWFILE_COMPUTE_BACKEND`` and the flag.

    The headless flow runners set it to core's own backend for the run they make. ``None`` removes the override, so
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


def child_env() -> dict[str, str]:
    """The environment variables a child process needs to run on core's own backend.

    ``FLOWFILE_COMPUTE_BACKEND`` names it; the legacy ``FLOWFILE_OFFLOAD_TO_WORKER=0`` says the same to a release
    that only knows that flag.
    """
    return {BACKEND_ENV_VAR: LOCAL_BACKEND_ID, LEGACY_OFFLOAD_ENV_VAR: "0"}


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
