"""Compute backends, known by id.

A backend is made by a factory. Core registers its own in process with ``register_backend``; an installed
package adds one through the ``flowfile.compute_backends`` entry-point group, where the entry point's name
is the backend id and it loads to the factory. Entry points are scanned once, on the first lookup, and an
in-process registration wins over an entry point of the same name. Core's own backends are built in: ``_BUILTIN``
names the module and class of each, imported and registered on the first lookup of an id nothing registered, and an
entry point never shadows a built-in id. ``backend`` calls a factory once per process and hands out that instance
from then on.

``default_backend_id`` names the default backend, the one a ``"remote"`` flow and a call without a graph use:
``FLOWFILE_COMPUTE_BACKEND`` when set, else ``"worker"`` while the ``OFFLOAD_TO_WORKER`` setting is on and
``"local"`` when it is off. That setting is read here through ``flowfile_core.configs.settings``; once every call
site resolves compute through this registry, this module is the only place outside ``configs/settings.py`` that
reads the worker settings.
"""

from __future__ import annotations

import importlib
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
LOCAL_BACKEND_ID = "local"
WORKER_BACKEND_ID = "worker"

_BUILTIN: dict[str, tuple[str, str]] = {
    "local": ("flowfile_core.flowfile.execution.backends.local", "LocalBackend"),
    "worker": ("flowfile_core.flowfile.execution.backends.worker", "WorkerBackend"),
}
"""Backend id -> (module, class) of the backends core ships, imported on their first lookup."""

_lock = threading.RLock()
_factories: dict[str, BackendFactory] = {}
_instances: dict[str, ComputeBackend] = {}
_entry_points_scanned = False


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
            module_name, class_name = _BUILTIN[backend_id]
            _factories[backend_id] = getattr(importlib.import_module(module_name), class_name)
        _scan_entry_points()
        factory = _factories.get(backend_id)
        if factory is None:
            known = ", ".join(repr(known_id) for known_id in sorted(_factories.keys() | _BUILTIN.keys())) or "none"
            raise LookupError(f"Unknown compute backend {backend_id!r}; known backends: {known}")
        instance = _instances[backend_id] = factory()
        return instance


def default_backend_id() -> str:
    """The id of the default backend, the one a ``"remote"`` flow and a call without a graph use.

    ``FLOWFILE_COMPUTE_BACKEND`` when set to a non-empty value, else ``"worker"`` while ``OFFLOAD_TO_WORKER`` is on
    and ``"local"`` when it is off.
    """
    configured = os.environ.get(BACKEND_ENV_VAR, "").strip()
    if configured:
        return configured
    return WORKER_BACKEND_ID if settings.OFFLOAD_TO_WORKER else LOCAL_BACKEND_ID


def default_backend() -> ComputeBackend:
    """The instance of the default backend."""
    return backend(default_backend_id())


def reset() -> None:
    """Forget every registration, every instance and the entry-point scan, for tests."""
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
