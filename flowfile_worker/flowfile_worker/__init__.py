# ruff: noqa: E402

import importlib.machinery
import multiprocessing
import sys
import threading
from multiprocessing.context import SpawnContext
from typing import TYPE_CHECKING

from shared._version import get_version
from shared.storage_config import storage

if TYPE_CHECKING:
    from flowfile_worker.models import Status

    # Declared here only: a runtime annotation naming Status would break get_type_hints.
    status_dict: dict[str, Status]

__version__ = get_version()

from multiprocessing import get_context


def _prepare_spawn() -> None:
    """Make ``spawn`` the start method and, in the main process, give a ``__main__`` without a spec this package's.

    spawn's ``_fixup_main_from_name`` returns at once for a ``.__main__`` name, so a child skips re-executing the
    launcher and its full app import. Safe to run again: the force is idempotent and a stamped spec is left alone.
    """
    multiprocessing.set_start_method("spawn", force=True)
    if multiprocessing.current_process().name == "MainProcess":
        main_mod = sys.modules.get("__main__")
        if main_mod is not None and getattr(main_mod, "__spec__", None) is None:
            main_mod.__spec__ = importlib.machinery.ModuleSpec("flowfile_worker.__main__", None)


class SpawnProcess(SpawnContext.Process):
    """A spawn child that prepares this process for spawning as it starts, so ``import flowfile_worker`` does not.

    Named as the class it extends, so a child's default name stays ``SpawnProcess-<n>``.
    """

    def start(self) -> None:
        _prepare_spawn()
        super().start()


class _SpawnContext(SpawnContext):
    Process = SpawnProcess


mp_context = _SpawnContext()

status_dict = dict()
process_dict = dict()

status_dict_lock = threading.Lock()
process_dict_lock = threading.Lock()


CACHE_EXPIRATION_TIME = 24 * 60 * 60


CACHE_DIR = storage.cache_directory


PROCESS_MEMORY_USAGE: dict[str, float] = dict()

# Submodules the old eager `from flowfile_worker.models import Status` chain used to bind.
_LAZY_ATTRS = ("Status", "configs", "external_sources", "models", "secrets")

# Pinned to the pre-lazification star surface so `from flowfile_worker import *` is unchanged.
__all__ = [
    "CACHE_DIR",
    "CACHE_EXPIRATION_TIME",
    "PROCESS_MEMORY_USAGE",
    "Status",
    "configs",
    "external_sources",
    "get_context",
    "get_version",
    "importlib",
    "models",
    "mp_context",
    "multiprocessing",
    "process_dict",
    "process_dict_lock",
    "secrets",
    "status_dict",
    "status_dict_lock",
    "storage",
    "sys",
    "threading",
]


def __getattr__(name: str):
    # PEP 562: `models` pulls pydantic + the external-source models, which spawned children don't need.
    if name not in _LAZY_ATTRS:
        raise AttributeError(f"module 'flowfile_worker' has no attribute {name!r}")
    import importlib as _importlib

    if name == "Status":
        value = _importlib.import_module("flowfile_worker.models").Status
    else:
        value = _importlib.import_module(f"flowfile_worker.{name}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_LAZY_ATTRS))
