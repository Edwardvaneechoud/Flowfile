# ruff: noqa: E402

import importlib.machinery
import multiprocessing
import sys
import threading
from typing import TYPE_CHECKING

from shared._version import get_version
from shared.storage_config import storage

if TYPE_CHECKING:
    from flowfile_worker.models import Status

    # Declared here only: a runtime annotation naming Status would break get_type_hints.
    status_dict: dict[str, Status]

__version__ = get_version()
multiprocessing.set_start_method("spawn", force=True)

if multiprocessing.current_process().name == "MainProcess":
    _main_mod = sys.modules.get("__main__")
    if _main_mod is not None and getattr(_main_mod, "__spec__", None) is None:
        # spawn's _fixup_main_from_name returns immediately for a ".__main__" name,
        # so children skip re-executing the launcher (and its full app import).
        _main_mod.__spec__ = importlib.machinery.ModuleSpec("flowfile_worker.__main__", None)

from multiprocessing import get_context

mp_context = get_context("spawn")

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
