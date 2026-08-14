# ruff: noqa: E402

import importlib.machinery
import multiprocessing
import sys
import threading

from shared._version import get_version
from shared.storage_config import storage

__version__ = get_version()
multiprocessing.set_start_method("spawn", force=True)

if multiprocessing.current_process().name == "MainProcess":
    _main_mod = sys.modules.get("__main__")
    if _main_mod is not None and getattr(_main_mod, "__spec__", None) is None:
        # spawn's _fixup_main_from_name returns immediately for a ".__main__" name,
        # so children skip re-executing the launcher (and its full app import).
        _main_mod.__spec__ = importlib.machinery.ModuleSpec("flowfile_worker.__main__", None)

from multiprocessing import get_context

from flowfile_worker.models import Status

mp_context = get_context("spawn")

status_dict: dict[str, Status] = dict()
process_dict = dict()

status_dict_lock = threading.Lock()
process_dict_lock = threading.Lock()


CACHE_EXPIRATION_TIME = 24 * 60 * 60


CACHE_DIR = storage.cache_directory


PROCESS_MEMORY_USAGE: dict[str, float] = dict()
