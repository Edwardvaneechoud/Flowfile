# ruff: noqa: E402

import multiprocessing
import threading
from importlib.metadata import PackageNotFoundError, version

from shared.storage_config import storage

try:
    __version__ = version("Flowfile")
except PackageNotFoundError:
    __version__ = "0.5.0"
multiprocessing.set_start_method("spawn", force=True)

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
