# ruff: noqa: E402

import multiprocessing
import threading

from shared._version import get_version
from shared.storage_config import storage

__version__ = get_version()
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
