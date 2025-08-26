from typing import Dict
import threading
import multiprocessing
from shared.storage_config import storage

multiprocessing.set_start_method('spawn', force=True)

from multiprocessing import get_context
from flowfile_worker.models import Status

mp_context = get_context("spawn")

status_dict: Dict[str, Status] = dict()
process_dict = dict()

status_dict_lock = threading.Lock()
process_dict_lock = threading.Lock()


CACHE_EXPIRATION_TIME = 24 * 60 * 60


CACHE_DIR = storage.cache_directory


PROCESS_MEMORY_USAGE: Dict[str, float] = dict()
