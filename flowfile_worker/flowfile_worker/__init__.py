from typing import Dict
import tempfile
import threading

from multiprocessing import get_context
from flowfile_worker.models import Status
mp_context = get_context("spawn")
status_dict: Dict[str, Status] = dict()
process_dict = dict()

status_dict_lock = threading.Lock()
process_dict_lock = threading.Lock()

# Shared dictionaries

# Define the cache expiration time (1 day in seconds)
CACHE_EXPIRATION_TIME = 24 * 60 * 60
CACHE_DIR = tempfile.TemporaryDirectory()
PROCESS_MEMORY_USAGE: Dict[str, float] = {}