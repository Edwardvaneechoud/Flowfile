from typing import Dict
import tempfile
import threading
import multiprocessing
import os
multiprocessing.set_start_method('spawn', force=True)


from multiprocessing import get_context
from flowfile_worker.models import Status
mp_context = get_context("spawn")
status_dict: Dict[str, Status] = dict()
process_dict = dict()

status_dict_lock = threading.Lock()
process_dict_lock = threading.Lock()


class SharedTempDirectory:
    """A class that mimics tempfile.TemporaryDirectory but uses a fixed directory"""
    def __init__(self, dir_path):
        self._path = dir_path
        os.makedirs(self._path, exist_ok=True)

    @property
    def name(self):
        return self._path

    def cleanup(self):
        # Could implement actual cleanup if needed
        pass

    def __enter__(self):
        return self.name

    def __exit__(self, exc, value, tb):
        self.cleanup()

# Shared dictionaries

# Define the cache expiration time (1 day in seconds)
CACHE_EXPIRATION_TIME = 24 * 60 * 60

TEMP_DIR = os.getenv('TEMP_DIR')
if TEMP_DIR:
    CACHE_DIR = SharedTempDirectory(TEMP_DIR)
else:
    CACHE_DIR = tempfile.TemporaryDirectory()

PROCESS_MEMORY_USAGE: Dict[str, float] = dict()
