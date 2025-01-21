from typing import Dict
import tempfile
import threading
import multiprocessing
import os
import shutil
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
        """Remove all contents of the temp directory"""
        try:
            shutil.rmtree(self._path)
            os.makedirs(self._path, exist_ok=True)
            print(f"Cleaned up temporary directory: {self._path}")
        except Exception as e:
            print(f"Error during cleanup: {e}")

    def __enter__(self):
        return self.name

    def __exit__(self, exc, value, tb):
        self.cleanup()


CACHE_EXPIRATION_TIME = 24 * 60 * 60


TEMP_DIR = os.getenv('TEMP_DIR')
if TEMP_DIR:
    CACHE_DIR = SharedTempDirectory(TEMP_DIR)
else:
    CACHE_DIR = tempfile.TemporaryDirectory()

PROCESS_MEMORY_USAGE: Dict[str, float] = dict()
