from typing import Dict
import threading
import multiprocessing
from shared.storage_config import storage

# DO NOT call set_start_method here - it runs on import!

from multiprocessing import get_context
from flowfile_worker.models import Status

mp_context = get_context("spawn")

status_dict: Dict[str, Status] = dict()
process_dict = dict()

status_dict_lock = threading.Lock()
process_dict_lock = threading.Lock()

# NEW: Add process limiting
import platform
import os
from flowfile_worker.configs import logger

def _calculate_max_workers() -> int:
    """
    Calculate platform-appropriate max concurrent processes.
    
    Platform limits:
    - Windows: Lower limit due to handle restrictions (max 32)
    - Unix: Higher limit but respect system resources (max 61)
    
    Returns:
        Maximum number of concurrent worker processes
    """
    cpu_count = os.cpu_count() or 4
    
    if platform.system() == 'Windows':
        # Windows has lower handle limits
        default_max = min(32, cpu_count + 4)
    else:
        # Unix systems (Linux, macOS) can handle more
        default_max = min(61, cpu_count * 2)
    
    return default_max

# Allow override via environment variable
MAX_CONCURRENT_PROCESSES = int(os.environ.get('FLOWFILE_MAX_WORKERS', _calculate_max_workers()))

# Semaphore to enforce the limit
process_semaphore = threading.Semaphore(MAX_CONCURRENT_PROCESSES)

logger.info(f"Maximum concurrent processes: {MAX_CONCURRENT_PROCESSES}")

CACHE_EXPIRATION_TIME = 24 * 60 * 60

CACHE_DIR = storage.cache_directory

PROCESS_MEMORY_USAGE: Dict[str, float] = dict()
