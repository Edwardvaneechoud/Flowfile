"""
Flowfile Worker initialization module.

This module sets up:
- Multiprocessing context for process spawning
- Centralized worker state management
- Process limiting with semaphore
- Cache configuration
"""
import threading
from multiprocessing import get_context
from flowfile_worker.configs import config, logger
from flowfile_worker.state import WorkerState
from shared.storage_config import storage

# Initialize multiprocessing context with spawn method
mp_context = get_context("spawn")

# Initialize centralized worker state
worker_state = WorkerState()

# Process limiting configuration
MAX_CONCURRENT_PROCESSES = config.calculated_max_workers

# Semaphore to enforce the process limit
process_semaphore = threading.Semaphore(MAX_CONCURRENT_PROCESSES)

logger.info(f"Maximum concurrent processes: {MAX_CONCURRENT_PROCESSES}")

# Cache configuration
CACHE_EXPIRATION_TIME = config.cache_expiration_seconds
CACHE_DIR = storage.cache_directory

logger.info(f"Cache directory: {CACHE_DIR}")
logger.info(f"Cache expiration: {config.cache_expiration_hours} hours ({CACHE_EXPIRATION_TIME} seconds)")
