"""
Flowfile Worker initialization module.

This module sets up:
- Multiprocessing context for process spawning
- Global state dictionaries and locks
- Process limiting with semaphore
- Cache configuration

All global state will be migrated to WorkerState in Phase 2.
"""
from typing import Dict, Any
import threading
import os
import multiprocessing

# DO NOT call set_start_method here - it runs on import!
from multiprocessing import get_context
from flowfile_worker.models import Status
from flowfile_worker.configs import config, logger
from flowfile_worker.state import WorkerState
from shared.storage_config import storage

# Initialize multiprocessing context with spawn method
mp_context = get_context("spawn")

# Initialize centralized worker state
worker_state = WorkerState()

# Legacy global dictionaries for backward compatibility (deprecated - use worker_state instead)
# These will be gradually phased out as routes and spawner are updated
status_dict: Dict[str, Status] = dict()
process_dict: Dict[str, 'multiprocessing.Process'] = dict()

# Locks for thread-safe access to legacy global state
status_dict_lock = threading.Lock()
process_dict_lock = threading.Lock()

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

# Process memory usage tracking
PROCESS_MEMORY_USAGE: Dict[str, float] = dict()
