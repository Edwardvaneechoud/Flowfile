from airbyte.caches import DuckDBCache

import os
import atexit
from pathlib import Path
import filelock
from contextlib import contextmanager

from flowfile_worker.configs import logger


class DuckDBCacheManager:
    """
    Manages DuckDB cache instances with multiprocessing-safe lock handling.
    Coordinates cache access across different processes using file-based locks.
    """

    def __init__(self,
                 base_path: Path = Path.home() / ".flowfile/.tmp",
                 max_retries: int = 3,
                 retry_delay: float = 0.5):
        logger.info(f"Initializing DuckDBCacheManager with base_path: {base_path}")

        # Shared resources path
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Central coordination directory
        self.coordinator_dir = self.base_path / "coordinator"
        self.coordinator_dir.mkdir(exist_ok=True)

        # Process-specific information
        self.process_id = os.getpid()
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Global lock for cache allocation
        self.global_lock_file = self.coordinator_dir / "global.lock"
        logger.debug(f"Process {self.process_id} initialized with global lock at {self.global_lock_file}")

        # Register cleanup
        atexit.register(self.cleanup_process_resources)

    @contextmanager
    def get_cache(self) -> 'DuckDBCache':
        """
        Get an available cache instance with proper locking.
        """
        logger.debug(f"Process {self.process_id} attempting to get cache")
        cache_id = None
        cache = None

        # First, try to get any available existing cache
        with filelock.FileLock(str(self.global_lock_file), timeout=0.1):
            logger.debug(f"Process {self.process_id} acquired global lock")
            for i in range(10):  # Check first 10 possible cache slots
                try:
                    cache_lock = filelock.FileLock(
                        str(self.coordinator_dir / f"cache_{i}.lock"),
                        timeout=0.1
                    )
                    cache_lock.acquire()

                    # If we got the lock, use this cache slot
                    cache_id = i
                    cache_path = self.base_path / f"airbyte_cache_{i}"
                    logger.info(f"Process {self.process_id} acquired cache slot {i} at {cache_path}")

                    cache = DuckDBCache(
                        db_path=cache_path,
                        schema_name="main"
                    )

                    # Keep track of which process is using this cache
                    with open(self.coordinator_dir / f"cache_{i}.pid", 'w') as f:
                        f.write(str(self.process_id))

                    break

                except filelock.Timeout:
                    logger.debug(f"Process {self.process_id} failed to acquire lock for cache slot {i}")
                    continue

        if cache is None:
            logger.info(f"Process {self.process_id} couldn't acquire existing cache, creating new one")
            # If no existing cache is available, create a new one
            cache_id = self._create_new_cache_slot()
            cache_path = self.base_path / f"airbyte_cache_{cache_id}"
            cache = DuckDBCache(
                db_path=cache_path,
                schema_name="main"
            )

        try:
            yield cache
        finally:
            # Cleanup
            if cache_id is not None:
                lock_file = self.coordinator_dir / f"cache_{cache_id}.lock"
                pid_file = self.coordinator_dir / f"cache_{cache_id}.pid"
                logger.debug(f"Process {self.process_id} cleaning up cache slot {cache_id}")

                try:
                    if lock_file.exists():
                        lock_file.unlink()
                    if pid_file.exists():
                        pid_file.unlink()
                except OSError as e:
                    logger.error(f"Process {self.process_id} failed to cleanup files for cache {cache_id}: {str(e)}")

    def _create_new_cache_slot(self) -> int:
        """Create a new cache slot with proper locking."""
        logger.debug(f"Process {self.process_id} creating new cache slot")
        with filelock.FileLock(str(self.global_lock_file), timeout=10):
            # Find the next available slot
            existing_caches = set()
            for cache_file in self.coordinator_dir.glob("cache_*.lock"):
                try:
                    slot = int(cache_file.stem.split('_')[1])
                    existing_caches.add(slot)
                except (ValueError, IndexError):
                    continue

            # Get the first available slot number
            new_slot = 0
            while new_slot in existing_caches:
                new_slot += 1

            # Create the lock file for this slot
            lock_file = self.coordinator_dir / f"cache_{new_slot}.lock"
            lock_file.touch()
            logger.info(f"Process {self.process_id} created new cache slot {new_slot}")

            return new_slot

    def cleanup_process_resources(self):
        """Clean up resources when the process exits."""
        logger.debug(f"Process {self.process_id} starting cleanup")
        # Clean up any cache slots owned by this process
        for pid_file in self.coordinator_dir.glob("*.pid"):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())

                if pid == self.process_id:
                    cache_id = pid_file.stem.split('_')[1]
                    logger.info(f"Process {self.process_id} cleaning up cache slot {cache_id}")

                    # Remove the cache files
                    cache_path = self.base_path / f"airbyte_cache_{cache_id}"
                    lock_file = self.coordinator_dir / f"cache_{cache_id}.lock"

                    for file in [pid_file, lock_file, cache_path]:
                        try:
                            if file.exists():
                                file.unlink()
                                logger.debug(f"Process {self.process_id} removed file: {file}")
                        except OSError as e:
                            logger.error(f"Process {self.process_id} failed to remove file {file}: {str(e)}")
            except (ValueError, OSError) as e:
                logger.error(f"Process {self.process_id} encountered error during cleanup: {str(e)}")
