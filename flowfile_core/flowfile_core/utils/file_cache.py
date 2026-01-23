"""
File-based caching with modification time tracking.

This module provides caching for file schema and data to avoid redundant reads
when files haven't changed. It uses file modification time and size as cache keys.
"""

import hashlib
import os
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

from flowfile_core.configs import logger

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

T = TypeVar("T")


@dataclass
class FileInfo:
    """Tracks file metadata for cache invalidation."""

    path: str
    mtime: float
    size: int

    @classmethod
    def from_path(cls, path: str) -> "FileInfo | None":
        """Create from file path, returns None if file doesn't exist."""
        try:
            stat = os.stat(path)
            return cls(path=path, mtime=stat.st_mtime, size=stat.st_size)
        except OSError:
            return None

    def has_changed(self) -> bool:
        """Check if file has changed since this info was recorded."""
        try:
            stat = os.stat(self.path)
            return stat.st_mtime != self.mtime or stat.st_size != self.size
        except OSError:
            return True  # File missing = changed

    def to_key(self) -> str:
        """Generate a unique key based on file metadata."""
        return f"{self.path}:{self.mtime}:{self.size}"


@dataclass
class CacheEntry:
    """A cache entry with file info and cached value."""

    file_info: FileInfo
    settings_hash: str
    value: Any
    hit_count: int = 0


class FileSchemaCache:
    """
    Thread-safe cache for file schemas with modification time tracking.

    The cache uses a combination of file path, modification time, size, and
    settings hash to determine cache validity. This ensures that:
    1. Changed files are re-read
    2. Different settings for the same file are cached separately
    3. Unchanged files with same settings return cached schemas instantly

    Supports xlsx, csv, and parquet files with different caching strategies.
    """

    _instance: "FileSchemaCache | None" = None
    _lock: threading.RLock = threading.RLock()

    def __new__(cls) -> "FileSchemaCache":
        """Singleton pattern for global cache."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """Initialize cache storage."""
        self._cache: dict[str, CacheEntry] = {}
        self._max_entries: int = 100
        self._enabled_file_types: set[str] = {"xlsx", "excel", "csv", "parquet"}
        self._debug_mode: bool = False

    @classmethod
    def get_instance(cls) -> "FileSchemaCache":
        """Get the singleton cache instance."""
        return cls()

    def set_debug_mode(self, enabled: bool) -> None:
        """Enable/disable debug mode which enables caching for all file types."""
        self._debug_mode = enabled
        logger.info(f"File cache debug mode: {enabled}")

    def is_caching_enabled(self, file_type: str) -> bool:
        """Check if caching is enabled for the given file type."""
        if self._debug_mode:
            return True
        return file_type.lower() in self._enabled_file_types

    def _generate_settings_hash(self, settings: dict[str, Any]) -> str:
        """Generate a hash from settings dictionary."""
        # Sort keys for consistent hashing
        sorted_items = sorted(settings.items())
        settings_str = str(sorted_items)
        return hashlib.md5(settings_str.encode()).hexdigest()[:16]

    def _generate_cache_key(self, file_info: FileInfo, settings_hash: str) -> str:
        """Generate a unique cache key."""
        return f"{file_info.to_key()}:{settings_hash}"

    def _evict_if_needed(self) -> None:
        """Evict least-used entries if cache is full."""
        if len(self._cache) >= self._max_entries:
            # Remove entries for files that no longer exist or have changed
            stale_keys = []
            for key, entry in self._cache.items():
                if entry.file_info.has_changed():
                    stale_keys.append(key)

            for key in stale_keys:
                del self._cache[key]
                logger.debug(f"Evicted stale cache entry: {key}")

            # If still over limit, remove least-used entries
            if len(self._cache) >= self._max_entries:
                sorted_entries = sorted(
                    self._cache.items(), key=lambda x: x[1].hit_count
                )
                entries_to_remove = len(self._cache) - self._max_entries + 10
                for key, _ in sorted_entries[:entries_to_remove]:
                    del self._cache[key]
                    logger.debug(f"Evicted LRU cache entry: {key}")

    def get_schema(
        self,
        file_path: str,
        file_type: str,
        settings: dict[str, Any],
    ) -> "list[FlowfileColumn] | None":
        """
        Get cached schema if available and file hasn't changed.

        Args:
            file_path: Path to the file
            file_type: Type of file (xlsx, csv, parquet, etc.)
            settings: Dict of settings that affect schema (e.g., sheet_name, has_headers)

        Returns:
            Cached schema if valid, None if cache miss or file changed
        """
        if not self.is_caching_enabled(file_type):
            return None

        file_info = FileInfo.from_path(file_path)
        if file_info is None:
            logger.debug(f"Cache miss: file not found: {file_path}")
            return None

        settings_hash = self._generate_settings_hash(settings)
        cache_key = self._generate_cache_key(file_info, settings_hash)

        with self._lock:
            entry = self._cache.get(cache_key)
            if entry is None:
                logger.debug(f"Cache miss: no entry for {file_path}")
                return None

            # Check if file has changed since caching
            if entry.file_info.has_changed():
                del self._cache[cache_key]
                logger.info(f"Cache invalidated: file changed: {file_path}")
                return None

            entry.hit_count += 1
            logger.info(
                f"Cache hit for schema: {file_path} (hits: {entry.hit_count})"
            )
            return entry.value

    def set_schema(
        self,
        file_path: str,
        file_type: str,
        settings: dict[str, Any],
        schema: "list[FlowfileColumn]",
    ) -> None:
        """
        Cache a schema for a file.

        Args:
            file_path: Path to the file
            file_type: Type of file (xlsx, csv, parquet, etc.)
            settings: Dict of settings that affect schema
            schema: The schema to cache
        """
        if not self.is_caching_enabled(file_type):
            return

        file_info = FileInfo.from_path(file_path)
        if file_info is None:
            logger.warning(f"Cannot cache schema: file not found: {file_path}")
            return

        settings_hash = self._generate_settings_hash(settings)
        cache_key = self._generate_cache_key(file_info, settings_hash)

        with self._lock:
            self._evict_if_needed()
            self._cache[cache_key] = CacheEntry(
                file_info=file_info,
                settings_hash=settings_hash,
                value=schema,
                hit_count=0,
            )
            logger.info(f"Cached schema for: {file_path}")

    def invalidate_file(self, file_path: str) -> int:
        """
        Invalidate all cache entries for a specific file.

        Args:
            file_path: Path to the file

        Returns:
            Number of entries invalidated
        """
        with self._lock:
            keys_to_remove = [
                key for key, entry in self._cache.items()
                if entry.file_info.path == file_path
            ]
            for key in keys_to_remove:
                del self._cache[key]
            if keys_to_remove:
                logger.info(f"Invalidated {len(keys_to_remove)} cache entries for: {file_path}")
            return len(keys_to_remove)

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.info(f"Cleared file cache ({count} entries)")

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_hits = sum(entry.hit_count for entry in self._cache.values())
            return {
                "entries": len(self._cache),
                "max_entries": self._max_entries,
                "total_hits": total_hits,
                "enabled_file_types": list(self._enabled_file_types),
                "debug_mode": self._debug_mode,
            }


# Global cache instance
_file_schema_cache: FileSchemaCache | None = None


def get_file_schema_cache() -> FileSchemaCache:
    """Get the global file schema cache instance."""
    global _file_schema_cache
    if _file_schema_cache is None:
        _file_schema_cache = FileSchemaCache.get_instance()
    return _file_schema_cache


def get_cached_schema(
    file_path: str,
    file_type: str,
    settings: dict[str, Any],
) -> "list[FlowfileColumn] | None":
    """
    Convenience function to get cached schema.

    Args:
        file_path: Path to the file
        file_type: Type of file (xlsx, csv, parquet, etc.)
        settings: Dict of settings that affect schema

    Returns:
        Cached schema if valid, None otherwise
    """
    return get_file_schema_cache().get_schema(file_path, file_type, settings)


def cache_schema(
    file_path: str,
    file_type: str,
    settings: dict[str, Any],
    schema: "list[FlowfileColumn]",
) -> None:
    """
    Convenience function to cache a schema.

    Args:
        file_path: Path to the file
        file_type: Type of file (xlsx, csv, parquet, etc.)
        settings: Dict of settings that affect schema
        schema: The schema to cache
    """
    get_file_schema_cache().set_schema(file_path, file_type, settings, schema)


def invalidate_file_cache(file_path: str) -> int:
    """
    Convenience function to invalidate cache for a file.

    Args:
        file_path: Path to the file

    Returns:
        Number of entries invalidated
    """
    return get_file_schema_cache().invalidate_file(file_path)


def clear_file_cache() -> None:
    """Convenience function to clear all cached schemas."""
    get_file_schema_cache().clear()


def set_file_cache_debug_mode(enabled: bool) -> None:
    """Enable/disable debug mode for file caching."""
    get_file_schema_cache().set_debug_mode(enabled)


def get_file_cache_stats() -> dict[str, Any]:
    """Get file cache statistics."""
    return get_file_schema_cache().get_stats()
