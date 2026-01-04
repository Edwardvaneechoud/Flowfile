"""
Shared utilities for Flowfile services.
This package contains common functionality that can be used across
flowfile_core, flowfile_worker, and other components without creating
circular dependencies.
"""

from .storage_config import get_cache_directory, get_flows_directory, get_temp_directory, storage

__all__ = ["storage", "get_cache_directory", "get_temp_directory", "get_flows_directory"]
