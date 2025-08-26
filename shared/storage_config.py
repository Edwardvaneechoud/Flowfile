# shared/storage_config.py
"""
Centralized storage configuration for Flowfile.
This module can be imported by both core and worker without creating dependencies.
"""
import os
from pathlib import Path
from typing import Optional, Literal

DirectoryOptions = Literal["temp_directory", "logs_directory",
                            "system_logs_directory", "database_directory",
                            "cache_directory", "flows_directory"]

class FlowfileStorage:
    """Centralized storage manager for Flowfile applications."""

    def __init__(self):
        self._base_dir: Optional[Path] = None
        self._ensure_directories()

    @property
    def base_directory(self) -> Path:
        """Get the base Flowfile storage directory."""
        if self._base_dir is None:
            # Priority: Docker env var > Dev env var > Home directory
            if os.environ.get("RUNNING_IN_DOCKER") == "true":
                # In Docker, use the mounted volume path
                base_path = os.environ.get("FLOWFILE_STORAGE_DIR", "/app/shared")
            else:
                # Local development or production
                base_path = os.environ.get("FLOWFILE_STORAGE_DIR")
                if not base_path:
                    home_dir = Path.home()
                    base_path = home_dir / ".flowfile"

            self._base_dir = Path(base_path)

        return self._base_dir

    @property
    def cache_directory(self) -> Path:
        """Cache directory for worker-core communication."""
        return self.base_directory / "cache"

    @property
    def system_logs_directory(self) -> Path:
        """Directory for flow storage and versioning."""
        return self.base_directory / "system_logs"

    @property
    def flows_directory(self) -> Path:
        """Directory for flow storage and versioning."""
        return self.base_directory / "flows"

    @property
    def database_directory(self) -> Path:
        """Directory for local database files."""
        return self.base_directory / "database"

    @property
    def logs_directory(self) -> Path:
        """Directory for application logs."""
        return self.base_directory / "logs"

    @property
    def temp_directory(self) -> Path:
        """Directory for temporary files."""
        return self.base_directory / "temp"

    def _ensure_directories(self) -> None:
        """Create all necessary directories if they don't exist."""
        directories = [
            self.cache_directory,
            self.flows_directory,
            self.database_directory,
            self.logs_directory,
            self.temp_directory,
            self.system_logs_directory,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def get_cache_file_path(self, filename: str) -> Path:
        """Get full path for a cache file."""
        return self.cache_directory / filename

    def get_flow_file_path(self, filename: str) -> Path:
        """Get full path for a flow file."""
        return self.flows_directory / filename

    def get_log_file_path(self, filename: str) -> Path:
        """Get full path for an application log file."""
        return self.logs_directory / filename

    def get_system_log_file_path(self, filename: str) -> Path:
        """Get full path for a system log file."""
        return self.system_logs_directory / filename

    def get_temp_file_path(self, filename: str) -> Path:
        """Get full path for a temporary file."""
        return self.temp_directory / filename

    def cleanup_directory(self, directory_option: DirectoryOptions):
        """Clean up any directory of the folder"""
        import time
        import shutil

        if not hasattr(self, directory_option):
            raise Exception(f"Directory does not exist in {self.base_directory}")

        directory = getattr(self, directory_option)
        if not isinstance(directory, Path):
            raise Exception(f"Directory attribute {directory_option} is not a Path object")

        if not directory.exists():
            return

        current_time = time.time()
        cutoff_time = current_time - (24 * 60 * 60)  # 24 hours

        for item in directory.iterdir():
            try:
                if item.stat().st_mtime < cutoff_time:
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        shutil.rmtree(item)
            except (OSError, FileNotFoundError):
                # Handle permission errors or files that disappeared
                continue

    def cleanup_temp_directory(self) -> None:
        """Clean up temporary files older than 24 hours."""
        self.cleanup_directory("temp_directory")


# Global instance
storage = FlowfileStorage()


# Convenience functions for backward compatibility
def get_cache_directory() -> str:
    """Get cache directory path as string."""
    return str(storage.cache_directory)


def get_temp_directory() -> str:
    """Get temp directory path as string."""
    return str(storage.temp_directory)


def get_flows_directory() -> str:
    """Get flows directory path as string."""
    return str(storage.flows_directory)


def get_logs_directory() -> str:
    """Get application logs directory path as string."""
    return str(storage.logs_directory)


def get_system_logs_directory() -> str:
    """Get system logs directory path as string."""
    return str(storage.system_logs_directory)
