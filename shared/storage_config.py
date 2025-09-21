# shared/storage_config.py - Updated for Option 3
"""
Centralized storage configuration for Flowfile.
This module can be imported by both core and worker without creating dependencies.
"""
import os
from pathlib import Path
from typing import Optional, Literal

DirectoryOptions = Literal["temp_directory", "logs_directory",
                            "system_logs_directory", "database_directory",
                            "cache_directory", "flows_directory", "user_defined_nodes_directory"]


class FlowfileStorage:
    """Centralized storage manager for Flowfile applications."""

    def __init__(self):
        self._base_dir: Optional[Path] = None
        self._user_data_dir: Optional[Path] = None
        self._ensure_directories()

    @property
    def base_directory(self) -> Path:
        """Get the base Flowfile storage directory (for internal container communication)."""
        if self._base_dir is None:
            if os.environ.get("RUNNING_IN_DOCKER") == "true":
                # In Docker, internal storage stays inside /app
                base_path = os.environ.get("FLOWFILE_STORAGE_DIR", "/app/internal_storage")
            else:
                # Local development
                base_path = os.environ.get("FLOWFILE_STORAGE_DIR")
                if not base_path:
                    home_dir = Path.home()
                    base_path = home_dir / ".flowfile"

            self._base_dir = Path(base_path)
        return self._base_dir

    @property
    def user_data_directory(self) -> Path:
        """Get the user data directory (completely separate from application code)."""
        if self._user_data_dir is None:
            if os.environ.get("RUNNING_IN_DOCKER") == "true":
                # In Docker, user data is at /data/user (completely outside /app)
                user_data_path = os.environ.get("FLOWFILE_USER_DATA_DIR", "/data/user")
            else:
                # Local development - use user's home directory
                user_data_path = Path.home()

            self._user_data_dir = Path(user_data_path)
        return self._user_data_dir

    @property
    def cache_directory(self) -> Path:
        """Cache directory for worker-core communication (internal)."""
        return self.base_directory / "cache"

    def get_flow_cache_directory(self, flow_id: int) -> Path:
        """Get or create a cache directory for a specific flow (internal)."""
        flow_cache_dir = self.cache_directory / str(flow_id)
        flow_cache_dir.mkdir(parents=True, exist_ok=True)
        return flow_cache_dir

    @property
    def system_logs_directory(self) -> Path:
        """Directory for system logs (internal)."""
        return self.base_directory / "system_logs"

    @property
    def flows_directory(self) -> Path:
        """Directory for flow storage (user-accessible)."""
        if os.environ.get("RUNNING_IN_DOCKER") == "true":
            # In Docker, flows are in separate user data area
            return self.user_data_directory / "flows"
        else:
            # Local development - flows in ~/.flowfile/flows
            return self.base_directory / "flows"

    @property
    def uploads_directory(self) -> Path:
        """Directory for user uploads (user-accessible)."""
        if os.environ.get("RUNNING_IN_DOCKER") == "true":
            # In Docker, uploads are in separate user data area
            return self.user_data_directory / "uploads"
        else:
            # Local development - uploads in ~/.flowfile/uploads
            return self.base_directory / "uploads"

    @property
    def user_defined_nodes_directory(self) -> Path:
        """Directory for user-defined custom nodes (user-accessible)."""
        if os.environ.get("RUNNING_IN_DOCKER") == "true":
            return self.user_data_directory / "user_defined_nodes"
        else:
            return self.base_directory / "user_defined_nodes"

    @property
    def user_defined_nodes_icons(self) -> Path:
        """Directory for user-defined custom node icon (user-accessible)."""
        return self.user_defined_nodes_directory / "icons"

    @property
    def outputs_directory(self) -> Path:
        """Directory for user outputs (user-accessible)."""
        if os.environ.get("RUNNING_IN_DOCKER") == "true":
            # In Docker, outputs are in separate user data area
            return self.user_data_directory / "outputs"
        else:
            # Local development - outputs in ~/.flowfile/outputs
            return self.base_directory / "outputs"

    @property
    def database_directory(self) -> Path:
        """Directory for local database files (internal)."""
        return self.base_directory / "database"

    @property
    def logs_directory(self) -> Path:
        """Directory for application logs (internal)."""
        return self.base_directory / "logs"

    @property
    def temp_directory(self) -> Path:
        """Directory for temporary files (internal)."""
        return self.base_directory / "temp"

    @property
    def temp_directory_for_flows(self) -> Path:
        """Directory for temporary files specific to flows (internal)."""
        return self.temp_directory / "flows"

    def _ensure_directories(self) -> None:
        """Create all necessary directories if they don't exist."""
        # Internal directories (always created in base_directory)
        internal_directories = [
            self.cache_directory,
            self.database_directory,
            self.logs_directory,
            self.temp_directory,
            self.system_logs_directory,
            self.temp_directory_for_flows,
        ]

        # User-accessible directories (location depends on environment)
        user_directories = [
            self.flows_directory,
            self.uploads_directory,
            self.outputs_directory,
            self.user_defined_nodes_directory,
            self.user_defined_nodes_icons,
        ]

        for directory in internal_directories + user_directories:
            directory.mkdir(parents=True, exist_ok=True)

    def get_cache_file_path(self, filename: str) -> Path:
        """Get full path for a cache file (internal)."""
        return self.cache_directory / filename

    def get_flow_file_path(self, filename: str) -> Path:
        """Get full path for a flow file (user-accessible)."""
        return self.flows_directory / filename

    def get_upload_file_path(self, filename: str) -> Path:
        """Get full path for an uploaded file (user-accessible)."""
        return self.uploads_directory / filename

    def get_output_file_path(self, filename: str) -> Path:
        """Get full path for an output file (user-accessible)."""
        return self.outputs_directory / filename

    def get_log_file_path(self, filename: str) -> Path:
        """Get full path for an application log file (internal)."""
        return self.logs_directory / filename

    def get_system_log_file_path(self, filename: str) -> Path:
        """Get full path for a system log file (internal)."""
        return self.system_logs_directory / filename

    def get_temp_file_path(self, filename: str) -> Path:
        """Get full path for a temporary file (internal)."""
        return self.temp_directory / filename

    def cleanup_directory(self, directory_option: DirectoryOptions, storage_duration_hours: int = 24) -> None:
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
        cutoff_time = current_time - (storage_duration_hours * 60 * 60)

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

    def cleanup_directories(self) -> None:
        """Clean up temporary files older than specified hours."""
        self.cleanup_directory("temp_directory", storage_duration_hours=24)
        self.cleanup_directory("cache_directory", storage_duration_hours=1)
        self.cleanup_directory("logs_directory", storage_duration_hours=168)
        self.cleanup_directory("system_logs_directory", storage_duration_hours=168)


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


def get_uploads_directory() -> str:
    """Get uploads directory path as string."""
    return str(storage.uploads_directory)


def get_outputs_directory() -> str:
    """Get outputs directory path as string."""
    return str(storage.outputs_directory)


def get_logs_directory() -> str:
    """Get application logs directory path as string."""
    return str(storage.logs_directory)


def get_system_logs_directory() -> str:
    """Get system logs directory path as string."""
    return str(storage.system_logs_directory)
