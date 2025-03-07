import logging
from pathlib import Path
from datetime import datetime
from flowfile_core.configs.settings import get_temp_dir
import os
import logging.handlers
import queue
import threading

_process_safe_queue = queue.Queue(-1)
main_logger = logging.getLogger('PipelineHandler')


class NodeLogger:
    """Logger for individual flow nodes"""

    def __init__(self, node_id: str | int, flow_id: int, flow_logger_parent: "FlowLogger"):
        self.flow_id = flow_id
        self.node_id = node_id
        self.flow_logger_parent = flow_logger_parent

    @property
    def logger(self) -> logging.Logger:
        return self.flow_logger_parent.logger

    def info(self, msg: str):
        self.logger.info(f"Node ID: {self.node_id} - {msg}")

    def error(self, msg: str):
        self.logger.error(f"Node ID: {self.node_id} - {msg}")

    def warning(self, msg: str):
        self.logger.warning(f"Node ID: {self.node_id} - {msg}")

    def debug(self, msg: str):
        self.logger.debug(f"Node ID: {self.node_id} - {msg}")


class FlowLogger:
    """Thread-safe logger for flow execution"""
    _instances = {}
    _instances_lock = threading.RLock()
    _queue_listener = None
    _queue_listener_lock = threading.Lock()

    @staticmethod
    def handle_extra_log_info(flow_id: int, extra: dict = None) -> dict:
        if extra is None:
            extra = {}
        extra['flow_id'] = flow_id
        return extra

    def __new__(cls, flow_id: int, clear_existing_logs: bool = False):
        with cls._instances_lock:
            if flow_id not in cls._instances:
                instance = super().__new__(cls)
                instance._initialize(flow_id, clear_existing_logs)
                cls._instances[flow_id] = instance
            else:
                instance = cls._instances[flow_id]
                if clear_existing_logs:
                    instance.clear_log_file()
            return instance

    def _initialize(self, flow_id: int, clear_existing_logs: bool):
        self.flow_id = flow_id
        self._logger = None
        self.log_file_path = get_flow_log_file(self.flow_id)
        self._file_lock = threading.RLock()
        self._setup_new_logger()

        with self._queue_listener_lock:
            if not FlowLogger._queue_listener:
                FlowLogger._start_queue_listener()

    def _setup_new_logger(self):
        """Creates a new logger instance with appropriate handlers"""
        logger_name = f'FlowExecution.{self.flow_id}'
        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(logging.INFO)
        self.setup_logging()

    @property
    def logger(self):
        """Get the logger instance"""
        if self._logger is None:
            self._setup_new_logger()
        return self._logger

    def cleanup_self(self):
        """Clean up just this logger instance (not global)"""
        # Try with non-blocking lock first
        if self._file_lock.acquire(blocking=False):
            try:
                self._cleanup_handlers()
            finally:
                self._file_lock.release()
        else:
            # If we can't get lock, proceed anyway
            main_logger.warning(f"Could not acquire lock for flow {self.flow_id}, proceeding with cleanup anyway")
            self._cleanup_handlers()

    def _cleanup_handlers(self):
        """Close and remove all handlers"""
        if self._logger:
            for handler in self._logger.handlers[:]:
                try:
                    handler.close()
                    self._logger.removeHandler(handler)
                except Exception as e:
                    main_logger.error(f"Error closing handler: {e}")

    def recreate_self(self):
        """Recreate this logger instance after cleanup"""
        # Try with non-blocking lock first
        if self._file_lock.acquire(blocking=False):
            try:
                self._recreate_impl()
            finally:
                self._file_lock.release()
        else:
            # If we can't get lock, proceed anyway
            main_logger.warning(f"Could not acquire lock for flow {self.flow_id}, proceeding with recreation anyway")
            self._recreate_impl()

    def _recreate_impl(self):
        """Implementation of recreate operation"""
        # Make sure the log directory exists
        log_dir = Path(self.log_file_path).parent
        log_dir.mkdir(exist_ok=True, parents=True)

        try:
            # Create an empty file
            with open(self.log_file_path, 'w') as f:
                pass

            # Re-setup the logger
            self._setup_new_logger()
            main_logger.info(f"Log file was recreated for flow {self.flow_id}")
        except Exception as e:
            main_logger.error(f"Error recreating log file for flow {self.flow_id}: {e}")

    def refresh_logger_if_needed(self):
        """Check if log file exists and refresh logger if needed"""
        if not os.path.exists(self.log_file_path):
            main_logger.info(f"Log file missing, recreating: {self.log_file_path}")
            self.cleanup_self()
            self.recreate_self()
            return True
        return False

    @classmethod
    def _start_queue_listener(cls):
        """Start the queue listener for asynchronous logging"""
        queue_handler = logging.handlers.QueueHandler(_process_safe_queue)
        cls._queue_listener = logging.handlers.QueueListener(
            _process_safe_queue,
            queue_handler,
            respect_handler_level=True
        )
        cls._queue_listener.start()

    def setup_logging(self):
        """Set up file handlers for logging"""
        if self._file_lock.acquire(blocking=False):
            try:
                self._setup_logging_impl()
            finally:
                self._file_lock.release()
        else:
            # Try with timeout
            if self._file_lock.acquire(timeout=1):
                try:
                    self._setup_logging_impl()
                finally:
                    self._file_lock.release()
            else:
                # If still can't get lock, proceed anyway
                main_logger.warning(f"Could not acquire lock for flow {self.flow_id}, proceeding with setup anyway")
                self._setup_logging_impl()

    def _setup_logging_impl(self):
        """Implementation of setup_logging without lock handling"""
        # Remove existing handlers
        for handler in self._logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                self._logger.removeHandler(handler)

        # Make sure the log directory exists
        log_dir = Path(self.log_file_path).parent
        log_dir.mkdir(exist_ok=True, parents=True)

        # Add file handler
        file_handler = logging.FileHandler(self.log_file_path)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

    def clear_log_file(self):
        """Clear the log file for this flow"""
        if self._file_lock.acquire(blocking=False):
            try:
                self._clear_log_impl()
            finally:
                self._file_lock.release()
        else:
            # If can't get lock, try with timeout
            if self._file_lock.acquire(timeout=1):
                try:
                    self._clear_log_impl()
                finally:
                    self._file_lock.release()
            else:
                # If still can't get lock, proceed anyway
                main_logger.warning(
                    f"Could not acquire lock for flow {self.flow_id}, proceeding with file clearing anyway")
                self._clear_log_impl()

    def _clear_log_impl(self):
        """Implementation of clear_log_file without lock handling"""
        try:
            # Ensure parent directory exists
            self.refresh_logger_if_needed()
            # Truncate file
            with open(self.log_file_path, 'w') as f:
                pass
            main_logger.info(f"Log file cleared for flow {self.flow_id}")
        except Exception as e:
            main_logger.error(f"Error clearing log file {self.log_file_path}: {e}")

    @classmethod
    def cleanup_instance(cls, flow_id: int):
        """Clean up a specific flow logger instance"""
        with cls._instances_lock:
            if flow_id in cls._instances:
                instance = cls._instances[flow_id]
                instance.cleanup_logging()
                del cls._instances[flow_id]

    def cleanup_logging(self):
        """Clean up logging for this flow"""
        if self._file_lock.acquire(blocking=False):
            try:
                self._cleanup_handlers()
            finally:
                self._file_lock.release()
        else:
            # If can't get lock, proceed anyway
            main_logger.warning(f"Could not acquire lock for flow {self.flow_id}, proceeding with cleanup anyway")
            self._cleanup_handlers()

    @classmethod
    def get_instance(cls, flow_id: int):
        """Get an existing flow logger instance without creating a new one"""
        with cls._instances_lock:
            return cls._instances.get(flow_id)

    def get_node_logger(self, node_id: str | int) -> NodeLogger:
        """Get a logger for a specific node in this flow"""
        return NodeLogger(node_id, flow_id=self.flow_id, flow_logger_parent=self)

    # Logging methods with automatic refresh
    def info(self, msg: str, extra: dict = None, node_id: str | int = -1):
        self.refresh_logger_if_needed()
        if node_id != -1:
            msg = f"Node ID: {node_id} - {msg}"
        extra = self.handle_extra_log_info(self.flow_id, extra)
        self.logger.info(msg, extra=extra)

    def error(self, msg: str, extra: dict = None, node_id: str | int = -1):
        self.refresh_logger_if_needed()
        if node_id != -1:
            msg = f"Node ID: {node_id} - {msg}"
        extra = self.handle_extra_log_info(self.flow_id, extra)
        self.logger.error(msg, extra=extra)

    def warning(self, msg: str, extra: dict = None, node_id: str | int = -1):
        self.refresh_logger_if_needed()
        if node_id != -1:
            msg = f"Node ID: {node_id} - {msg}"
        extra = self.handle_extra_log_info(self.flow_id, extra)
        self.logger.warning(msg, extra=extra)

    def debug(self, msg: str, extra: dict = None, node_id: str | int = -1):
        self.refresh_logger_if_needed()
        if node_id != -1:
            msg = f"Node ID: {node_id} - {msg}"
        extra = self.handle_extra_log_info(self.flow_id, extra)
        self.logger.debug(msg, extra=extra)

    def get_log_filepath(self):
        """Get the path to the log file for this flow"""
        return str(self.log_file_path)

    def read_from_line(self, start_line: int = 0):
        """Read log content starting from a specific line"""
        # Refresh logger if needed before reading
        self.refresh_logger_if_needed()

        if self._file_lock.acquire(blocking=False):
            try:
                return read_log_from_line(self.log_file_path, start_line)
            finally:
                self._file_lock.release()
        else:
            # Reading is safe without lock
            return read_log_from_line(self.log_file_path, start_line)

    @classmethod
    def refresh_all_loggers(cls):
        """Refresh all loggers that need it"""
        with cls._instances_lock:
            for flow_id, instance in cls._instances.items():
                try:
                    instance.refresh_logger_if_needed()
                except Exception as e:
                    main_logger.error(f"Error refreshing logger for flow {flow_id}: {e}")

    @classmethod
    def global_cleanup(cls):
        """Cleanup all loggers, handlers and queue listener."""
        with cls._instances_lock:
            # Get a copy of keys to avoid modification during iteration
            flow_ids = list(cls._instances.keys())

            # Cleanup all instances
            for flow_id in flow_ids:
                try:
                    cls.cleanup_instance(flow_id)
                except Exception as e:
                    main_logger.error(f"Error cleaning up instance for flow {flow_id}: {e}")

            # Stop queue listener
            with cls._queue_listener_lock:
                if cls._queue_listener:
                    try:
                        cls._queue_listener.stop()
                        cls._queue_listener = None
                    except Exception as e:
                        main_logger.error(f"Error stopping queue listener: {e}")

            # Clear instances
            cls._instances.clear()

    def __del__(self):
        """Cleanup instance on deletion."""
        try:
            self.cleanup_instance(self.flow_id)
        except:
            pass  # Ignore errors during deletion


def get_logs_dir() -> Path:
    """Get the directory for flow logs"""
    base_dir = Path(get_temp_dir())
    logs_dir = base_dir / "flowfile_logs"
    logs_dir.mkdir(exist_ok=True, parents=True)
    return logs_dir


def get_flow_log_file(flow_id: int) -> Path:
    """Get the path to the log file for a specific flow"""
    return get_logs_dir() / f"flow_{flow_id}.log"


def cleanup_old_logs(max_age_days: int = 7):
    """Delete log files older than specified days"""
    logs_dir = get_logs_dir()
    now = datetime.now().timestamp()
    deleted_count = 0

    for log_file in logs_dir.glob("flow_*.log"):
        try:
            if (now - log_file.stat().st_mtime) > (max_age_days * 24 * 60 * 60):
                log_file.unlink()
                deleted_count += 1
        except Exception as e:
            main_logger.error(f"Failed to delete old log file {log_file}: {e}")

    if deleted_count > 0:
        main_logger.info(f"Deleted {deleted_count} old log files")


def clear_all_flow_logs():
    """Delete all flow log files"""
    logs_dir = get_logs_dir()
    deleted_count = 0

    try:
        # First close all handlers
        with FlowLogger._instances_lock:
            for flow_id, instance in FlowLogger._instances.items():
                try:
                    if instance._logger:
                        for handler in instance._logger.handlers[:]:
                            if isinstance(handler, logging.FileHandler):
                                handler.close()
                                instance._logger.removeHandler(handler)
                except Exception as e:
                    main_logger.error(f"Error closing handlers for flow {flow_id}: {e}")

        # Now delete all log files
        for log_file in logs_dir.glob("*.log"):
            try:
                os.remove(log_file)
                deleted_count += 1
            except Exception as e:
                main_logger.error(f"Error removing log file {log_file}: {e}")

        main_logger.info(f"Successfully deleted {deleted_count} flow log files")
    except Exception as e:
        main_logger.error(f"Failed to delete flow log files: {e}")


def read_log_from_line(log_file_path: Path, start_line: int = 0):
    """Read log file content starting from a specific line"""
    lines = []
    try:
        with open(log_file_path, "r") as file:
            # Skip lines efficiently if needed
            if start_line > 0:
                for _ in range(start_line):
                    next(file, None)

            # Read remaining lines
            lines = file.readlines()
    except FileNotFoundError:
        main_logger.error(f"Log file not found: {log_file_path}")
    except Exception as e:
        main_logger.error(f"Error reading log file {log_file_path}: {e}")

    return lines