import logging
from pathlib import Path
from datetime import datetime
from flowfile_core.configs.settings import get_temp_dir
import os
import logging.handlers
import multiprocessing
import threading

_process_safe_queue = multiprocessing.Queue(-1)
main_logger = logging.getLogger('PipelineHandler')


class FlowLogger:
    _instances = {}
    _instances_lock = threading.RLock()
    _queue_listener = None
    _queue_listener_lock = threading.Lock()

    def __new__(cls, flow_id: int, clear_existing_logs: bool = False):
        with cls._instances_lock:
            if flow_id not in cls._instances:
                instance = super().__new__(cls)
                instance._initialize(flow_id, clear_existing_logs)
                cls._instances[flow_id] = instance
            else:
                instance = cls._instances[flow_id]
                if clear_existing_logs:
                    instance.clear_log_file()  # Only clear file, not handlers
            return instance

    def _initialize(self, flow_id: int, clear_existing_logs: bool):
        self.flow_id = flow_id
        self._setup_new_logger()

        with self._queue_listener_lock:
            if not FlowLogger._queue_listener:
                FlowLogger._start_queue_listener()

    def _setup_new_logger(self):
        self.logger = logging.getLogger(f'FlowExecution.{self.flow_id}')
        self.logger.setLevel(logging.INFO)
        self.log_file_path = get_flow_log_file(self.flow_id)
        self._file_lock = threading.Lock()
        self.setup_logging()

    def clear_previous_logs(self):
        self.clear_log_file()

    @classmethod
    def _start_queue_listener(cls):
        queue_handler = logging.handlers.QueueHandler(_process_safe_queue)
        cls._queue_listener = logging.handlers.QueueListener(
            _process_safe_queue,
            queue_handler,
            respect_handler_level=True
        )
        cls._queue_listener.start()

    def setup_logging(self):
        with self._file_lock:
            file_handler = logging.FileHandler(self.log_file_path)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def clear_log_file(self):
        with self._file_lock:
            try:
                with open(self.log_file_path, 'w') as f:
                    pass
                main_logger.info("Log file cleared - starting new flow execution")
            except Exception as e:
                main_logger.error(f"Error clearing log file {self.log_file_path}: {e}")

    @classmethod
    def cleanup_instance(cls, flow_id: int):
        with cls._instances_lock:
            if flow_id in cls._instances:
                instance = cls._instances[flow_id]
                instance.cleanup_logging()
                del cls._instances[flow_id]

    def cleanup_logging(self):
        with self._file_lock:
            for handler in self.logger.handlers[:]:
                handler.close()
                self.logger.removeHandler(handler)

    @classmethod
    def get_instance(cls, flow_id: int):
        with cls._instances_lock:
            return cls._instances.get(flow_id)

    def info(self, msg: str):
        self.logger.info(msg, extra={'flow_id': self.flow_id})

    def error(self, msg: str):
        self.logger.error(msg, extra={'flow_id': self.flow_id})

    def warning(self, msg: str):
        self.logger.warning(msg, extra={'flow_id': self.flow_id})

    def debug(self, msg: str):
        self.logger.debug(msg, extra={'flow_id': self.flow_id})

    def get_log_filepath(self):
        return str(self.log_file_path)

    def read_from_line(self, start_line: int = 0):
        with self._file_lock:
            return read_log_from_line(self.log_file_path, start_line)

    @classmethod
    def global_cleanup(cls):
        """Cleanup all loggers, handlers and queue listener."""
        with cls._instances_lock:
            # Cleanup all instances
            for flow_id in list(cls._instances.keys()):
                cls.cleanup_instance(flow_id)

            # Stop queue listener
            with cls._queue_listener_lock:
                if cls._queue_listener:
                    cls._queue_listener.stop()
                    cls._queue_listener = None

            # Clear instances
            cls._instances.clear()

    def __del__(self):
        """Cleanup instance on deletion."""
        self.cleanup_instance(self.flow_id)


def get_logs_dir() -> Path:
    base_dir = Path(get_temp_dir())
    logs_dir = base_dir / "flowfile_logs"
    logs_dir.mkdir(exist_ok=True, parents=True)
    return logs_dir


def get_flow_log_file(flow_id: int) -> Path:
    return get_logs_dir() / f"flow_{flow_id}.log"


def cleanup_old_logs(max_age_days: int = 7):
    logs_dir = get_logs_dir()
    now = datetime.now().timestamp()

    for log_file in logs_dir.glob("flow_*.log"):
        try:
            if (now - log_file.stat().st_mtime) > (max_age_days * 24 * 60 * 60):
                log_file.unlink()
        except Exception as e:
            main_logger.error(f"Failed to delete old log file {log_file}: {e}")


def clear_all_flow_logs():
    logs_dir = get_logs_dir()
    try:
        for log_file in logs_dir.glob("*.log"):
            os.remove(log_file)
        main_logger.info(f"Successfully deleted all flow log files in {logs_dir}")
    except Exception as e:
        main_logger.error(f"Failed to delete flow log files in {logs_dir}: {e}")


def read_log_from_line(log_file_path: Path, start_line: int = 0):
    lines = []
    try:
        with open(log_file_path, "r") as file:
            for i, line in enumerate(file):
                if i >= start_line:
                    lines.append(line)
    except FileNotFoundError:
        main_logger.error(f"Log file not found: {log_file_path}")
        return []
    except Exception as e:
        main_logger.error(f"Error reading log file {log_file_path}: {e}")
        return []
    return lines