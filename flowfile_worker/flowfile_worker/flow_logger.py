import logging
from pathlib import Path
from datetime import datetime
from flowfile_core.configs.settings import get_temp_dir
import os

# Base logger setup
main_logger = logging.getLogger('FlowfileWorker')


class FlowLogger:
    """Helper class to automatically add flow_id to logs, manage flow-specific loggers, and store log file location."""

    # Class-level dictionary to track instances by flow_id
    _instances = {}

    def __init__(self, flow_id: int):
        self.flow_id = flow_id

        # If an instance with this flow_id exists, clean it up first
        if flow_id in self._instances:
            old_instance = self._instances[flow_id]
            old_instance.cleanup_logging()
            old_instance.clear_log_file()

        # Create new logger and setup
        self.logger = logging.getLogger(f'FlowExecution.{flow_id}')
        self.logger.setLevel(logging.INFO)
        self.log_file_path = get_flow_log_file(self.flow_id)
        self.setup_logging()

        self._instances[flow_id] = self

    def setup_logging(self):
        """Set up logging for a specific flow"""
        file_handler = logging.FileHandler(self.log_file_path)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def cleanup_logging(self):
        """Clean up logging for a specific flow"""
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)

    def clear_log_file(self):
        """Clear the contents of the log file without deleting it"""
        try:
            with open(self.log_file_path, 'w') as f:
                pass
            self.info("Log file cleared - starting new flow execution")
        except Exception as e:
            main_logger.error(f"Error clearing log file {self.log_file_path}: {e}")

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
        return read_log_from_line(self.log_file_path, start_line)


def get_logs_dir() -> Path:
    """Get the logs directory path, respecting Docker environment"""
    base_dir = Path(get_temp_dir())
    logs_dir = base_dir / "flowfile_logs"
    logs_dir.mkdir(exist_ok=True, parents=True)
    return logs_dir


def get_flow_log_file(flow_id: int) -> Path:
    """Get the path to a flow's current log file"""
    return get_logs_dir() / f"flow_{flow_id}.log"


def cleanup_old_logs(max_age_days: int = 7):
    """Clean up log files older than max_age_days"""
    logs_dir = get_logs_dir()

    now = datetime.now().timestamp()
    for log_file in logs_dir.glob("flow_*.log"):
        try:
            if (now - log_file.stat().st_mtime) > (max_age_days * 24 * 60 * 60):
                log_file.unlink()
        except Exception as e:
            main_logger.error(f"Failed to delete old log file {log_file}: {e}")


def clear_all_flow_logs():
    """Deletes all .log files within the flowfile_logs directory."""
    logs_dir = get_logs_dir()
    try:
        for log_file in logs_dir.glob("*.log"):  # Only match .log files
            os.remove(log_file)
        main_logger.info(f"Successfully deleted all flow log files in {logs_dir}")
    except Exception as e:
        main_logger.error(f"Failed to delete flow log files in {logs_dir}: {e}")


def read_log_from_line(log_file_path: Path, start_line: int = 0):
    """
    Reads a log file from a specific line number.

    Args:
        log_file_path: The path to the log file.
        start_line: The line number to start reading from (0-indexed).

    Returns:
        A list of strings, where each string is a line from the log file
        starting from `start_line`.
    """

    lines = []
    try:
        with open(log_file_path, "r") as file:
            for i, line in enumerate(file):
                if i >= start_line:
                    lines.append(line)
    except FileNotFoundError:
        main_logger.error(f"Log file not found: {log_file_path}")
        return []  # Or raise an exception, depending on how you want to handle it
    except Exception as e:
        main_logger.error(f"Error reading log file {log_file_path}: {e}")
        return []  # Or raise an exception

    return lines