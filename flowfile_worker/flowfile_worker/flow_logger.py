import logging
import os
import threading

import grpc
import requests

from flowfile_worker.configs import FLOWFILE_CORE_URI
from flowfile_worker.models import RawLogInput

LOGGING_URL = FLOWFILE_CORE_URI + "/raw_logs"

# gRPC settings
DEFAULT_LOGGING_GRPC_PORT = 50052
LOGGING_GRPC_HOST = os.environ.get("CORE_HOST", "0.0.0.0")
LOGGING_GRPC_PORT = int(os.environ.get("FLOWFILE_LOGGING_GRPC_PORT", DEFAULT_LOGGING_GRPC_PORT))
USE_GRPC_LOGGING = os.environ.get("FLOWFILE_USE_GRPC", "1") == "1"

# Global gRPC channel and stub for logging
_log_channel: grpc.Channel | None = None
_log_stub = None
_channel_lock = threading.Lock()


def get_logging_stub():
    """Get or create a gRPC stub for the logging service."""
    global _log_channel, _log_stub

    with _channel_lock:
        if _log_stub is None:
            from shared.grpc_protos import LoggingServiceStub

            target = f"{LOGGING_GRPC_HOST}:{LOGGING_GRPC_PORT}"
            _log_channel = grpc.insecure_channel(target)
            _log_stub = LoggingServiceStub(_log_channel)

        return _log_stub


def close_logging_channel():
    """Close the gRPC logging channel."""
    global _log_channel, _log_stub

    with _channel_lock:
        if _log_channel is not None:
            _log_channel.close()
            _log_channel = None
            _log_stub = None


class FlowfileLogHandler(logging.Handler):
    """Log handler that sends logs to the core service via REST or gRPC."""

    def __init__(self, flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1, use_grpc: bool = None):
        super().__init__()
        self.flowfile_flow_id = flowfile_flow_id
        self.flowfile_node_id = flowfile_node_id
        self.use_grpc = use_grpc if use_grpc is not None else USE_GRPC_LOGGING

    def emit(self, record):
        try:
            log_message = self.format(record)

            extra = {"Node Id": self.flowfile_node_id}
            for k, v in extra.items():
                log_message = f"{k}: {v} - {log_message}"

            # Skip logging if flow_id or node_id is invalid
            if self.flowfile_flow_id == -1 or self.flowfile_node_id == -1:
                return

            if self.use_grpc:
                self._emit_grpc(log_message, record.levelname.upper())
            else:
                self._emit_rest(log_message, record.levelname.upper())

        except Exception as e:
            print(f"Error sending log: {e}")

    def _emit_grpc(self, log_message: str, log_type: str):
        """Send log via gRPC."""
        try:
            from shared.grpc_protos import LogEntry

            stub = get_logging_stub()
            entry = LogEntry(
                flow_id=self.flowfile_flow_id,
                log_message=log_message,
                log_type=log_type,
                extra={},
            )
            stub.SendLog(entry, timeout=5)
        except grpc.RpcError as e:
            # Fall back to REST if gRPC fails
            print(f"gRPC logging failed ({e.code()}), falling back to REST")
            self._emit_rest(log_message, log_type)
        except Exception as e:
            print(f"Error sending log via gRPC: {e}")

    def _emit_rest(self, log_message: str, log_type: str):
        """Send log via REST (fallback)."""
        try:
            raw_log_input = RawLogInput(
                flowfile_flow_id=self.flowfile_flow_id,
                log_message=log_message,
                log_type=log_type,
                extra={},
            )
            response = requests.post(
                LOGGING_URL, json=raw_log_input.__dict__, headers={"Content-Type": "application/json"}, timeout=5
            )
            if response.status_code != 200:
                raise Exception(f"Failed to send log: {response.text}")
        except Exception as e:
            print(f"Error sending log to {LOGGING_URL}: {e}")


class GrpcStreamingLogHandler(logging.Handler):
    """
    Log handler that uses gRPC streaming to send logs to the core service.
    More efficient for high-volume logging as it maintains a persistent stream.
    """

    def __init__(self, flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1):
        super().__init__()
        self.flowfile_flow_id = flowfile_flow_id
        self.flowfile_node_id = flowfile_node_id
        self._stream = None
        self._lock = threading.Lock()

    def emit(self, record):
        try:
            from shared.grpc_protos import LogEntry

            log_message = self.format(record)

            extra = {"Node Id": str(self.flowfile_node_id)}
            for k, v in extra.items():
                log_message = f"{k}: {v} - {log_message}"

            if self.flowfile_flow_id == -1 or self.flowfile_node_id == -1:
                return

            stub = get_logging_stub()
            entry = LogEntry(
                flow_id=self.flowfile_flow_id,
                log_message=log_message,
                log_type=record.levelname.upper(),
                extra={},
            )
            stub.SendLog(entry, timeout=5)

        except Exception as e:
            print(f"Error sending log via gRPC stream: {e}")


def get_worker_logger(flowfile_flow_id: int, flowfile_node_id: int | str, use_grpc: bool = None) -> logging.Logger:
    """
    Create a logger configured to send logs to the core service.

    Args:
        flowfile_flow_id: The flow ID to associate logs with
        flowfile_node_id: The node ID to associate logs with
        use_grpc: Whether to use gRPC for logging (defaults to USE_GRPC_LOGGING)

    Returns:
        A configured Logger instance
    """
    if use_grpc is None:
        use_grpc = USE_GRPC_LOGGING

    logger_name = f"NodeLog: {flowfile_node_id}"
    logger = logging.getLogger(logger_name)
    logger.propagate = False  # Prevent propagation to parent loggers
    logger.setLevel(logging.DEBUG)

    # Only add handlers if they don't already exist to avoid duplicates
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

        http_handler = FlowfileLogHandler(
            flowfile_flow_id=flowfile_flow_id,
            flowfile_node_id=flowfile_node_id,
            use_grpc=use_grpc,
        )
        http_handler.setLevel(logging.INFO)
        http_formatter = logging.Formatter("%(message)s")
        http_handler.setFormatter(http_formatter)
        logger.addHandler(http_handler)

    return logger
