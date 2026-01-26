import logging
import os
import threading

import grpc

# gRPC settings
DEFAULT_LOGGING_GRPC_PORT = 50052
LOGGING_GRPC_HOST = os.environ.get("CORE_HOST", "0.0.0.0")
LOGGING_GRPC_PORT = int(os.environ.get("FLOWFILE_LOGGING_GRPC_PORT", DEFAULT_LOGGING_GRPC_PORT))

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
    """Log handler that sends logs to the core service via gRPC."""

    def __init__(self, flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1):
        super().__init__()
        self.flowfile_flow_id = flowfile_flow_id
        self.flowfile_node_id = flowfile_node_id

    def emit(self, record):
        try:
            log_message = self.format(record)

            extra = {"Node Id": self.flowfile_node_id}
            for k, v in extra.items():
                log_message = f"{k}: {v} - {log_message}"

            # Skip logging if flow_id or node_id is invalid
            if self.flowfile_flow_id == -1 or self.flowfile_node_id == -1:
                return

            self._emit_grpc(log_message, record.levelname.upper())

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
            print(f"gRPC logging failed: {e.code()}")
        except Exception as e:
            print(f"Error sending log via gRPC: {e}")


def get_worker_logger(flowfile_flow_id: int, flowfile_node_id: int | str) -> logging.Logger:
    """
    Create a logger configured to send logs to the core service via gRPC.

    Args:
        flowfile_flow_id: The flow ID to associate logs with
        flowfile_node_id: The node ID to associate logs with

    Returns:
        A configured Logger instance
    """
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

        grpc_handler = FlowfileLogHandler(
            flowfile_flow_id=flowfile_flow_id,
            flowfile_node_id=flowfile_node_id,
        )
        grpc_handler.setLevel(logging.INFO)
        grpc_formatter = logging.Formatter("%(message)s")
        grpc_handler.setFormatter(grpc_formatter)
        logger.addHandler(grpc_handler)

    return logger
