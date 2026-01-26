# flowfile_core/flowfile_core/routes/grpc_logging_server.py
"""
gRPC Logging server for receiving logs from the worker.
This replaces the REST /raw_logs endpoint for worker -> core logging.
"""

import threading
from concurrent import futures

import grpc

from flowfile_core.configs import logger
from flowfile_core.configs.settings import SERVER_HOST
from shared.grpc_protos import (
    LogEntry,
    LogResponse,
    LoggingServiceServicer,
    add_LoggingServiceServicer_to_server,
)

# Default gRPC logging port
DEFAULT_LOGGING_GRPC_PORT = 50052

_server: grpc.Server | None = None
_server_lock = threading.Lock()


class LoggingServiceImpl(LoggingServiceServicer):
    """Implementation of the gRPC LoggingService for receiving worker logs."""

    def SendLog(self, request: LogEntry, context):
        """Receive a single log entry from the worker."""
        try:
            from flowfile_core import flow_file_handler

            flow = flow_file_handler.get_flow(request.flow_id)
            if not flow:
                logger.warning(f"gRPC Logging: Flow not found: {request.flow_id}")
                return LogResponse(success=False, message="Flow not found")

            flow_logger = flow.flow_logger

            # Extract extra data
            extra = dict(request.extra) if request.extra else {}

            # Log the message based on type
            if request.log_type == "INFO":
                flow_logger.info(request.log_message, extra=extra)
            elif request.log_type == "ERROR":
                flow_logger.error(request.log_message, extra=extra)
            else:
                flow_logger.info(request.log_message, extra=extra)

            return LogResponse(success=True)

        except Exception as e:
            logger.error(f"gRPC Logging: Error processing log: {str(e)}")
            return LogResponse(success=False, message=str(e))

    def StreamLogs(self, request_iterator, context):
        """Receive a stream of log entries from the worker."""
        try:
            from flowfile_core import flow_file_handler

            count = 0
            for request in request_iterator:
                if context.is_active():
                    flow = flow_file_handler.get_flow(request.flow_id)
                    if flow:
                        flow_logger = flow.flow_logger
                        extra = dict(request.extra) if request.extra else {}

                        if request.log_type == "INFO":
                            flow_logger.info(request.log_message, extra=extra)
                        elif request.log_type == "ERROR":
                            flow_logger.error(request.log_message, extra=extra)
                        else:
                            flow_logger.info(request.log_message, extra=extra)

                        count += 1
                else:
                    break

            return LogResponse(success=True, message=f"Processed {count} log entries")

        except Exception as e:
            logger.error(f"gRPC Logging: Error processing log stream: {str(e)}")
            return LogResponse(success=False, message=str(e))


def start_logging_server(host: str = None, port: int = None) -> grpc.Server:
    """
    Start the gRPC logging server.

    Args:
        host: The host to bind to (defaults to SERVER_HOST)
        port: The port to bind to (defaults to DEFAULT_LOGGING_GRPC_PORT)

    Returns:
        The gRPC server instance
    """
    global _server

    if host is None:
        host = SERVER_HOST
    if port is None:
        port = DEFAULT_LOGGING_GRPC_PORT

    with _server_lock:
        if _server is not None:
            logger.warning("gRPC Logging server already running")
            return _server

        _server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        add_LoggingServiceServicer_to_server(LoggingServiceImpl(), _server)
        _server.add_insecure_port(f"{host}:{port}")
        _server.start()
        logger.info(f"gRPC Logging server started on {host}:{port}")
        return _server


def stop_logging_server(grace: int = 5):
    """Stop the gRPC logging server."""
    global _server

    with _server_lock:
        if _server is not None:
            logger.info("Stopping gRPC Logging server...")
            _server.stop(grace=grace)
            _server = None
            logger.info("gRPC Logging server stopped")


def get_logging_server() -> grpc.Server | None:
    """Get the current gRPC logging server instance."""
    return _server
