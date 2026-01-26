# shared/grpc_protos/__init__.py
"""
gRPC Protocol Buffers and generated stubs for Flowfile worker-core communication.
"""

from .worker_service_pb2 import (
    CloudStorageConnection,
    CloudStorageWriteRequest,
    CloudStorageWriteSettings,
    CreateTableRequest,
    DatabaseConnection,
    DatabaseReadRequest,
    DatabaseWriteRequest,
    Empty,
    FetchResultsResponse,
    FuzzyJoinRequest,
    FuzzyMapping,
    LogEntry,
    LogResponse,
    MemoryUsageResponse,
    MessageResponse,
    PolarsOperation,
    StatusResponse,
    StoreSampleRequest,
    SubmitQueryRequest,
    TaskIdRequest,
    TaskIdsResponse,
    WriteResultsRequest,
)
from .worker_service_pb2_grpc import (
    LoggingServiceServicer,
    LoggingServiceStub,
    WorkerServiceServicer,
    WorkerServiceStub,
    add_LoggingServiceServicer_to_server,
    add_WorkerServiceServicer_to_server,
)

__all__ = [
    # Message types
    "Empty",
    "TaskIdRequest",
    "SubmitQueryRequest",
    "StoreSampleRequest",
    "PolarsOperation",
    "FuzzyMapping",
    "FuzzyJoinRequest",
    "CreateTableRequest",
    "DatabaseConnection",
    "DatabaseReadRequest",
    "DatabaseWriteRequest",
    "CloudStorageConnection",
    "CloudStorageWriteSettings",
    "CloudStorageWriteRequest",
    "WriteResultsRequest",
    "StatusResponse",
    "FetchResultsResponse",
    "MemoryUsageResponse",
    "TaskIdsResponse",
    "MessageResponse",
    "LogEntry",
    "LogResponse",
    # Service stubs and servicers
    "WorkerServiceStub",
    "WorkerServiceServicer",
    "LoggingServiceStub",
    "LoggingServiceServicer",
    "add_WorkerServiceServicer_to_server",
    "add_LoggingServiceServicer_to_server",
]
