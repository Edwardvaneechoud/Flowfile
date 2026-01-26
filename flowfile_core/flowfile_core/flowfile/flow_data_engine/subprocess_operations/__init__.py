# flowfile_core/flowfile_core/flowfile/flow_data_engine/subprocess_operations/__init__.py
"""
Worker communication via gRPC.
"""

# Import models
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import (
    FuzzyJoinInput,
    OperationType,
    PolarsOperation,
    PolarsScript,
    Status,
)

# Import gRPC client functions and classes
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.grpc_client import (
    BaseFetcher,
    ExternalCloudWriter,
    ExternalCreateFetcher,
    ExternalDatabaseFetcher,
    ExternalDatabaseWriter,
    ExternalDfFetcher,
    ExternalExecutorTracker,
    ExternalFuzzyMatchFetcher,
    ExternalSampler,
    cancel_task,
    clear_task_from_worker,
    close_grpc_channel,
    fetch_unique_values,
    get_df_result,
    get_external_df_result,
    get_results,
    get_status,
    results_exists,
    trigger_cloud_storage_write,
    trigger_create_operation,
    trigger_database_read_collector,
    trigger_database_write,
    trigger_df_operation,
    trigger_fuzzy_match_operation,
    trigger_sample_operation,
    trigger_write_results,
)

__all__ = [
    # Models
    "OperationType",
    "PolarsOperation",
    "PolarsScript",
    "FuzzyJoinInput",
    "Status",
    # Functions
    "trigger_df_operation",
    "trigger_sample_operation",
    "trigger_fuzzy_match_operation",
    "trigger_create_operation",
    "trigger_database_read_collector",
    "trigger_database_write",
    "trigger_cloud_storage_write",
    "trigger_write_results",
    "get_results",
    "results_exists",
    "clear_task_from_worker",
    "get_df_result",
    "get_external_df_result",
    "get_status",
    "cancel_task",
    "fetch_unique_values",
    "close_grpc_channel",
    # Classes
    "BaseFetcher",
    "ExternalDfFetcher",
    "ExternalSampler",
    "ExternalFuzzyMatchFetcher",
    "ExternalCreateFetcher",
    "ExternalDatabaseFetcher",
    "ExternalDatabaseWriter",
    "ExternalCloudWriter",
    "ExternalExecutorTracker",
]
