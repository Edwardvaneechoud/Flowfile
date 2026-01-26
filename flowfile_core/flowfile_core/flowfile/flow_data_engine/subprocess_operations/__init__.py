# flowfile_core/flowfile_core/flowfile/flow_data_engine/subprocess_operations/__init__.py
"""
Unified interface for worker communication.
Automatically selects between REST and gRPC based on configuration.
"""

from flowfile_core.configs.settings import USE_GRPC

# Import models that are needed regardless of transport
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import (
    FuzzyJoinInput,
    OperationType,
    PolarsOperation,
    Status,
)

if USE_GRPC:
    # Use gRPC client
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
else:
    # Use REST client (original implementation)
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
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
    )

    # REST client doesn't have these
    def close_grpc_channel():
        """No-op for REST client."""
        pass

    def trigger_write_results(*args, **kwargs):
        """Not available in REST client - use trigger_df_operation with write_output operation."""
        raise NotImplementedError("trigger_write_results is only available with gRPC client")

__all__ = [
    # Models
    "OperationType",
    "PolarsOperation",
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
