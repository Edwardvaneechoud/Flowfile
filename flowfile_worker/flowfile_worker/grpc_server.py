# flowfile_worker/flowfile_worker/grpc_server.py
"""
gRPC server implementation for the Flowfile worker service.
This replaces the REST API endpoints with gRPC services.
"""

import json
import os
import uuid
from concurrent import futures
from time import sleep

import grpc
import polars as pl

from flowfile_worker import CACHE_DIR, PROCESS_MEMORY_USAGE, models, status_dict, status_dict_lock
from flowfile_worker.configs import logger
from flowfile_worker.create import FileType, table_creator_factory_method
from flowfile_worker.create.models import ReceivedTable
from flowfile_worker.external_sources.sql_source.main import read_sql_source
from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings
from flowfile_worker.spawner import process_manager, start_fuzzy_process, start_generic_process, start_process
from shared.grpc_protos import (
    Empty,
    FetchResultsResponse,
    MemoryUsageResponse,
    MessageResponse,
    StatusResponse,
    TaskIdsResponse,
    WorkerServiceServicer,
    add_WorkerServiceServicer_to_server,
)


def create_and_get_default_cache_dir(flowfile_flow_id: int) -> str:
    """Create and return the cache directory for a flow."""
    default_cache_dir = CACHE_DIR / str(flowfile_flow_id)
    default_cache_dir.mkdir(parents=True, exist_ok=True)
    return str(default_cache_dir)


def status_to_response(status: models.Status) -> StatusResponse:
    """Convert a models.Status to a gRPC StatusResponse."""
    response = StatusResponse(
        task_id=status.background_task_id,
        status=status.status,
        file_ref=status.file_ref,
        progress=status.progress or 0,
        result_type=status.result_type or "polars",
    )
    if status.error_message:
        response.error_message = status.error_message
    if status.results:
        # Serialize the results if they exist
        if isinstance(status.results, bytes):
            response.results = status.results
        elif hasattr(status.results, "serialize"):
            response.results = status.results.serialize()
        else:
            response.results = str(status.results).encode()
    return response


def validate_result(task_id: str) -> bool | None:
    """
    Validate the result of a completed task by checking the IPC file.

    Args:
        task_id (str): ID of the task to validate

    Returns:
        bool | None: True if valid, False if error, None if not applicable
    """
    logger.debug(f"Validating result for task: {task_id}")
    status = status_dict.get(task_id)
    if status is None:
        return None
    if status.status == "Completed" and status.result_type == "polars":
        try:
            pl.scan_ipc(status.file_ref)
            logger.debug(f"Validation successful for task: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Validation failed for task {task_id}: {str(e)}")
            return False
    return True


class WorkerServiceImpl(WorkerServiceServicer):
    """Implementation of the gRPC WorkerService."""

    def SubmitQuery(self, request, context):
        """Submit a dataframe query for processing."""
        try:
            task_id = request.task_id or str(uuid.uuid4())
            operation_type = request.operation_type or "store"
            flow_id = request.flow_id or 1
            node_id = request.node_id or "-1"

            # Try to parse node_id as int
            try:
                node_id = int(node_id)
            except ValueError:
                pass

            logger.info(f"gRPC: Processing query with operation: {operation_type}")

            default_cache_dir = create_and_get_default_cache_dir(flow_id)
            file_path = os.path.join(default_cache_dir, f"{task_id}.arrow")
            result_type = "polars" if operation_type == "store" else "other"

            status = models.Status(
                background_task_id=task_id, status="Starting", file_ref=file_path, result_type=result_type
            )
            status_dict[task_id] = status

            # Start the background task in a thread
            import threading

            thread = threading.Thread(
                target=start_process,
                kwargs={
                    "polars_serializable_object": request.polars_data,
                    "task_id": task_id,
                    "operation": operation_type,
                    "file_ref": file_path,
                    "flowfile_flow_id": flow_id,
                    "flowfile_node_id": node_id,
                    "kwargs": {},
                },
                daemon=True,
            )
            thread.start()

            logger.info(f"gRPC: Started background task: {task_id}")
            return status_to_response(status)

        except Exception as e:
            logger.error(f"gRPC: Error processing query: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return StatusResponse(task_id="", status="Error", file_ref="", error_message=str(e), result_type="other")

    def StoreSample(self, request, context):
        """Store a sample of a dataframe."""
        try:
            task_id = request.task_id or str(uuid.uuid4())
            sample_size = request.sample_size or 100
            flow_id = request.flow_id or 1
            node_id = request.node_id or "-1"

            try:
                node_id = int(node_id)
            except ValueError:
                pass

            logger.info(f"gRPC: Processing sample storage with size: {sample_size}")

            default_cache_dir = create_and_get_default_cache_dir(flow_id)
            file_path = os.path.join(default_cache_dir, f"{task_id}.arrow")

            status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="other")
            status_dict[task_id] = status

            import threading

            thread = threading.Thread(
                target=start_process,
                kwargs={
                    "polars_serializable_object": request.polars_data,
                    "task_id": task_id,
                    "operation": "store_sample",
                    "file_ref": file_path,
                    "flowfile_flow_id": flow_id,
                    "flowfile_node_id": node_id,
                    "kwargs": {"sample_size": sample_size},
                },
                daemon=True,
            )
            thread.start()

            logger.info(f"gRPC: Started sample storage task: {task_id}")
            return status_to_response(status)

        except Exception as e:
            logger.error(f"gRPC: Error storing sample: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return StatusResponse(task_id="", status="Error", file_ref="", error_message=str(e), result_type="other")

    def AddFuzzyJoin(self, request, context):
        """Execute a fuzzy join operation."""
        try:
            flow_id = request.flow_id or 1
            node_id = request.node_id or "-1"

            try:
                node_id = int(node_id)
            except ValueError:
                pass

            logger.info("gRPC: Starting fuzzy join operation")

            default_cache_dir = create_and_get_default_cache_dir(flow_id)
            task_id = request.task_id or str(uuid.uuid4())
            cache_dir = request.cache_dir if request.cache_dir else default_cache_dir

            left_serializable_object = request.left_df_operation.operation
            right_serializable_object = request.right_df_operation.operation

            # Convert gRPC FuzzyMapping to models.FuzzyMapping
            from pl_fuzzy_frame_match import FuzzyMapping

            fuzzy_maps = []
            for fm in request.fuzzy_maps:
                fuzzy_maps.append(
                    FuzzyMapping(
                        left_col=fm.left_col,
                        right_col=fm.right_col,
                        threshold=fm.threshold,
                        algorithm=fm.algorithm,
                        limit=fm.limit,
                    )
                )

            file_path = os.path.join(cache_dir, f"{task_id}.arrow")
            status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars")
            status_dict[task_id] = status

            import threading

            thread = threading.Thread(
                target=start_fuzzy_process,
                kwargs={
                    "left_serializable_object": left_serializable_object,
                    "right_serializable_object": right_serializable_object,
                    "file_ref": file_path,
                    "fuzzy_maps": fuzzy_maps,
                    "task_id": task_id,
                    "flowfile_flow_id": flow_id,
                    "flowfile_node_id": node_id,
                },
                daemon=True,
            )
            thread.start()

            logger.info(f"gRPC: Started fuzzy join task: {task_id}")
            return status_to_response(status)

        except Exception as e:
            logger.error(f"gRPC: Error in fuzzy join: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return StatusResponse(task_id="", status="Error", file_ref="", error_message=str(e), result_type="other")

    def CreateTable(self, request, context):
        """Create a table from received data."""
        try:
            flow_id = request.flow_id or 1
            node_id = request.node_id or "-1"

            try:
                node_id = int(node_id)
            except ValueError:
                pass

            file_type = FileType(request.file_type)
            logger.info(f"gRPC: Creating table of type: {file_type}")

            # Decode the received table from JSON
            received_table = ReceivedTable.model_validate_json(request.table_data)

            task_id = str(uuid.uuid4())
            file_ref = os.path.join(create_and_get_default_cache_dir(flow_id), f"{task_id}.arrow")
            status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_ref, result_type="polars")
            status_dict[task_id] = status

            func_ref = table_creator_factory_method(file_type)

            import threading

            thread = threading.Thread(
                target=start_generic_process,
                kwargs={
                    "func_ref": func_ref,
                    "file_ref": file_ref,
                    "task_id": task_id,
                    "kwargs": {"received_table": received_table},
                    "flowfile_flow_id": flow_id,
                    "flowfile_node_id": node_id,
                },
                daemon=True,
            )
            thread.start()

            logger.info(f"gRPC: Started table creation task: {task_id}")
            return status_to_response(status)

        except Exception as e:
            logger.error(f"gRPC: Error creating table: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return StatusResponse(task_id="", status="Error", file_ref="", error_message=str(e), result_type="other")

    def StoreDatabaseReadResult(self, request, context):
        """Read from database and store result."""
        try:
            flow_id = request.flow_id or 1
            node_id = request.node_id or "-1"

            try:
                node_id = int(node_id)
            except ValueError:
                pass

            logger.info("gRPC: Processing database read operation")

            # Convert gRPC DatabaseReadRequest to DatabaseReadSettings
            from flowfile_worker.external_sources.sql_source.models import DatabaseConnection

            connection = DatabaseConnection(
                driver=request.connection.driver,
                host=request.connection.host,
                port=request.connection.port,
                username=request.connection.username,
                password=request.connection.password,
                database=request.connection.database,
                schema=request.connection.schema if request.connection.schema else None,
            )
            database_read_settings = DatabaseReadSettings(
                connection=connection,
                query=request.query,
                flowfile_flow_id=flow_id,
                flowfile_node_id=node_id,
            )

            task_id = str(uuid.uuid4())
            file_path = os.path.join(create_and_get_default_cache_dir(flow_id), f"{task_id}.arrow")
            status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars")
            status_dict[task_id] = status

            import threading

            thread = threading.Thread(
                target=start_generic_process,
                kwargs={
                    "func_ref": read_sql_source,
                    "file_ref": file_path,
                    "flowfile_flow_id": flow_id,
                    "flowfile_node_id": node_id,
                    "task_id": task_id,
                    "kwargs": {"database_read_settings": database_read_settings},
                },
                daemon=True,
            )
            thread.start()

            logger.info(f"gRPC: Started database read task: {task_id}")
            return status_to_response(status)

        except Exception as e:
            logger.error(f"gRPC: Error processing database read: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return StatusResponse(task_id="", status="Error", file_ref="", error_message=str(e), result_type="other")

    def StoreDatabaseWriteResult(self, request, context):
        """Write dataframe to database."""
        try:
            flow_id = request.flow_id or 1
            node_id = request.node_id or "-1"

            try:
                node_id = int(node_id)
            except ValueError:
                pass

            logger.info("gRPC: Starting write operation to database")

            from flowfile_worker.external_sources.sql_source.models import DatabaseConnection, DatabaseWriteSettings

            connection = DatabaseConnection(
                driver=request.connection.driver,
                host=request.connection.host,
                port=request.connection.port,
                username=request.connection.username,
                password=request.connection.password,
                database=request.connection.database,
                schema=request.connection.schema if request.connection.schema else None,
            )
            database_write_settings = DatabaseWriteSettings(
                connection=connection,
                table_name=request.table_name,
                if_exists=request.if_exists,
                flowfile_flow_id=flow_id,
                flowfile_node_id=node_id,
            )

            task_id = str(uuid.uuid4())
            status = models.Status(background_task_id=task_id, status="Starting", file_ref="", result_type="other")
            status_dict[task_id] = status

            import threading

            thread = threading.Thread(
                target=start_process,
                kwargs={
                    "polars_serializable_object": request.polars_data,
                    "task_id": task_id,
                    "operation": "write_to_database",
                    "file_ref": "",
                    "flowfile_flow_id": flow_id,
                    "flowfile_node_id": node_id,
                    "kwargs": {"database_write_settings": database_write_settings},
                },
                daemon=True,
            )
            thread.start()

            logger.info(f"gRPC: Started database write task: {task_id}")
            return status_to_response(status)

        except Exception as e:
            logger.error(f"gRPC: Error in database write operation: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return StatusResponse(task_id="", status="Error", file_ref="", error_message=str(e), result_type="other")

    def WriteDataToCloud(self, request, context):
        """Write dataframe to cloud storage."""
        try:
            flow_id = request.flow_id or 1
            node_id = request.node_id or "-1"

            try:
                node_id = int(node_id)
            except ValueError:
                pass

            logger.info("gRPC: Starting write operation to cloud storage")

            from flowfile_worker.external_sources.s3_source.models import (
                CloudStorageConnection,
                CloudStorageWriteSettings,
                OutputFileSettings,
            )

            connection = CloudStorageConnection(
                provider=request.connection.provider,
                bucket=request.connection.bucket,
                access_key=request.connection.access_key,
                secret_key=request.connection.secret_key,
                region=request.connection.region if request.connection.region else None,
                endpoint_url=request.connection.endpoint_url if request.connection.endpoint_url else None,
            )
            write_settings = OutputFileSettings(
                path=request.write_settings.path,
                file_type=request.write_settings.file_type,
                write_mode=request.write_settings.write_mode,
            )
            cloud_storage_write_settings = CloudStorageWriteSettings(
                connection=connection,
                write_settings=write_settings,
                flowfile_flow_id=flow_id,
                flowfile_node_id=node_id,
            )

            task_id = str(uuid.uuid4())
            status = models.Status(background_task_id=task_id, status="Starting", file_ref="", result_type="other")
            status_dict[task_id] = status

            import threading

            thread = threading.Thread(
                target=start_process,
                kwargs={
                    "polars_serializable_object": request.polars_data,
                    "task_id": task_id,
                    "operation": "write_to_cloud_storage",
                    "file_ref": "",
                    "flowfile_flow_id": flow_id,
                    "flowfile_node_id": node_id,
                    "kwargs": {"cloud_write_settings": cloud_storage_write_settings},
                },
                daemon=True,
            )
            thread.start()

            logger.info(f"gRPC: Started cloud storage write task: {task_id}")
            return status_to_response(status)

        except Exception as e:
            logger.error(f"gRPC: Error in cloud storage write: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return StatusResponse(task_id="", status="Error", file_ref="", error_message=str(e), result_type="other")

    def WriteResults(self, request, context):
        """Write dataframe to file."""
        try:
            flow_id = request.flow_id or 1
            node_id = request.node_id or "-1"

            try:
                node_id = int(node_id)
            except ValueError:
                pass

            logger.info(f"gRPC: Starting write operation to: {request.path}")

            task_id = str(uuid.uuid4())
            file_path = request.path
            status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="other")
            status_dict[task_id] = status

            kwargs = {
                "data_type": request.data_type,
                "path": request.path,
                "write_mode": request.write_mode,
            }
            if request.HasField("sheet_name"):
                kwargs["sheet_name"] = request.sheet_name
            if request.HasField("delimiter"):
                kwargs["delimiter"] = request.delimiter

            import threading

            thread = threading.Thread(
                target=start_process,
                kwargs={
                    "polars_serializable_object": request.polars_data,
                    "task_id": task_id,
                    "operation": "write_output",
                    "file_ref": file_path,
                    "flowfile_flow_id": flow_id,
                    "flowfile_node_id": node_id,
                    "kwargs": kwargs,
                },
                daemon=True,
            )
            thread.start()

            logger.info(f"gRPC: Started write task: {task_id}")
            return status_to_response(status)

        except Exception as e:
            logger.error(f"gRPC: Error in write operation: {str(e)}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return StatusResponse(task_id="", status="Error", file_ref="", error_message=str(e), result_type="other")

    def GetStatus(self, request, context):
        """Get task status (unary call)."""
        task_id = request.task_id
        logger.debug(f"gRPC: Getting status for task: {task_id}")

        status = status_dict.get(task_id)
        if status is None:
            logger.warning(f"gRPC: Task not found: {task_id}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Task not found")
            return StatusResponse(task_id=task_id, status="Error", file_ref="", error_message="Task not found", result_type="other")

        result_valid = validate_result(task_id)
        if result_valid is False:
            logger.error(f"gRPC: Invalid result for task: {task_id}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Task not found")
            return StatusResponse(task_id=task_id, status="Error", file_ref="", error_message="Invalid result", result_type="other")

        return status_to_response(status)

    def StreamStatus(self, request, context):
        """Stream task status updates (server streaming)."""
        task_id = request.task_id
        logger.debug(f"gRPC: Streaming status for task: {task_id}")

        while context.is_active():
            status = status_dict.get(task_id)
            if status is None:
                logger.warning(f"gRPC: Task not found: {task_id}")
                yield StatusResponse(task_id=task_id, status="Error", file_ref="", error_message="Task not found", result_type="other")
                return

            yield status_to_response(status)

            # Check if task is complete
            if status.status in ("Completed", "Error", "Unknown Error", "Cancelled"):
                return

            sleep(0.5)  # Poll interval

    def FetchResults(self, request, context):
        """Fetch completed task results."""
        task_id = request.task_id
        logger.debug(f"gRPC: Fetching results for task: {task_id}")

        status = status_dict.get(task_id)
        if not status:
            logger.warning(f"gRPC: Result not found: {task_id}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Result not found")
            return FetchResultsResponse(task_id=task_id, result=b"", ready=False)

        if status.status == "Processing":
            return FetchResultsResponse(task_id=task_id, result=b"", ready=False)

        if status.status == "Error":
            logger.error(f"gRPC: Task error: {status.error_message}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"An error occurred during processing: {status.error_message}")
            return FetchResultsResponse(task_id=task_id, result=b"", ready=False)

        try:
            lf = pl.scan_parquet(status.file_ref)
            return FetchResultsResponse(task_id=task_id, result=lf.serialize(), ready=True)
        except Exception as e:
            logger.error(f"gRPC: Error reading results: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Error reading results")
            return FetchResultsResponse(task_id=task_id, result=b"", ready=False)

    def GetMemoryUsage(self, request, context):
        """Get memory usage for a task."""
        task_id = request.task_id
        logger.debug(f"gRPC: Getting memory usage for task: {task_id}")

        memory_usage = PROCESS_MEMORY_USAGE.get(task_id)
        if memory_usage is None:
            logger.warning(f"gRPC: Memory usage not found: {task_id}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Memory usage data not found for this task ID")
            return MemoryUsageResponse(task_id=task_id, memory_usage={})

        return MemoryUsageResponse(task_id=task_id, memory_usage=memory_usage)

    def GetAllTaskIds(self, request, context):
        """Get all task IDs."""
        logger.debug("gRPC: Fetching all task IDs")
        task_ids = list(status_dict.keys())
        logger.debug(f"gRPC: Found {len(task_ids)} tasks")
        return TaskIdsResponse(task_ids=task_ids)

    def ClearTask(self, request, context):
        """Clear a task and its cached files."""
        task_id = request.task_id
        logger.info(f"gRPC: Clearing task: {task_id}")

        status = status_dict.get(task_id)
        if not status:
            logger.warning(f"gRPC: Task not found for clearing: {task_id}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Task not found")
            return MessageResponse(message=f"Task {task_id} not found.", success=False)

        try:
            if os.path.exists(status.file_ref):
                os.remove(status.file_ref)
                logger.debug(f"gRPC: Removed file: {status.file_ref}")
        except Exception as e:
            logger.error(f"gRPC: Error removing file {status.file_ref}: {str(e)}", exc_info=True)

        with status_dict_lock:
            status_dict.pop(task_id, None)
            PROCESS_MEMORY_USAGE.pop(task_id, None)
            logger.info(f"gRPC: Successfully cleared task: {task_id}")

        return MessageResponse(message=f"Task {task_id} has been cleared.", success=True)

    def CancelTask(self, request, context):
        """Cancel a running task."""
        task_id = request.task_id
        logger.info(f"gRPC: Attempting to cancel task: {task_id}")

        if not process_manager.cancel_process(task_id):
            logger.warning(f"gRPC: Cannot cancel task: {task_id}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Task not found or already completed")
            return MessageResponse(message=f"Task {task_id} not found or already completed.", success=False)

        with status_dict_lock:
            if task_id in status_dict:
                status_dict[task_id].status = "Cancelled"
                logger.info(f"gRPC: Successfully cancelled task: {task_id}")

        return MessageResponse(message=f"Task {task_id} has been cancelled.", success=True)

    def Shutdown(self, request, context):
        """Shutdown the worker gracefully."""
        logger.info("gRPC: Received shutdown request")
        # Signal shutdown to main process
        import signal
        import os

        os.kill(os.getpid(), signal.SIGTERM)
        return MessageResponse(message="Shutdown initiated", success=True)


def serve(host: str = "0.0.0.0", port: int = 50051, max_workers: int = 10) -> grpc.Server:
    """
    Start the gRPC server.

    Args:
        host: The host to bind to
        port: The port to bind to
        max_workers: Maximum number of thread pool workers

    Returns:
        The gRPC server instance
    """
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        options=[
            ("grpc.max_send_message_length", 512 * 1024 * 1024),  # 512MB
            ("grpc.max_receive_message_length", 512 * 1024 * 1024),  # 512MB
        ],
    )
    add_WorkerServiceServicer_to_server(WorkerServiceImpl(), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    logger.info(f"gRPC Worker service started on {host}:{port}")
    return server


if __name__ == "__main__":
    server = serve()
    server.wait_for_termination()
