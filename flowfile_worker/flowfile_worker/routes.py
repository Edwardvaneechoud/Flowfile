import asyncio
import gc
import json
import os
import uuid
from queue import Empty

import polars as pl
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, Response

from flowfile_worker import CACHE_DIR, PROCESS_MEMORY_USAGE, funcs, models, mp_context, status_dict, status_dict_lock
from flowfile_worker.configs import logger
from flowfile_worker.create import FileType, table_creator_factory_method
from flowfile_worker.create.models import ReceivedTable
from flowfile_worker.external_sources.google_analytics_source.main import read_google_analytics
from flowfile_worker.external_sources.google_analytics_source.models import GoogleAnalyticsReadSettings
from flowfile_worker.external_sources.kafka_source.main import read_kafka
from flowfile_worker.external_sources.rest_api_source.main import read_rest_api
from flowfile_worker.external_sources.rest_api_source.models import RestApiReadSettings
from flowfile_worker.external_sources.sql_source.main import read_sql_source
from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings
from flowfile_worker.spawner import (
    process_manager,
    start_apply_model_process,
    start_fuzzy_process,
    start_generic_process,
    start_process,
    start_train_model_process,
)
from shared.kafka.models import KafkaReadSettings
from shared.storage_config import storage

router = APIRouter()

# Re-use the single validation helper from funcs (backed by shared.delta_utils).
_validate_catalog_path = funcs._validate_catalog_path


def create_and_get_default_cache_dir(flowfile_flow_id: int) -> str:
    default_cache_dir = CACHE_DIR / str(flowfile_flow_id)
    default_cache_dir.mkdir(parents=True, exist_ok=True)
    return str(default_cache_dir)


@router.post("/submit_query/")
async def submit_query(request: Request, background_tasks: BackgroundTasks) -> models.Status:
    """Accept raw binary data with metadata in headers for efficient transfer."""
    try:
        polars_serializable_object = await request.body()

        task_id = request.headers.get("X-Task-Id") or str(uuid.uuid4())
        operation_type = request.headers.get("X-Operation-Type", "store")
        flow_id = int(request.headers.get("X-Flow-Id", "1"))
        node_id = request.headers.get("X-Node-Id", "-1")
        try:
            node_id = int(node_id)
        except ValueError:
            pass

        logger.info(f"Processing query with operation: {operation_type}")

        kwargs_str = request.headers.get("X-Kwargs")
        kwargs = json.loads(kwargs_str) if kwargs_str else {}

        default_cache_dir = create_and_get_default_cache_dir(flow_id)
        file_path = os.path.join(default_cache_dir, f"{task_id}.arrow")
        result_type = "polars" if operation_type == "store" else "other"

        status = models.Status(
            background_task_id=task_id, status="Starting", file_ref=file_path, result_type=result_type
        )
        status_dict[task_id] = status

        background_tasks.add_task(
            start_process,
            polars_serializable_object=polars_serializable_object,
            task_id=task_id,
            operation=operation_type,
            file_ref=file_path,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
            kwargs=kwargs,
        )
        logger.info(f"Started background task: {task_id}")
        return status

    except Exception as e:
        logger.error(f"Error processing query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/store_sample/")
async def store_sample(request: Request, background_tasks: BackgroundTasks) -> models.Status:
    """Accept raw binary data with metadata in headers for efficient transfer."""
    try:
        polars_serializable_object = await request.body()

        task_id = request.headers.get("X-Task-Id") or str(uuid.uuid4())
        sample_size = int(request.headers.get("X-Sample-Size", "100"))
        flow_id = int(request.headers.get("X-Flow-Id", "1"))
        node_id = request.headers.get("X-Node-Id", "-1")
        try:
            node_id = int(node_id)
        except ValueError:
            pass

        logger.info(f"Processing sample storage with size: {sample_size}")

        default_cache_dir = create_and_get_default_cache_dir(flow_id)
        file_path = os.path.join(default_cache_dir, f"{task_id}.arrow")

        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="other")
        status_dict[task_id] = status

        background_tasks.add_task(
            start_process,
            polars_serializable_object=polars_serializable_object,
            task_id=task_id,
            operation="store_sample",
            file_ref=file_path,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
            kwargs={"sample_size": sample_size},
        )
        logger.info(f"Started sample storage task: {task_id}")

        return status

    except Exception as e:
        logger.error(f"Error storing sample: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/write_data_to_cloud/")
def write_data_to_cloud(
    cloud_storage_script_write: models.CloudStorageScriptWrite, background_tasks: BackgroundTasks
) -> models.Status:
    """
    Write polars dataframe to a file in cloud storage.
    Args:
        cloud_storage_script_write (): Contains dataframe and write options for cloud storage
        background_tasks (): FastAPI background tasks handler

    Returns:
        models.Status: Status object tracking the write operation
    """
    try:
        logger.info("Starting write operation to: cloud storage")
        task_id = str(uuid.uuid4())
        polars_serializable_object = cloud_storage_script_write.polars_serializable_object()
        status = models.Status(background_task_id=task_id, status="Starting", file_ref="", result_type="other")
        status_dict[task_id] = status
        background_tasks.add_task(
            start_process,
            polars_serializable_object=polars_serializable_object,
            task_id=task_id,
            operation="write_to_cloud_storage",
            file_ref="",
            flowfile_flow_id=cloud_storage_script_write.flowfile_flow_id,
            flowfile_node_id=cloud_storage_script_write.flowfile_node_id,
            kwargs=dict(cloud_write_settings=cloud_storage_script_write.get_cloud_storage_write_settings()),
        )
        logger.info(f"Started write task: {task_id} to database")
        return status
    except Exception as e:
        logger.error(f"Error in write operation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/store_database_write_result/")
def store_in_database(
    database_script_write: models.DatabaseScriptWrite, background_tasks: BackgroundTasks
) -> models.Status:
    """
    Write polars dataframe to a file in specified format.

    Args:
        database_script_write (models.DatabaseScriptWrite): Contains dataframe and write options for database
        background_tasks (BackgroundTasks): FastAPI background tasks handler

    Returns:
        models.Status: Status object tracking the write operation
    """
    logger.info("Starting write operation to: database")
    try:
        task_id = str(uuid.uuid4())
        polars_serializable_object = database_script_write.polars_serializable_object()
        status = models.Status(background_task_id=task_id, status="Starting", file_ref="", result_type="other")
        status_dict[task_id] = status
        background_tasks.add_task(
            start_process,
            polars_serializable_object=polars_serializable_object,
            task_id=task_id,
            operation="write_to_database",
            file_ref="",
            flowfile_flow_id=database_script_write.flowfile_flow_id,
            flowfile_node_id=database_script_write.flowfile_node_id,
            kwargs=dict(database_write_settings=database_script_write.get_database_write_settings()),
        )

        logger.info(f"Started write task: {task_id} to database")

        return status

    except Exception as e:
        logger.error(f"Error in write operation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/write_results/")
def write_results(polars_script_write: models.PolarsScriptWrite, background_tasks: BackgroundTasks) -> models.Status:
    """
    Write polars dataframe to a file in specified format.

    Args:
        polars_script_write (models.PolarsScriptWrite): Contains dataframe and write options
        background_tasks (BackgroundTasks): FastAPI background tasks handler

    Returns:
        models.Status: Status object tracking the write operation
    """
    logger.info(f"Starting write operation to: {polars_script_write.path}")
    try:
        task_id = str(uuid.uuid4())
        file_path = polars_script_write.path
        polars_serializable_object = polars_script_write.polars_serializable_object()
        result_type = "other"
        status = models.Status(
            background_task_id=task_id, status="Starting", file_ref=file_path, result_type=result_type
        )
        status_dict[task_id] = status
        background_tasks.add_task(
            start_process,
            polars_serializable_object=polars_serializable_object,
            task_id=task_id,
            operation="write_output",
            file_ref=file_path,
            flowfile_flow_id=polars_script_write.flowfile_flow_id,
            flowfile_node_id=polars_script_write.flowfile_node_id,
            kwargs=dict(
                data_type=polars_script_write.data_type,
                path=polars_script_write.path,
                write_mode=polars_script_write.write_mode,
                sheet_name=polars_script_write.sheet_name,
                delimiter=polars_script_write.delimiter,
                compression=polars_script_write.compression,
            ),
        )
        logger.info(f"Started write task: {task_id} with type: {polars_script_write.data_type}")

        return status

    except Exception as e:
        logger.error(f"Error in write operation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/store_database_read_result")
def store_sql_db_result(
    database_read_settings: DatabaseReadSettings, background_tasks: BackgroundTasks
) -> models.Status:
    """
    Store the result of an sql source operation.

    Args:
        database_read_settings (SQLSourceSettings): Settings for the SQL source operation
        background_tasks (BackgroundTasks): FastAPI background tasks handler

    Returns:
        models.Status: Status object tracking the Sql operation
    """
    logger.info("Processing Sql source operation")

    try:
        task_id = str(uuid.uuid4())
        file_path = os.path.join(
            create_and_get_default_cache_dir(database_read_settings.flowfile_flow_id), f"{task_id}.arrow"
        )
        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars")
        status_dict[task_id] = status
        logger.info(f"Starting reading from database source task: {task_id}")
        background_tasks.add_task(
            start_generic_process,
            func_ref=read_sql_source,
            file_ref=file_path,
            flowfile_flow_id=database_read_settings.flowfile_flow_id,
            flowfile_node_id=database_read_settings.flowfile_node_id,
            task_id=task_id,
            kwargs=dict(database_read_settings=database_read_settings),
        )
        return status

    except Exception as e:
        logger.error(f"Error processing sql source: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/store_kafka_read_result")
def store_kafka_result(kafka_read_settings: KafkaReadSettings, background_tasks: BackgroundTasks) -> models.Status:
    """Consume messages from a Kafka topic and store the result as an IPC file.

    Follows the same pattern as store_database_read_result.
    """
    logger.info("Processing Kafka source operation for topic: %s", kafka_read_settings.topic)

    try:
        task_id = str(uuid.uuid4())
        file_path = os.path.join(
            create_and_get_default_cache_dir(kafka_read_settings.flowfile_flow_id), f"{task_id}.arrow"
        )
        sidecar_path = file_path + ".offsets.json"
        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars")
        status_dict[task_id] = status
        logger.info("Starting Kafka read task: %s", task_id)
        background_tasks.add_task(
            start_generic_process,
            func_ref=read_kafka,
            file_ref=file_path,
            flowfile_flow_id=kafka_read_settings.flowfile_flow_id,
            flowfile_node_id=kafka_read_settings.flowfile_node_id,
            task_id=task_id,
            kwargs={
                "kafka_read_settings": kafka_read_settings,
                "sidecar_path": sidecar_path,
                "file_path": file_path,
            },
        )
        return status

    except Exception as e:
        logger.error("Error processing Kafka source: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/store_google_analytics_read_result")
def store_google_analytics_result(
    ga_read_settings: GoogleAnalyticsReadSettings, background_tasks: BackgroundTasks
) -> models.Status:
    """Fetch a GA4 report in the background and persist it as an Arrow IPC file.

    Follows the same offload pattern as ``store_database_read_result`` /
    ``store_kafka_read_result``: the network I/O, token refresh, and pagination
    run in a worker subprocess so the core's event loop never blocks on the
    Google API.
    """
    logger.info(
        "Processing Google Analytics source operation for property: %s",
        ga_read_settings.property_id,
    )
    try:
        task_id = str(uuid.uuid4())
        file_path = os.path.join(
            create_and_get_default_cache_dir(ga_read_settings.flowfile_flow_id), f"{task_id}.arrow"
        )
        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars")
        status_dict[task_id] = status
        logger.info("Starting Google Analytics read task: %s", task_id)
        background_tasks.add_task(
            start_generic_process,
            func_ref=read_google_analytics,
            file_ref=file_path,
            flowfile_flow_id=ga_read_settings.flowfile_flow_id,
            flowfile_node_id=ga_read_settings.flowfile_node_id,
            task_id=task_id,
            kwargs={"ga_read_settings": ga_read_settings},
        )
        return status

    except Exception as e:
        logger.error("Error processing Google Analytics source: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/store_rest_api_read_result")
def store_rest_api_result(
    rest_api_read_settings: RestApiReadSettings, background_tasks: BackgroundTasks
) -> models.Status:
    """Fetch from a REST API in the background and persist it as an Arrow IPC file.

    Follows the same offload pattern as ``store_google_analytics_read_result`` /
    ``store_database_read_result``: the HTTP round-trips, pagination, and retries
    run in a worker subprocess so the core's event loop never blocks on the API.
    """
    logger.info("Processing REST API source operation for url: %s", rest_api_read_settings.url)
    try:
        task_id = str(uuid.uuid4())
        file_path = os.path.join(
            create_and_get_default_cache_dir(rest_api_read_settings.flowfile_flow_id), f"{task_id}.arrow"
        )
        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type="polars")
        status_dict[task_id] = status
        logger.info("Starting REST API read task: %s", task_id)
        background_tasks.add_task(
            start_generic_process,
            func_ref=read_rest_api,
            file_ref=file_path,
            flowfile_flow_id=rest_api_read_settings.flowfile_flow_id,
            flowfile_node_id=rest_api_read_settings.flowfile_node_id,
            task_id=task_id,
            kwargs={"settings": rest_api_read_settings},
        )
        return status

    except Exception as e:
        logger.error("Error processing REST API source: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/kafka_offsets/{task_id}")
def get_kafka_offsets(task_id: str):
    """Return deferred Kafka offset data for a completed task.

    The worker writes a sidecar JSON file (``<file_ref>.offsets.json``)
    during Kafka consumption.  Core calls this endpoint after the task
    completes to retrieve the offsets for deferred commit.
    """
    status = status_dict.get(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")

    sidecar = status.file_ref + ".offsets.json"
    if os.path.exists(sidecar):
        import json

        with open(sidecar) as f:
            return json.loads(f.read())
    return None


@router.post("/create_table/{file_type}")
def create_table(
    file_type: FileType,
    received_table: ReceivedTable,
    background_tasks: BackgroundTasks,
    flowfile_flow_id: int = 1,
    flowfile_node_id: int | str = -1,
) -> models.Status:
    """
    Create a Polars table from received dictionary data based on specified file type.

    Args:
        file_type (FileType): Type of file/format for table creation
        received_table (Dict): Raw table data as dictionary
        background_tasks (BackgroundTasks): FastAPI background tasks handler
        flowfile_flow_id: Flowfile ID
        flowfile_node_id: Node ID

    Returns:
        models.Status: Status object tracking the table creation
    """
    logger.info(f"Creating table of type: {file_type}")
    try:
        task_id = str(uuid.uuid4())
        file_ref = os.path.join(create_and_get_default_cache_dir(flowfile_flow_id), f"{task_id}.arrow")
        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_ref, result_type="polars")
        status_dict[task_id] = status
        func_ref = table_creator_factory_method(file_type)
        background_tasks.add_task(
            start_generic_process,
            func_ref=func_ref,
            file_ref=file_ref,
            task_id=task_id,
            kwargs={"received_table": received_table},
            flowfile_flow_id=flowfile_flow_id,
            flowfile_node_id=flowfile_node_id,
        )
        logger.info(f"Started table creation task: {task_id}")

        return status

    except Exception as e:
        logger.error(f"Error creating table: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/flow/resolve_virtual_table", response_model=models.ResolveVirtualTableResponse)
def resolve_virtual_table(payload: models.ResolveVirtualTableRequest) -> models.ResolveVirtualTableResponse:
    """Materialise a flow-virtual table from a serialised polars plan.

    Idempotent on ``(table_id, source_versions_hash)`` — repeated calls return
    the same IPC file without re-executing the producer plan.
    """
    try:
        return funcs.resolve_virtual_table(payload)
    except Exception as e:
        logger.error(f"Error in resolve_virtual_table: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/catalog/sql_query", response_model=models.SqlQueryResponse)
def catalog_sql_query(payload: models.SqlQueryRequest) -> models.SqlQueryResponse:
    """Execute a SQL query against catalog tables (physical + virtual)."""
    try:
        result = funcs.execute_sql_query(
            payload.query,
            payload.tables,
            payload.max_rows,
            virtual_refs=payload.virtual_refs,
        )
        return models.SqlQueryResponse(**result)
    except ValueError as e:
        return models.SqlQueryResponse(error=str(e))
    except Exception as e:
        logger.error(f"Error executing SQL query: {str(e)}", exc_info=True)
        return models.SqlQueryResponse(error=str(e))


@router.post("/catalog/materialize", response_model=models.CatalogMaterializeResponse)
def materialize_catalog_table(payload: models.CatalogMaterializeRequest) -> models.CatalogMaterializeResponse:
    source_path = os.path.abspath(payload.source_file_path)
    ext = os.path.splitext(source_path)[1].lower()

    if ext not in (".csv", ".txt", ".tsv", ".parquet", ".xlsx", ".xls"):
        raise HTTPException(
            status_code=422,
            detail={"error_type": "unsupported_file_type", "message": f"Unsupported file type: {ext}"},
        )

    dest_dir = storage.catalog_tables_directory
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Generate a directory name for the Delta table (not a .parquet filename)
    if payload.table_name:
        dir_name = f"{payload.table_name}_{uuid.uuid4().hex[:8]}"
    else:
        dir_name = f"catalog_{uuid.uuid4().hex}"

    dest_path = str(dest_dir / dir_name)

    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    queue = mp_context.Queue(maxsize=1)

    p = mp_context.Process(
        target=funcs.materialize_catalog_table_task,
        kwargs={
            "source_file_path": source_path,
            "dest_path": dest_path,
            "progress": progress,
            "error_message": error_message,
            "queue": queue,
        },
    )
    p.start()
    p.join()

    try:
        with progress.get_lock():
            final_progress = progress.value

        if final_progress != 100:
            with error_message.get_lock():
                err = error_message.value.decode().rstrip("\x00")
            logger.error(f"Catalog materialize subprocess failed: {err}")
            raise HTTPException(
                status_code=500,
                detail={"error_type": "materialize_failure", "message": err},
            )

        result = queue.get(timeout=5)
        column_schema = [models.ColumnSchema(name=s["name"], dtype=s["dtype"]) for s in result["schema"]]
        return models.CatalogMaterializeResponse(
            table_path=result["table_path"],
            column_schema=column_schema,
            row_count=result["row_count"],
            column_count=result["column_count"],
            size_bytes=result["size_bytes"],
        )
    finally:
        del p, progress, error_message, queue
        gc.collect()


def _drain_then_join(p, queue) -> dict | None:
    """Read the subprocess result *before* joining: a child cannot exit until its
    queued payload fits through the pipe (~64KB), so join-first deadlocks on large
    results (e.g. a vacuum's file list)."""
    result = None
    while True:
        try:
            result = queue.get(timeout=0.5)
            break
        except Empty:
            if not p.is_alive():
                try:
                    result = queue.get(timeout=1)
                except Empty:
                    pass
                break
    p.join()
    return result


@router.post("/catalog/optimize", response_model=models.CatalogOptimizeResponse)
def optimize_catalog_table(payload: models.CatalogOptimizeRequest) -> models.CatalogOptimizeResponse:
    """Compact (and optionally Z-order) a Delta catalog table in a spawned subprocess."""
    try:
        resolved_path = str(_validate_catalog_path(payload.table_path))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    queue = mp_context.Queue(maxsize=1)
    p = mp_context.Process(
        target=funcs.optimize_catalog_table_task,
        kwargs={
            "table_path": resolved_path,
            "z_order_columns": payload.z_order_columns,
            "progress": progress,
            "error_message": error_message,
            "queue": queue,
        },
    )
    p.start()
    result = _drain_then_join(p, queue)
    try:
        with progress.get_lock():
            final_progress = progress.value
        if final_progress != 100 or result is None:
            with error_message.get_lock():
                err = error_message.value.decode().rstrip("\x00")
            logger.error(f"Catalog optimize subprocess failed: {err}")
            raise HTTPException(
                status_code=500,
                detail={"error_type": "optimize_failure", "message": err or "subprocess returned no result"},
            )
        return models.CatalogOptimizeResponse(
            metrics=result.get("metrics", {}), size_bytes=result.get("size_bytes")
        )
    finally:
        del p, progress, error_message, queue
        gc.collect()


@router.post("/catalog/vacuum", response_model=models.CatalogVacuumResponse)
def vacuum_catalog_table(payload: models.CatalogVacuumRequest) -> models.CatalogVacuumResponse:
    """Vacuum tombstoned files from a Delta catalog table in a spawned subprocess."""
    try:
        resolved_path = str(_validate_catalog_path(payload.table_path))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    queue = mp_context.Queue(maxsize=1)
    p = mp_context.Process(
        target=funcs.vacuum_catalog_table_task,
        kwargs={
            "table_path": resolved_path,
            "retention_hours": payload.retention_hours,
            "dry_run": payload.dry_run,
            "progress": progress,
            "error_message": error_message,
            "queue": queue,
        },
    )
    p.start()
    result = _drain_then_join(p, queue)
    try:
        with progress.get_lock():
            final_progress = progress.value
        if final_progress != 100 or result is None:
            with error_message.get_lock():
                err = error_message.value.decode().rstrip("\x00")
            logger.error(f"Catalog vacuum subprocess failed: {err}")
            raise HTTPException(
                status_code=500,
                detail={"error_type": "vacuum_failure", "message": err or "subprocess returned no result"},
            )
        return models.CatalogVacuumResponse(
            files_removed=result.get("files_removed", []),
            file_count=result.get("file_count", 0),
            size_bytes=result.get("size_bytes"),
        )
    finally:
        del p, progress, error_message, queue
        gc.collect()


@router.post("/catalog/table_metadata", response_model=models.TableMetadataResponse)
def read_table_metadata(payload: models.TableMetadataRequest) -> models.TableMetadataResponse:
    """Read schema, row_count, column_count, size_bytes from a table path.

    This offloads metadata reading from the core process to the worker.
    """
    try:
        _validate_catalog_path(payload.table_path)
        result = funcs.read_table_metadata(payload.table_path)
        column_schema = [models.ColumnSchema(name=s["name"], dtype=s["dtype"]) for s in result["schema"]]
        return models.TableMetadataResponse(
            column_schema=column_schema,
            row_count=result["row_count"],
            column_count=result["column_count"],
            size_bytes=result["size_bytes"],
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error reading table metadata: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/catalog/delta_history", response_model=models.DeltaHistoryResponse)
def get_delta_history(payload: models.DeltaHistoryRequest) -> models.DeltaHistoryResponse:
    """Read version history from a Delta table."""
    try:
        _validate_catalog_path(payload.table_path)
        return funcs.get_delta_history(payload.table_path, payload.limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error reading delta history: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/catalog/delta_version_preview", response_model=models.DeltaVersionPreviewResponse)
def get_delta_version_preview(payload: models.DeltaVersionPreviewRequest) -> models.DeltaVersionPreviewResponse:
    """Preview data from a Delta table at a specific version."""
    try:
        _validate_catalog_path(payload.table_path)
        return funcs.read_delta_version_preview(payload.table_path, payload.version, payload.n_rows)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error reading delta version preview: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/catalog/visualize_query", response_model=models.VisualizeQueryResponse)
async def catalog_visualize_query(payload: models.VisualizeQueryRequest) -> models.VisualizeQueryResponse:
    """Run a Graphic Walker workflow against a cached source LazyFrame."""
    from flowfile_worker.viz_sessions import viz_session_registry

    loop = asyncio.get_running_loop()
    try:
        result, _ = await loop.run_in_executor(
            None,
            viz_session_registry.execute,
            payload.source,
            "execute",
            payload.payload,
            payload.max_rows,
        )
        return models.VisualizeQueryResponse(**result)
    except HTTPException:
        raise
    except ValueError as e:
        return models.VisualizeQueryResponse(error=str(e))
    except Exception as e:
        logger.error(f"Error in visualize_query: {str(e)}", exc_info=True)
        return models.VisualizeQueryResponse(error=str(e))


@router.post("/catalog/visualize_fields", response_model=models.VisualizeFieldsResponse)
async def catalog_visualize_fields(payload: models.VisualizeFieldsRequest) -> models.VisualizeFieldsResponse:
    """Return the Graphic Walker field schema for a cached source LazyFrame."""
    from flowfile_worker.viz_sessions import viz_session_registry

    loop = asyncio.get_running_loop()
    try:
        result, cache_hit = await loop.run_in_executor(
            None,
            viz_session_registry.execute,
            payload.source,
            "fields",
            None,
            None,
        )
        return models.VisualizeFieldsResponse(fields=result["fields"], cache_hit=cache_hit)
    except HTTPException:
        raise
    except ValueError as e:
        return models.VisualizeFieldsResponse(error=str(e))
    except Exception as e:
        logger.error(f"Error in visualize_fields: {str(e)}", exc_info=True)
        return models.VisualizeFieldsResponse(error=str(e))


@router.post("/catalog/visualize_column_stats", response_model=models.VisualizeColumnStatsResponse)
async def catalog_visualize_column_stats(
    payload: models.VisualizeColumnStatsRequest,
) -> models.VisualizeColumnStatsResponse:
    """Distinct values + min/max for a single column on a cached source LazyFrame."""
    from flowfile_worker.viz_sessions import viz_session_registry

    loop = asyncio.get_running_loop()
    try:
        result, cache_hit = await loop.run_in_executor(
            None,
            viz_session_registry.execute,
            payload.source,
            "column_stats",
            {"column": payload.column, "limit": payload.limit},
            None,
        )
        return models.VisualizeColumnStatsResponse(**result, cache_hit=cache_hit)
    except HTTPException:
        raise
    except ValueError as e:
        return models.VisualizeColumnStatsResponse(error=str(e))
    except Exception as e:
        logger.error(f"Error in visualize_column_stats: {str(e)}", exc_info=True)
        return models.VisualizeColumnStatsResponse(error=str(e))


@router.post("/catalog/visualize_evict")
def catalog_visualize_evict(session_key: str):
    """Drop a cached viz session (called by core after a table update/delete)."""
    from flowfile_worker.viz_sessions import viz_session_registry

    viz_session_registry.evict(session_key)
    return {"ok": True, "session_key": session_key}


@router.get("/catalog/visualize_stats")
def catalog_visualize_stats() -> list[dict]:
    """Return per-child viz-session statistics (debug/observability)."""
    from flowfile_worker.viz_sessions import viz_session_registry

    return viz_session_registry.stats()


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
    if status.status == "Completed" and status.result_type == "polars":
        try:
            pl.scan_ipc(status.file_ref)
            logger.debug(f"Validation successful for task: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Validation failed for task {task_id}: {str(e)}")
            return False
    return True


@router.get("/status/{task_id}", response_model=models.Status)
def get_status(task_id: str) -> models.Status:
    """Get status of a task by ID and validate its result if completed.

    Args:
        task_id: Unique identifier of the task

    Returns:
        models.Status: Current status of the task

    Raises:
        HTTPException: If task not found or invalid result
    """
    logger.debug(f"Getting status for task: {task_id}")
    status = status_dict.get(task_id)
    if status is None:
        logger.warning(f"Task not found: {task_id}")
        raise HTTPException(status_code=404, detail="Task not found")
    result_valid = validate_result(task_id)
    if not result_valid:
        logger.error(f"Invalid result for task: {task_id}")
        raise HTTPException(status_code=404, detail="Task not found")
    return status


@router.get("/fetch_results/{task_id}")
async def fetch_results(task_id: str):
    """Fetch results for a completed task.

    Args:
        task_id: Unique identifier of the task

    Returns:
        dict: Task ID and serialized result data

    Raises:
        HTTPException: If result not found or error occurred
    """
    logger.debug(f"Fetching results for task: {task_id}")
    status = status_dict.get(task_id)
    if not status:
        logger.warning(f"Result not found: {task_id}")
        raise HTTPException(status_code=404, detail="Result not found")
    if status.status == "Processing":
        return Response(status_code=202, content="Result not ready yet")
    if status.status == "Error":
        logger.error(f"Task error: {status.error_message}")
        raise HTTPException(status_code=404, detail=f"An error occurred during processing: {status.error_message}")
    try:
        lf = pl.scan_parquet(status.file_ref)
        return {"task_id": task_id, "result": lf.serialize()}
    except Exception as e:
        logger.error(f"Error reading results: {str(e)}")
        raise HTTPException(status_code=500, detail="Error reading results") from e


@router.get("/memory_usage/{task_id}")
async def memory_usage(task_id: str):
    """Get memory usage for a specific task.

    Args:
        task_id: Unique identifier of the task

    Returns:
        dict: Task ID and memory usage data

    Raises:
        HTTPException: If memory usage data not found
    """
    logger.debug(f"Getting memory usage for task: {task_id}")
    memory_usage = PROCESS_MEMORY_USAGE.get(task_id)
    if memory_usage is None:
        logger.warning(f"Memory usage not found: {task_id}")
        raise HTTPException(status_code=404, detail="Memory usage data not found for this task ID")
    return {"task_id": task_id, "memory_usage": memory_usage}


@router.post("/train_ml_model")
async def train_ml_model(
    polars_script: models.TrainModelInput, background_tasks: BackgroundTasks
) -> models.Status:
    """Fit a regression model and write its serialised bytes to ``staging_path``.

    Core has already called ``ArtifactService.prepare_upload`` and reserved the
    staging path, so the worker only needs to write the file there. The
    ``Status.results`` field carries ``{sha256, size_bytes, model_type}`` once
    training completes; core then finalises the artifact upload.
    """
    logger.info("Starting train_ml_model task: model_type=%s", polars_script.model_type)
    try:
        default_cache_dir = create_and_get_default_cache_dir(polars_script.flowfile_flow_id)
        polars_script.task_id = polars_script.task_id or str(uuid.uuid4())
        polars_script.cache_dir = polars_script.cache_dir or default_cache_dir
        polars_serializable_object = polars_script.df_operation.polars_serializable_object()

        status = models.Status(
            background_task_id=polars_script.task_id,
            status="Starting",
            file_ref=polars_script.staging_path,
            result_type="other",
        )
        status_dict[polars_script.task_id] = status
        background_tasks.add_task(
            start_train_model_process,
            polars_serializable_object=polars_serializable_object,
            task_id=polars_script.task_id,
            file_ref=polars_script.staging_path,
            model_type=polars_script.model_type,
            target_column=polars_script.target_column,
            feature_columns=polars_script.feature_columns,
            params=polars_script.params,
            staging_path=polars_script.staging_path,
            flowfile_flow_id=polars_script.flowfile_flow_id,
            flowfile_node_id=polars_script.flowfile_node_id,
        )
        logger.info(f"Started train_ml_model task: {polars_script.task_id}")
        return status
    except Exception as e:
        logger.error(f"Error starting train_ml_model: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/apply_ml_model")
async def apply_ml_model(
    polars_script: models.ApplyModelInput, background_tasks: BackgroundTasks
) -> models.Status:
    """Score input data using a previously trained model artifact."""
    logger.info("Starting apply_ml_model task: model_path=%s", polars_script.model_path)
    try:
        default_cache_dir = create_and_get_default_cache_dir(polars_script.flowfile_flow_id)
        polars_script.task_id = polars_script.task_id or str(uuid.uuid4())
        polars_script.cache_dir = polars_script.cache_dir or default_cache_dir
        polars_serializable_object = polars_script.df_operation.polars_serializable_object()

        file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
        status = models.Status(
            background_task_id=polars_script.task_id,
            status="Starting",
            file_ref=file_path,
            result_type="polars",
        )
        status_dict[polars_script.task_id] = status
        background_tasks.add_task(
            start_apply_model_process,
            polars_serializable_object=polars_serializable_object,
            task_id=polars_script.task_id,
            file_ref=file_path,
            model_path=polars_script.model_path,
            output_column=polars_script.output_column,
            flowfile_flow_id=polars_script.flowfile_flow_id,
            flowfile_node_id=polars_script.flowfile_node_id,
        )
        logger.info(f"Started apply_ml_model task: {polars_script.task_id}")
        return status
    except Exception as e:
        logger.error(f"Error starting apply_ml_model: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/add_fuzzy_join")
async def add_fuzzy_join(polars_script: models.FuzzyJoinInput, background_tasks: BackgroundTasks) -> models.Status:
    """Start a fuzzy join operation between two dataframes.

    Args:
        polars_script: Input containing left and right dataframes and fuzzy mapping config
        background_tasks: FastAPI background tasks handler

    Returns:
        models.Status: Status object for the fuzzy join task

    Raises:
        HTTPException: If error occurs during setup
    """
    logger.info("Starting fuzzy join operation")
    try:
        default_cache_dir = create_and_get_default_cache_dir(polars_script.flowfile_flow_id)
        polars_script.task_id = str(uuid.uuid4()) if polars_script.task_id is None else polars_script.task_id
        polars_script.cache_dir = polars_script.cache_dir if polars_script.cache_dir is not None else default_cache_dir
        left_serializable_object = polars_script.left_df_operation.polars_serializable_object()
        right_serializable_object = polars_script.right_df_operation.polars_serializable_object()

        file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
        status = models.Status(
            background_task_id=polars_script.task_id, status="Starting", file_ref=file_path, result_type="polars"
        )
        status_dict[polars_script.task_id] = status
        background_tasks.add_task(
            start_fuzzy_process,
            left_serializable_object=left_serializable_object,
            right_serializable_object=right_serializable_object,
            file_ref=file_path,
            fuzzy_maps=polars_script.fuzzy_maps,
            task_id=polars_script.task_id,
            flowfile_flow_id=polars_script.flowfile_flow_id,
            flowfile_node_id=polars_script.flowfile_node_id,
        )
        logger.info(f"Started fuzzy join task: {polars_script.task_id}")
        return status
    except Exception as e:
        logger.error(f"Error in fuzzy join: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/clear_task/{task_id}")
def clear_task(task_id: str):
    """
    Clear task data and status by ID.

    Args:
        task_id: Unique identifier of the task to clear
    Returns:
        dict: Success message
    Raises:
        HTTPException: If task not found
    """

    logger.info(f"Clearing task: {task_id}")
    status = status_dict.get(task_id)
    if not status:
        logger.warning(f"Task not found for clearing: {task_id}")
        raise HTTPException(status_code=404, detail="Task not found")
    try:
        if os.path.exists(status.file_ref):
            os.remove(status.file_ref)
            logger.debug(f"Removed file: {status.file_ref}")
        sidecar = status.file_ref + ".offsets.json"
        if os.path.exists(sidecar):
            os.remove(sidecar)
            logger.debug(f"Removed sidecar: {sidecar}")
    except Exception as e:
        logger.error(f"Error removing file {status.file_ref}: {str(e)}", exc_info=True)
    with status_dict_lock:
        status_dict.pop(task_id, None)
        PROCESS_MEMORY_USAGE.pop(task_id, None)
        logger.info(f"Successfully cleared task: {task_id}")
    return {"message": f"Task {task_id} has been cleared."}


@router.post("/cancel_task/{task_id}")
def cancel_task(task_id: str):
    """Cancel a running task by ID.

    Args:
        task_id: Unique identifier of the task to cancel

    Returns:
        dict: Success message

    Raises:
        HTTPException: If task cannot be cancelled
    """
    logger.info(f"Attempting to cancel task: {task_id}")
    if not process_manager.cancel_process(task_id):
        logger.warning(f"Cannot cancel task: {task_id}")
        raise HTTPException(status_code=404, detail="Task not found or already completed")
    with status_dict_lock:
        if task_id in status_dict:
            status_dict[task_id].status = "Cancelled"
            logger.info(f"Successfully cancelled task: {task_id}")
    return {"message": f"Task {task_id} has been cancelled."}


@router.get("/ids")
async def get_all_ids():
    """Get list of all task IDs in the system.

    Returns:
        list: List of all task IDs currently tracked
    """
    logger.debug("Fetching all task IDs")
    ids = [k for k in status_dict.keys()]
    logger.debug(f"Found {len(ids)} tasks")
    return ids
