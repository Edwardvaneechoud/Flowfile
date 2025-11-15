"""
API routes for Flowfile Worker.

This module defines all REST API endpoints for:
- Task submission and management
- Data reading/writing operations
- Status checking and result fetching
- Memory usage tracking
"""
import polars as pl
import uuid
import os
from fastapi import APIRouter, HTTPException, Response, BackgroundTasks
from typing import Dict, List, Union
from base64 import encodebytes

from flowfile_worker import CACHE_DIR, worker_state
from flowfile_worker import models
from flowfile_worker.spawner import start_process, start_fuzzy_process, start_generic_process, process_manager
from flowfile_worker.create import table_creator_factory_method, received_table_parser, FileType
from flowfile_worker.configs import logger
from flowfile_worker.error_handlers import (
    handle_route_errors_sync,
    handle_route_errors,
    TaskNotFoundError,
    InvalidResultError
)
from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings
from flowfile_worker.external_sources.sql_source.main import read_sql_source


router = APIRouter()


def create_and_get_default_cache_dir(flowfile_flow_id: int) -> str:
    """
    Create and get the default cache directory for a flow.
    
    Args:
        flowfile_flow_id: ID of the flow
        
    Returns:
        Path to the cache directory as string
    """
    default_cache_dir = CACHE_DIR / str(flowfile_flow_id)
    default_cache_dir.mkdir(parents=True, exist_ok=True)
    return str(default_cache_dir)


@router.post("/submit_query/")
@handle_route_errors_sync
def submit_query(polars_script: models.PolarsScript, background_tasks: BackgroundTasks) -> models.Status:
    """
    Submit a query for background processing.
    
    Args:
        polars_script: Script containing operation details and serialized data
        background_tasks: FastAPI background task handler
        
    Returns:
        Status object tracking the submitted task
        
    Raises:
        HTTPException: If task submission fails
    """
    logger.info(f"Processing query with operation: {polars_script.operation_type}")

    polars_script.task_id = str(uuid.uuid4()) if polars_script.task_id is None else polars_script.task_id
    default_cache_dir = create_and_get_default_cache_dir(polars_script.flowfile_flow_id)

    polars_script.cache_dir = polars_script.cache_dir if polars_script.cache_dir is not None else default_cache_dir
    polars_serializable_object = polars_script.polars_serializable_object()
    file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
    result_type = "polars" if polars_script.operation_type == "store" else "other"
    status = models.Status(
        background_task_id=polars_script.task_id,
        status="Starting",
        file_ref=file_path,
        result_type=result_type
    )
    
    worker_state.set_status(polars_script.task_id, status)
    
    background_tasks.add_task(
        start_process,
        polars_serializable_object=polars_serializable_object,
        task_id=polars_script.task_id,
        operation=polars_script.operation_type,
        file_ref=file_path,
        flowfile_flow_id=polars_script.flowfile_flow_id,
        flowfile_node_id=polars_script.flowfile_node_id,
        kwargs={}
    )
    logger.info(f"Started background task: {polars_script.task_id}")
    return status


@router.post('/store_sample/')
@handle_route_errors_sync
def store_sample(polars_script: models.PolarsScriptSample, background_tasks: BackgroundTasks) -> models.Status:
    """
    Store a sample of data for preview purposes.
    
    Args:
        polars_script: Script containing operation details, data, and sample size
        background_tasks: FastAPI background task handler
        
    Returns:
        Status object tracking the sample storage task
        
    Raises:
        HTTPException: If sample storage fails
    """
    logger.info(f"Processing sample storage with size: {polars_script.sample_size}")

    default_cache_dir = create_and_get_default_cache_dir(polars_script.flowfile_flow_id)
    polars_script.task_id = str(uuid.uuid4()) if polars_script.task_id is None else polars_script.task_id
    polars_script.cache_dir = polars_script.cache_dir if polars_script.cache_dir is not None else default_cache_dir
    polars_serializable_object = polars_script.polars_serializable_object()

    file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
    status = models.Status(
        background_task_id=polars_script.task_id,
        status="Starting",
        file_ref=file_path,
        result_type="other"
    )
    worker_state.set_status(polars_script.task_id, status)

    background_tasks.add_task(
        start_process,
        polars_serializable_object=polars_serializable_object,
        task_id=polars_script.task_id,
        operation=polars_script.operation_type,
        file_ref=file_path,
        flowfile_flow_id=polars_script.flowfile_flow_id,
        flowfile_node_id=polars_script.flowfile_node_id,
        kwargs={'sample_size': polars_script.sample_size}
    )
    logger.info(f"Started sample storage task: {polars_script.task_id}")

    return status


@router.post("/write_data_to_cloud/")
@handle_route_errors_sync
def write_data_to_cloud(
    cloud_storage_script_write: models.CloudStorageScriptWrite,
    background_tasks: BackgroundTasks
) -> models.Status:
    """
    Write polars dataframe to a file in cloud storage.
    
    Args:
        cloud_storage_script_write: Contains dataframe and write options for cloud storage
        background_tasks: FastAPI background tasks handler

    Returns:
        Status object tracking the write operation
        
    Raises:
        HTTPException: If write operation setup fails
    """
    logger.info("Starting write operation to cloud storage")
    
    task_id = str(uuid.uuid4())
    polars_serializable_object = cloud_storage_script_write.polars_serializable_object()
    status = models.Status(
        background_task_id=task_id,
        status="Starting",
        file_ref='',
        result_type="other"
    )
    worker_state.set_status(task_id, status)
    
    background_tasks.add_task(
        start_process,
        polars_serializable_object=polars_serializable_object,
        task_id=task_id,
        operation="write_to_cloud_storage",
        file_ref='',
        flowfile_flow_id=cloud_storage_script_write.flowfile_flow_id,
        flowfile_node_id=cloud_storage_script_write.flowfile_node_id,
        kwargs=dict(cloud_write_settings=cloud_storage_script_write.get_cloud_storage_write_settings()),
    )
    logger.info(f"Started write task: {task_id} to cloud storage")
    return status


@router.post('/store_database_write_result/')
@handle_route_errors_sync
def store_in_database(
    database_script_write: models.DatabaseScriptWrite,
    background_tasks: BackgroundTasks
) -> models.Status:
    """
    Write polars dataframe to a database.

    Args:
        database_script_write: Contains dataframe and write options for database
        background_tasks: FastAPI background tasks handler

    Returns:
        Status object tracking the write operation
        
    Raises:
        HTTPException: If write operation setup fails
    """
    logger.info("Starting write operation to database")
    
    task_id = str(uuid.uuid4())
    polars_serializable_object = database_script_write.polars_serializable_object()
    status = models.Status(
        background_task_id=task_id,
        status="Starting",
        file_ref='',
        result_type="other"
    )
    worker_state.set_status(task_id, status)
    
    background_tasks.add_task(
        start_process,
        polars_serializable_object=polars_serializable_object,
        task_id=task_id,
        operation="write_to_database",
        file_ref='',
        flowfile_flow_id=database_script_write.flowfile_flow_id,
        flowfile_node_id=database_script_write.flowfile_node_id,
        kwargs=dict(database_write_settings=database_script_write.get_database_write_settings()),
    )

    logger.info(f"Started write task: {task_id} to database")

    return status


@router.post('/write_results/')
@handle_route_errors_sync
def write_results(
    polars_script_write: models.PolarsScriptWrite,
    background_tasks: BackgroundTasks
) -> models.Status:
    """
    Write polars dataframe to a file in specified format.

    Args:
        polars_script_write: Contains dataframe and write options
        background_tasks: FastAPI background tasks handler

    Returns:
        Status object tracking the write operation
        
    Raises:
        HTTPException: If write operation setup fails
    """
    logger.info(f"Starting write operation to: {polars_script_write.path}")
    
    task_id = str(uuid.uuid4())
    file_path = polars_script_write.path
    polars_serializable_object = polars_script_write.polars_serializable_object()
    result_type = "other"
    status = models.Status(
        background_task_id=task_id,
        status="Starting",
        file_ref=file_path,
        result_type=result_type
    )
    worker_state.set_status(task_id, status)
    
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
            delimiter=polars_script_write.delimiter
        )
    )
    logger.info(f"Started write task: {task_id} with type: {polars_script_write.data_type}")

    return status


@router.post('/store_database_read_result')
@handle_route_errors_sync
def store_sql_db_result(
    database_read_settings: DatabaseReadSettings,
    background_tasks: BackgroundTasks
) -> models.Status:
    """
    Store the result of a SQL database read operation.

    Args:
        database_read_settings: Settings for the SQL source operation
        background_tasks: FastAPI background tasks handler

    Returns:
        Status object tracking the SQL operation
        
    Raises:
        HTTPException: If operation setup fails
    """
    logger.info("Processing SQL source operation")

    task_id = str(uuid.uuid4())
    file_path = os.path.join(
        create_and_get_default_cache_dir(database_read_settings.flowfile_flow_id),
        f"{task_id}.arrow"
    )
    status = models.Status(
        background_task_id=task_id,
        status="Starting",
        file_ref=file_path,
        result_type="polars"
    )
    worker_state.set_status(task_id, status)
    
    logger.info(f"Starting reading from database source task: {task_id}")
    background_tasks.add_task(
        start_generic_process,
        func_ref=read_sql_source,
        file_ref=file_path,
        flowfile_flow_id=database_read_settings.flowfile_flow_id,
        flowfile_node_id=database_read_settings.flowfile_node_id,
        task_id=task_id,
        kwargs=dict(database_read_settings=database_read_settings)
    )
    return status


@router.post('/create_table/{file_type}')
@handle_route_errors_sync
def create_table(
    file_type: FileType,
    received_table: Dict,
    background_tasks: BackgroundTasks,
    flowfile_flow_id: int = 1,
    flowfile_node_id: int | str = -1
) -> models.Status:
    """
    Create a Polars table from received dictionary data based on specified file type.

    Args:
        file_type: Type of file/format for table creation
        received_table: Raw table data as dictionary
        background_tasks: FastAPI background tasks handler
        flowfile_flow_id: Flowfile ID
        flowfile_node_id: Node ID

    Returns:
        Status object tracking the table creation
        
    Raises:
        HTTPException: If table creation setup fails
    """
    logger.info(f"Creating table of type: {file_type}")

    task_id = str(uuid.uuid4())
    file_ref = os.path.join(
        create_and_get_default_cache_dir(flowfile_flow_id),
        f"{task_id}.arrow"
    )

    status = models.Status(
        background_task_id=task_id,
        status="Starting",
        file_ref=file_ref,
        result_type="polars"
    )
    worker_state.set_status(task_id, status)
    
    func_ref = table_creator_factory_method(file_type)
    received_table_parsed = received_table_parser(received_table, file_type)
    background_tasks.add_task(
        start_generic_process,
        func_ref=func_ref,
        file_ref=file_ref,
        task_id=task_id,
        kwargs={'received_table': received_table_parsed},
        flowfile_flow_id=flowfile_flow_id,
        flowfile_node_id=flowfile_node_id
    )
    logger.info(f"Started table creation task: {task_id}")

    return status


def validate_result(task_id: str) -> bool:
    """
    Validate the result of a completed task by checking the IPC file.

    Args:
        task_id: ID of the task to validate

    Returns:
        True if valid, False if error or invalid
        
    Raises:
        TaskNotFoundError: If task not found
        InvalidResultError: If result validation fails
    """
    logger.debug(f"Validating result for task: {task_id}")
    status = worker_state.get_status(task_id)
    
    if status is None:
        raise TaskNotFoundError(f"Task {task_id} not found")
    
    if status.status == 'Completed' and status.result_type == 'polars':
        try:
            pl.scan_ipc(status.file_ref)
            logger.debug(f"Validation successful for task: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Validation failed for task {task_id}: {str(e)}")
            raise InvalidResultError(f"Result validation failed: {str(e)}")
    
    return True


@router.get('/status/{task_id}', response_model=models.Status)
@handle_route_errors_sync
def get_status(task_id: str) -> models.Status:
    """
    Get status of a task by ID and validate its result if completed.

    Args:
        task_id: Unique identifier of the task

    Returns:
        Current status of the task
        
    Raises:
        HTTPException: If task not found or invalid result
    """
    logger.debug(f"Getting status for task: {task_id}")
    status = worker_state.get_status(task_id)
    
    if status is None:
        logger.warning(f"Task not found: {task_id}")
        raise TaskNotFoundError(f"Task {task_id} not found")
    
    validate_result(task_id)
    return status


@router.get("/fetch_results/{task_id}", response_model=None)
@handle_route_errors
async def fetch_results(task_id: str):
    """
    Fetch results for a completed task.

    Args:
        task_id: Unique identifier of the task

    Returns:
        Dictionary containing task ID and serialized result data
        
    Raises:
        HTTPException: If result not found or error occurred
    """
    logger.debug(f"Fetching results for task: {task_id}")
    status = worker_state.get_status(task_id)
    
    if not status:
        logger.warning(f"Result not found: {task_id}")
        raise TaskNotFoundError(f"Result not found for task {task_id}")
    
    if status.status == "Processing":
        return Response(status_code=202, content="Result not ready yet")
    
    if status.status == "Error":
        logger.error(f"Task error: {status.error_message}")
        raise HTTPException(
            status_code=404,
            detail=f"An error occurred during processing: {status.error_message}"
        )
    
    try:
        lf = pl.scan_parquet(status.file_ref)
        return {"task_id": task_id, "result": encodebytes(lf.serialize()).decode()}
    except Exception as e:
        logger.error(f"Error reading results: {str(e)}")
        raise HTTPException(status_code=500, detail="Error reading results")


@router.get("/memory_usage/{task_id}")
@handle_route_errors
async def memory_usage(task_id: str) -> Dict:
    """
    Get memory usage for a specific task.

    Args:
        task_id: Unique identifier of the task

    Returns:
        Dictionary containing task ID and memory usage data
        
    Raises:
        HTTPException: If memory usage data not found
    """
    logger.debug(f"Getting memory usage for task: {task_id}")
    memory_usage_value = worker_state.get_memory_usage(task_id)
    
    if memory_usage_value is None:
        logger.warning(f"Memory usage not found: {task_id}")
        raise TaskNotFoundError(f"Memory usage data not found for task {task_id}")
    
    return {"task_id": task_id, "memory_usage": memory_usage_value}


@router.post("/add_fuzzy_join")
@handle_route_errors
async def add_fuzzy_join(
    polars_script: models.FuzzyJoinInput,
    background_tasks: BackgroundTasks
) -> models.Status:
    """
    Start a fuzzy join operation between two dataframes.

    Args:
        polars_script: Input containing left and right dataframes and fuzzy mapping config
        background_tasks: FastAPI background tasks handler

    Returns:
        Status object for the fuzzy join task
        
    Raises:
        HTTPException: If error occurs during setup
    """
    logger.info("Starting fuzzy join operation")
    
    default_cache_dir = create_and_get_default_cache_dir(polars_script.flowfile_flow_id)
    polars_script.task_id = str(uuid.uuid4()) if polars_script.task_id is None else polars_script.task_id
    polars_script.cache_dir = polars_script.cache_dir if polars_script.cache_dir is not None else default_cache_dir
    left_serializable_object = polars_script.left_df_operation.polars_serializable_object()
    right_serializable_object = polars_script.right_df_operation.polars_serializable_object()

    file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
    status = models.Status(
        background_task_id=polars_script.task_id,
        status="Starting",
        file_ref=file_path,
        result_type="polars"
    )
    worker_state.set_status(polars_script.task_id, status)
    
    background_tasks.add_task(
        start_fuzzy_process,
        left_serializable_object=left_serializable_object,
        right_serializable_object=right_serializable_object,
        file_ref=file_path,
        fuzzy_maps=polars_script.fuzzy_maps,
        task_id=polars_script.task_id,
        flowfile_flow_id=polars_script.flowfile_flow_id,
        flowfile_node_id=polars_script.flowfile_node_id
    )
    logger.info(f"Started fuzzy join task: {polars_script.task_id}")
    return status


@router.delete("/clear_task/{task_id}")
@handle_route_errors_sync
def clear_task(task_id: str) -> Dict:
    """
    Clear task data and status by ID.

    Args:
        task_id: Unique identifier of the task to clear
        
    Returns:
        Dictionary with success message
        
    Raises:
        HTTPException: If task not found
    """
    logger.info(f"Clearing task: {task_id}")
    status = worker_state.get_status(task_id)
    
    if not status:
        logger.warning(f"Task not found for clearing: {task_id}")
        raise TaskNotFoundError(f"Task {task_id} not found")
    
    try:
        if os.path.exists(status.file_ref):
            os.remove(status.file_ref)
            logger.debug(f"Removed file: {status.file_ref}")
    except Exception as e:
        logger.error(f"Error removing file {status.file_ref}: {str(e)}", exc_info=True)
    
    # Remove all task state atomically
    worker_state.remove_task(task_id)
    logger.info(f"Successfully cleared task: {task_id}")
    
    return {"message": f"Task {task_id} has been cleared."}


@router.post("/cancel_task/{task_id}")
@handle_route_errors_sync
def cancel_task(task_id: str) -> Dict:
    """
    Cancel a running task by ID.

    Args:
        task_id: Unique identifier of the task to cancel

    Returns:
        Dictionary with success message
        
    Raises:
        HTTPException: If task cannot be cancelled
    """
    logger.info(f"Attempting to cancel task: {task_id}")
    
    if not process_manager.cancel_process(task_id):
        logger.warning(f"Cannot cancel task: {task_id}")
        raise TaskNotFoundError("Task not found or already completed")
    
    # Update status atomically
    if worker_state.task_exists(task_id):
        worker_state.update_status_field(task_id, 'status', 'Cancelled')
        logger.info(f"Successfully cancelled task: {task_id}")
    
    return {"message": f"Task {task_id} has been cancelled."}


@router.get('/ids')
@handle_route_errors
async def get_all_ids() -> List[str]:
    """
    Get list of all task IDs in the system.

    Returns:
        List of all task IDs currently tracked
    """
    logger.debug("Fetching all task IDs")
    ids = worker_state.get_all_task_ids()
    logger.debug(f"Found {len(ids)} tasks")
    return ids