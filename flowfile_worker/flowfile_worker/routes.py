import polars as pl
import uuid
import os
from fastapi import APIRouter, HTTPException, Response, BackgroundTasks
from typing import Dict
from base64 import encodebytes

from flowfile_worker import status_dict, CACHE_DIR, PROCESS_MEMORY_USAGE, status_dict_lock
from flowfile_worker import models
from flowfile_worker.spawner import start_process, start_fuzzy_process, start_generic_process, process_manager
from flowfile_worker.create import table_creator_factory_method, received_table_parser, FileType
from flowfile_worker.configs import logger
from flowfile_worker.external_sources.airbyte_sources.models import AirbyteSettings
from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings
from flowfile_worker.external_sources.sql_source.main import read_sql_source, write_serialized_df_to_database
from flowfile_worker.external_sources.airbyte_sources.main import read_airbyte_source


router = APIRouter()


@router.post("/submit_query/")
def submit_query(polars_script: models.PolarsScript, background_tasks: BackgroundTasks) -> models.Status:
    logger.info(f"Processing query with operation: {polars_script.operation_type}")

    try:
        polars_script.task_id = str(uuid.uuid4()) if polars_script.task_id is None else polars_script.task_id
        polars_script.cache_dir = polars_script.cache_dir if polars_script.cache_dir is not None else CACHE_DIR.name
        polars_serializable_object = polars_script.polars_serializable_object()
        file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
        result_type = "polars" if polars_script.operation_type == "store" else "other"
        status = models.Status(background_task_id=polars_script.task_id, status="Starting", file_ref=file_path,
                               result_type=result_type)
        status_dict[polars_script.task_id] = status
        background_tasks.add_task(start_process, polars_serializable_object=polars_serializable_object,
                                  task_id=polars_script.task_id, operation=polars_script.operation_type,
                                  file_ref=file_path, flowfile_flow_id=polars_script.flowfile_flow_id,
                                  flowfile_node_id=polars_script.flowfile_node_id,
                                  kwargs={}
                                  )
        logger.info(f"Started background task: {polars_script.task_id}")
        return status

    except Exception as e:
        logger.error(f"Error processing query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/store_sample/')
def store_sample(polars_script: models.PolarsScriptSample, background_tasks: BackgroundTasks) -> models.Status:
    logger.info(f"Processing sample storage with size: {polars_script.sample_size}")

    try:
        polars_script.task_id = str(uuid.uuid4()) if polars_script.task_id is None else polars_script.task_id
        polars_script.cache_dir = polars_script.cache_dir if polars_script.cache_dir is not None else CACHE_DIR.name
        polars_serializable_object = polars_script.polars_serializable_object()

        file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
        status = models.Status(background_task_id=polars_script.task_id, status="Starting", file_ref=file_path,
                               result_type="other")
        status_dict[polars_script.task_id] = status

        background_tasks.add_task(start_process, polars_serializable_object=polars_serializable_object,
                                  task_id=polars_script.task_id, operation=polars_script.operation_type,
                                  file_ref=file_path, flowfile_flow_id=polars_script.flowfile_flow_id,
                                  flowfile_node_id=polars_script.flowfile_node_id,
                                  kwargs={'sample_size': polars_script.sample_size})
        logger.info(f"Started sample storage task: {polars_script.task_id}")

        return status

    except Exception as e:
        logger.error(f"Error storing sample: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/store_database_write_result/')
def store_in_database(database_script_write: models.DatabaseScriptWrite, background_tasks: BackgroundTasks) -> models.Status:
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
        status = models.Status(background_task_id=task_id, status="Starting", file_ref='',
                               result_type="other")
        status_dict[task_id] = status
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

        logger.info(
            f"Started write task: {task_id} to database"
        )

        return status

    except Exception as e:
        logger.error(f"Error in write operation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/write_results/')
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
        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path,
                               result_type=result_type)
        status_dict[task_id] = status
        background_tasks.add_task(start_process,
                                  polars_serializable_object=polars_serializable_object, task_id=task_id,
                                  operation="write_output",
                                  file_ref=file_path,
                                  flowfile_flow_id=polars_script_write.flowfile_flow_id,
                                  flowfile_node_id=polars_script_write.flowfile_node_id,
                                  kwargs=dict(
                                      data_type=polars_script_write.data_type,
                                      path=polars_script_write.path,
                                      write_mode=polars_script_write.write_mode,
                                      sheet_name=polars_script_write.sheet_name,
                                      delimiter=polars_script_write.delimiter)
                                  )
        logger.info(f"Started write task: {task_id} with type: {polars_script_write.data_type}")

        return status

    except Exception as e:
        logger.error(f"Error in write operation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/store_airbyte_result')
def store_airbyte_result(airbyte_settings: AirbyteSettings, background_tasks: BackgroundTasks) -> models.Status:
    """
    Store the result of an Airbyte source operation.

    Args:
        airbyte_settings (AirbyteSettings): Settings for the Airbyte source operation
        background_tasks (BackgroundTasks): FastAPI background tasks handler

    Returns:
        models.Status: Status object tracking the Airbyte source operation
    """
    logger.info("Processing Airbyte source operation")

    try:
        task_id = str(uuid.uuid4())
        file_path = os.path.join(CACHE_DIR.name, f"{task_id}.arrow")
        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path,
                               result_type="polars")
        status_dict[task_id] = status
        logger.info(f"Starting Airbyte source task: {task_id}")
        background_tasks.add_task(start_generic_process, func_ref=read_airbyte_source, file_ref=file_path,
                                  flowfile_flow_id=airbyte_settings.flowfile_flow_id,
                                  flowfile_node_id=airbyte_settings.flowfile_node_id,
                                  task_id=task_id, kwargs=dict(airbyte_settings=airbyte_settings))
        logger.info(f"Started Airbyte source task: {task_id}")

        return status

    except Exception as e:
        logger.error(f"Error processing Airbyte source: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/store_database_read_result')
def store_sql_db_result(database_read_settings: DatabaseReadSettings, background_tasks: BackgroundTasks) -> models.Status:
    """
    Store the result of an Airbyte source operation.

    Args:
        database_read_settings (SQLSourceSettings): Settings for the SQL source operation
        background_tasks (BackgroundTasks): FastAPI background tasks handler

    Returns:
        models.Status: Status object tracking the Sql operation
    """
    logger.info("Processing Airbyte source operation")

    try:
        task_id = str(uuid.uuid4())
        file_path = os.path.join(CACHE_DIR.name, f"{task_id}.arrow")
        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path,
                               result_type="polars")
        status_dict[task_id] = status
        logger.info(f"Starting reading from database source task: {task_id}")
        background_tasks.add_task(start_generic_process, func_ref=read_sql_source, file_ref=file_path,
                                  flowfile_flow_id=database_read_settings.flowfile_flow_id,
                                  flowfile_node_id=database_read_settings.flowfile_node_id,
                                  task_id=task_id, kwargs=dict(database_read_settings=database_read_settings))
        return status

    except Exception as e:
        logger.error(f"Error processing sql source: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/create_table/{file_type}')
def create_table(file_type: FileType, received_table: Dict, background_tasks: BackgroundTasks,
                 flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1) -> models.Status:
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
        file_ref = os.path.join(CACHE_DIR.name, f"{task_id}.arrow")

        status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_ref,
                               result_type="polars")
        status_dict[task_id] = status
        func_ref = table_creator_factory_method(file_type)
        received_table_parsed = received_table_parser(received_table, file_type)
        background_tasks.add_task(start_generic_process, func_ref=func_ref, file_ref=file_ref,
                                  task_id=task_id, kwargs={'received_table': received_table_parsed},
                                  flowfile_flow_id=flowfile_flow_id,
                                  flowfile_node_id=flowfile_node_id)
        logger.info(f"Started table creation task: {task_id}")

        return status

    except Exception as e:
        logger.error(f"Error creating table: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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
    if status.status == 'Completed' and status.result_type == 'polars':
        try:
            pl.scan_ipc(status.file_ref)
            logger.debug(f"Validation successful for task: {task_id}")
            return True
        except Exception as e:
            logger.error(f"Validation failed for task {task_id}: {str(e)}")
            return False
    return True


@router.get('/status/{task_id}', response_model=models.Status)
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
        return {"task_id": task_id, "result": encodebytes(lf.serialize()).decode()}
    except Exception as e:
        logger.error(f"Error reading results: {str(e)}")
        raise HTTPException(status_code=500, detail="Error reading results")


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
        polars_script.task_id = str(uuid.uuid4()) if polars_script.task_id is None else polars_script.task_id
        polars_script.cache_dir = polars_script.cache_dir if polars_script.cache_dir is not None else CACHE_DIR.name
        left_serializable_object = polars_script.left_df_operation.polars_serializable_object()
        right_serializable_object = polars_script.right_df_operation.polars_serializable_object()

        file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
        status = models.Status(background_task_id=polars_script.task_id, status="Starting", file_ref=file_path,
                               result_type="polars")
        status_dict[polars_script.task_id] = status
        background_tasks.add_task(start_fuzzy_process, left_serializable_object=left_serializable_object,
                                  right_serializable_object=right_serializable_object,
                                  file_ref=file_path,
                                  fuzzy_maps=polars_script.fuzzy_maps,
                                  task_id=polars_script.task_id,
                                  flowfile_flow_id=polars_script.flowfile_flow_id,
                                  flowfile_node_id=polars_script.flowfile_node_id)
        logger.info(f"Started fuzzy join task: {polars_script.task_id}")
        return status
    except Exception as e:
        logger.error(f"Error in fuzzy join: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


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


@router.get('/ids')
async def get_all_ids():
    """Get list of all task IDs in the system.

    Returns:
        list: List of all task IDs currently tracked
    """
    logger.debug("Fetching all task IDs")
    ids = [k for k in status_dict.keys()]
    logger.debug(f"Found {len(ids)} tasks")
    return ids
