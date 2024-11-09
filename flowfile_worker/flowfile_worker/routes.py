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

router = APIRouter()


@router.post("/submit_query/")
def submit_query(polars_script: models.PolarsScript, background_tasks: BackgroundTasks) -> models.Status:
    polars_script.task_id = str(uuid.uuid4()) if polars_script.task_id is None else polars_script.task_id
    polars_script.cache_dir = polars_script.cache_dir if polars_script.cache_dir is not None else CACHE_DIR.name
    polars_serializable_object = polars_script.polars_serializable_object()
    file_path = os.path.join(polars_script.cache_dir, f"{polars_script.task_id}.arrow")
    result_type = "polars" if polars_script.operation_type == "store" else "other"
    status = models.Status(background_task_id=polars_script.task_id, status="Starting", file_ref=file_path,
                           result_type=result_type)
    status_dict[polars_script.task_id] = status
    background_tasks.add_task(start_process, polars_serializable_object=polars_serializable_object,
                              task_id=polars_script.task_id, operation=polars_script.operation_type, file_ref=file_path,
                              args=())
    return status


@router.post('/write_results/')
def write_results(polars_script_write: models.PolarsScriptWrite, background_tasks: BackgroundTasks) -> models.Status:
    task_id = str(uuid.uuid4())
    file_path = polars_script_write.path
    polars_serializable_object = polars_script_write.polars_serializable_object()
    result_type = "other"
    status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path, result_type=result_type)
    status_dict[task_id] = status
    background_tasks.add_task(start_process, polars_serializable_object, task_id, "write_output", file_ref=file_path,
                  args=(polars_script_write.data_type, polars_script_write.path, polars_script_write.write_mode,
                        polars_script_write.sheet_name, polars_script_write.delimiter))
    return status


@router.post('/create_table/{file_type}')
def create_table(file_type: FileType, received_table: Dict, background_tasks: BackgroundTasks) -> models.Status:
    task_id = str(uuid.uuid4())
    file_path = os.path.join(CACHE_DIR.name, f"{task_id}.arrow")
    status = models.Status(background_task_id=task_id, status="Starting", file_ref=file_path,
                           result_type="polars")
    status_dict[task_id] = status
    table_creator_func = table_creator_factory_method(file_type)
    received_table_parsed = received_table_parser(received_table, file_type)
    background_tasks.add_task(start_generic_process, func_ref=table_creator_func, file_ref=file_path, task_id=task_id,
                                                    args=(received_table_parsed,))
    return status

def validate_result(task_id: str) -> bool | None:
    status = status_dict.get(task_id)
    if status.status == 'Completed' and status.result_type == 'polars':
        try:
            pl.scan_ipc(status.file_ref)
            return True
        except Exception as e:
            print(e)
            return False
    return True


@router.get('/status/{task_id}', response_model=models.Status)
def get_status(task_id: str) -> models.Status:
    status = status_dict.get(task_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Task not found")
    result_valid = validate_result(task_id)
    if not result_valid:
        raise HTTPException(status_code=404, detail="Task not found")
    return status


@router.get("/fetch_results/{task_id}")
async def fetch_results(task_id: str):
    status = status_dict.get(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Result not found")
    if status.status == "Processing":
        return Response(status_code=202, content="Result not ready yet")
    if status.status == "Error":
        raise HTTPException(status_code=404, detail=f"An error occurred during processing: {status.error_message}")
    lf = pl.scan_parquet(status.file_ref)
    return {"task_id": task_id, "result": encodebytes(lf.serialize()).decode()}


@router.get("/memory_usage/{task_id}")
async def memory_usage(task_id: str):
    memory_usage = PROCESS_MEMORY_USAGE.get(task_id)
    if memory_usage is None:
        raise HTTPException(status_code=404, detail="Memory usage data not found for this task ID")
    return {"task_id": task_id, "memory_usage": memory_usage}


@router.post("/add_fuzzy_join")
async def add_fuzzy_join(polars_script: models.FuzzyJoinInput, background_tasks: BackgroundTasks) -> models.Status:
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
                              task_id=polars_script.task_id)
    return status


@router.post("/cancel_task/{task_id}")
def cancel_task(task_id: str):
    if not process_manager.cancel_process(task_id):
        raise HTTPException(status_code=404, detail="Task not found or already completed")
    with status_dict_lock:
        if task_id in status_dict:
            status_dict[task_id].status = "Cancelled"  # Update the task status in status_dict
    return {"message": f"Task {task_id} has been cancelled."}


@router.get('/ids')
async def get_all_ids():
    return [k for k in status_dict.keys()]