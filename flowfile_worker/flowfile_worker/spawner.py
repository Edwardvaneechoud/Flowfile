from flowfile_worker import status_dict
from time import sleep
import gc
from typing import List, Tuple
from multiprocessing import Process, Queue
from flowfile_worker.process_manager import ProcessManager
from flowfile_worker import models, mp_context, funcs, status_dict_lock

# Initialize ProcessManager
process_manager = ProcessManager()


def handle_task(task_id: str, p: Process, progress: mp_context.Value, error_message: mp_context.Array, q: Queue):
    try:
        with status_dict_lock:
            status_dict[task_id].status = "Processing"

        while p.is_alive():
            sleep(1)
            with progress.get_lock():
                current_progress = progress.value
            with status_dict_lock:
                status_dict[task_id].progress = current_progress

                # Check if the task has been cancelled via status_dict
                if status_dict[task_id].status == "Cancelled":
                    p.terminate()
                    break

            if current_progress == -1:
                with status_dict_lock:
                    status_dict[task_id].status = "Error"
                    with error_message.get_lock():
                        status_dict[task_id].error_message = error_message.value.decode().rstrip('\x00')
                break

        p.join()

        with status_dict_lock:
            status = status_dict[task_id]
            if status.status != "Cancelled":
                if progress.value == 100:
                    status.status = "Completed"
                    if not q.empty():
                        status.results = q.get()
                elif progress.value != -1:
                    status_dict[task_id].status = "Unknown Error"

    finally:
        if p.is_alive():
            p.terminate()
        p.join()
        process_manager.remove_process(task_id)  # Remove from process manager
        del p, progress, error_message
        gc.collect()


def start_process(polars_serializable_object: bytes, task_id: str,
                  operation: models.OperationType,
                  file_ref: str, args: tuple = ()) -> None:
    progress = mp_context.Value('i', 0)  # Shared integer to track progress
    error_message = mp_context.Array('c', 256)  # Shared array to track error message
    process_task = getattr(funcs, operation)
    q = Queue(maxsize=1)

    if args == ():
        args = (polars_serializable_object, progress, error_message, q, file_ref)
    else:
        args = (polars_serializable_object, progress, error_message, q, file_ref, *args)
    # polars_serializable_object,progress,error_message,q,file_ref,data_type,path,write_mode,sheet_name,delimiter = args
    p: Process = mp_context.Process(target=process_task, args=args)
    p.start()

    process_manager.add_process(task_id, p)  # Add process to process manager
    handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)


def start_generic_process(func_ref: callable, task_id: str,
                          file_ref: str, args: Tuple = ()) -> None:
    progress = mp_context.Value('i', 0)  # Shared integer to track progress
    error_message = mp_context.Array('c', 256)  # Shared array to track error message
    process_task = getattr(funcs, 'generic_task')
    q = Queue(maxsize=1)

    if args == ():
        args = (func_ref, progress, error_message, q, file_ref)
    else:
        args = (func_ref, progress, error_message, q, file_ref, *args)

    p: Process = mp_context.Process(target=process_task, args=args)
    p.start()

    process_manager.add_process(task_id, p)  # Add process to process manager
    handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)


def start_fuzzy_process(left_serializable_object: bytes,
                        right_serializable_object: bytes,
                        file_ref: str,
                        fuzzy_maps: List[models.FuzzyMapping],
                        task_id: str):
    progress = mp_context.Value('i', 0)  # Shared integer to track progress
    error_message = mp_context.Array('c', 256)  # Shared array to track error message
    q = Queue(maxsize=1)

    args: Tuple[bytes, bytes, List[models.FuzzyMapping], mp_context.Array, str, mp_context.Value, Queue] = \
        (left_serializable_object, right_serializable_object, fuzzy_maps, error_message, file_ref, progress, q)

    p: Process = mp_context.Process(target=funcs.fuzzy_join_task, args=args)
    p.start()

    process_manager.add_process(task_id, p)  # Add process to process manager
    handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)
