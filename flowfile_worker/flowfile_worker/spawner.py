from flowfile_worker import status_dict
from time import sleep
import gc
from typing import List, Tuple
from multiprocessing import Process, Queue
from flowfile_worker.process_manager import ProcessManager
from flowfile_worker import models, mp_context, funcs, status_dict_lock

# Initialize ProcessManager
process_manager = ProcessManager()

flowfile_node_id_type = int|str


def handle_task(task_id: str, p: Process, progress: mp_context.Value, error_message: mp_context.Array, q: Queue):
    """
    Monitors and manages a running process task, updating its status and handling completion/errors.

    Args:
        task_id (str): Unique identifier for the task
        p (Process): The multiprocessing Process object being monitored
        progress (mp_context.Value): Shared value object tracking task progress (0-100)
        error_message (mp_context.Array): Shared array for storing error messages
        q (Queue): Queue for storing task results

    Notes:
        - Updates task status in status_dict while process is running
        - Handles task cancellation, completion, and error states
        - Cleans up process resources after completion
    """
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
                  file_ref: str, flowfile_flow_id: int,
                  flowfile_node_id: flowfile_node_id_type,
                  kwargs: dict = None) -> None:
    """
    Starts a new process for handling Polars dataframe operations.

    Args:
        polars_serializable_object (bytes): Serialized Polars dataframe
        task_id (str): Unique identifier for the task
        operation (models.OperationType): Type of operation to perform
        file_ref (str): Reference to the file being processed
        kwargs (dict, optional): Additional arguments for the operation. Defaults to {}
        flowfile_flow_id: id of the flow that started the process
        flowfile_node_id: id of the node that started the process

    Notes:
        - Creates shared memory objects for progress tracking and error handling
        - Initializes and starts a new process for the specified operation
        - Delegates to handle_task for process monitoring
    """
    if kwargs is None:
        kwargs = {}
    process_task = getattr(funcs, operation)
    kwargs['polars_serializable_object'] = polars_serializable_object
    kwargs['progress'] = mp_context.Value('i', 0)
    kwargs['error_message'] = mp_context.Array('c', 1024)
    kwargs['queue'] = Queue(maxsize=1)
    kwargs['file_path'] = file_ref
    kwargs['flowfile_flow_id'] = flowfile_flow_id
    kwargs['flowfile_node_id'] = flowfile_node_id

    p: Process = mp_context.Process(target=process_task, kwargs=kwargs)
    p.start()

    process_manager.add_process(task_id, p)
    handle_task(task_id=task_id, p=p, progress=kwargs['progress'], error_message=kwargs['error_message'], q=kwargs['queue'])


def start_generic_process(func_ref: callable, task_id: str,
                          file_ref: str, flowfile_flow_id: int,
                          flowfile_node_id: flowfile_node_id_type, kwargs: dict = None) -> None:
    """
    Starts a new process for handling generic function execution.

    Args:
        func_ref (callable): Reference to the function to be executed
        task_id (str): Unique identifier for the task
        file_ref (str): Reference to the file being processed
        flowfile_flow_id: id of the flow that started the process
        flowfile_node_id: id of the node that started the process
        kwargs (dict, optional): Additional arguments for the function. Defaults to None.

    Notes:
        - Creates shared memory objects for progress tracking and error handling
        - Initializes and starts a new process for the generic function
        - Delegates to handle_task for process monitoring
    """
    kwargs = {} if kwargs is None else kwargs
    kwargs['func'] = func_ref
    kwargs['progress'] = mp_context.Value('i', 0)
    kwargs['error_message'] = mp_context.Array('c', 1024)
    kwargs['queue'] = Queue(maxsize=1)
    kwargs['file_path'] = file_ref
    kwargs['flowfile_flow_id'] = flowfile_flow_id
    kwargs['flowfile_node_id'] = flowfile_node_id

    process_task = getattr(funcs, 'generic_task')
    p: Process = mp_context.Process(target=process_task, kwargs=kwargs)
    p.start()

    process_manager.add_process(task_id, p)  # Add process to process manager
    handle_task(task_id=task_id, p=p, progress=kwargs['progress'],
                error_message=kwargs['error_message'], q=kwargs['queue'])


def start_fuzzy_process(left_serializable_object: bytes,
                        right_serializable_object: bytes,
                        file_ref: str,
                        fuzzy_maps: List[models.FuzzyMapping],
                        task_id: str,
                        flowfile_flow_id: int,
                        flowfile_node_id: flowfile_node_id_type) -> None:
    """
    Starts a new process for performing fuzzy joining operations on two datasets.

    Args:
        left_serializable_object (bytes): Serialized left dataframe
        right_serializable_object (bytes): Serialized right dataframe
        file_ref (str): Reference to the file being processed
        fuzzy_maps (List[models.FuzzyMapping]): List of fuzzy mapping configurations
        task_id (str): Unique identifier for the task
        flowfile_flow_id: id of the flow that started the process
        flowfile_node_id: id of the node that started the process
    Notes:
        - Creates shared memory objects for progress tracking and error handling
        - Initializes and starts a new process for fuzzy joining operation
        - Delegates to handle_task for process monitoring
    """
    progress = mp_context.Value('i', 0)
    error_message = mp_context.Array('c', 1024)
    q = Queue(maxsize=1)

    args: Tuple[bytes, bytes, List[models.FuzzyMapping], mp_context.Array, str, mp_context.Value, Queue, int, flowfile_node_id_type] = \
        (left_serializable_object, right_serializable_object, fuzzy_maps, error_message, file_ref, progress, q,
         flowfile_flow_id, flowfile_node_id)

    p: Process = mp_context.Process(target=funcs.fuzzy_join_task, args=args)
    p.start()

    process_manager.add_process(task_id, p)  # Add process to process manager
    handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)
