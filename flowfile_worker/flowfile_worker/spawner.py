import gc
import queue as _queue
from base64 import b64encode
from multiprocessing import Process
from multiprocessing.queues import Queue
from time import monotonic, sleep

from flowfile_worker import funcs, models, mp_context, status_dict, status_dict_lock
from flowfile_worker.process_manager import ProcessManager

process_manager = ProcessManager()

flowfile_node_id_type = int | str

# Bound the result drain so a wedged child can't hang the monitor forever, while
# still allowing a large payload's feeder thread time to flush to the pipe.
RESULT_QUEUE_TIMEOUT = 30.0


def drain_result_queue(q: Queue, p: Process, timeout: float = RESULT_QUEUE_TIMEOUT):
    """Read the single result a child puts on *q*, draining BEFORE the caller joins *p*.

    A child that put() a payload larger than the OS pipe buffer (~64 KB) blocks in its
    feeder thread until the parent reads it, so joining first would deadlock. Poll instead
    of a single long get() so we bail the instant the child exits without a result (e.g. it
    crashed after signalling completion but before put()), independent of put/signal order.
    """
    deadline = monotonic() + timeout
    while True:
        try:
            return q.get(timeout=0.1)
        except _queue.Empty:
            if not p.is_alive():
                # Child exited: any put() has been flushed. One last non-blocking read.
                try:
                    return q.get_nowait()
                except _queue.Empty:
                    return None
            if monotonic() >= deadline:
                return None


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

                if status_dict[task_id].status == "Cancelled":
                    p.terminate()
                    break

            if current_progress == -1:
                with status_dict_lock:
                    status_dict[task_id].status = "Error"
                    with error_message.get_lock():
                        status_dict[task_id].error_message = error_message.value.decode().rstrip("\x00")
                break

            # A child that put() a large result blocks in its feeder thread and never
            # exits, so p.is_alive() would spin here forever. Break on the completion
            # signal and let the drain below read the queue (which unblocks the child).
            if current_progress == 100:
                break

        with status_dict_lock:
            cancelled = status_dict[task_id].status == "Cancelled"
        with progress.get_lock():
            final_progress = progress.value

        # Drain the queue BEFORE joining (see drain_result_queue). Only the success path
        # (progress == 100) puts a result; errors travel via the shared Array.
        result = None
        if not cancelled and final_progress == 100:
            result = drain_result_queue(q, p)

        p.join()

        with status_dict_lock:
            status = status_dict[task_id]
            if status.status != "Cancelled":
                if final_progress == 100:
                    status.status = "Completed"
                    if result is not None:
                        # b64-encode bytes for JSON-safe storage in status_dict (REST responses)
                        if isinstance(result, bytes):
                            status.results = b64encode(result).decode("ascii")
                        else:
                            status.results = result
                elif final_progress != -1:
                    status_dict[task_id].status = "Unknown Error"

    finally:
        if p.is_alive():
            p.terminate()
        p.join()
        process_manager.remove_process(task_id)
        del p, progress, error_message
        gc.collect()


def start_process(
    polars_serializable_object: bytes,
    task_id: str,
    operation: models.OperationType,
    file_ref: str,
    flowfile_flow_id: int,
    flowfile_node_id: flowfile_node_id_type,
    kwargs: dict = None,
) -> None:
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
    kwargs["polars_serializable_object"] = polars_serializable_object
    kwargs["progress"] = mp_context.Value("i", 0)
    kwargs["error_message"] = mp_context.Array("c", 1024)
    kwargs["queue"] = mp_context.Queue(maxsize=1)
    kwargs["file_path"] = file_ref
    kwargs["flowfile_flow_id"] = flowfile_flow_id
    kwargs["flowfile_node_id"] = flowfile_node_id

    p: Process = mp_context.Process(target=process_task, kwargs=kwargs)
    p.start()

    process_manager.add_process(task_id, p)
    handle_task(
        task_id=task_id, p=p, progress=kwargs["progress"], error_message=kwargs["error_message"], q=kwargs["queue"]
    )


def start_generic_process(
    func_ref: callable,
    task_id: str,
    file_ref: str,
    flowfile_flow_id: int,
    flowfile_node_id: flowfile_node_id_type,
    kwargs: dict = None,
) -> None:
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
    kwargs["func"] = func_ref
    kwargs["progress"] = mp_context.Value("i", 0)
    kwargs["error_message"] = mp_context.Array("c", 1024)
    kwargs["queue"] = mp_context.Queue(maxsize=1)
    kwargs["file_path"] = file_ref
    kwargs["flowfile_flow_id"] = flowfile_flow_id
    kwargs["flowfile_node_id"] = flowfile_node_id

    process_task = funcs.generic_task
    p: Process = mp_context.Process(target=process_task, kwargs=kwargs)
    p.start()

    process_manager.add_process(task_id, p)
    handle_task(
        task_id=task_id, p=p, progress=kwargs["progress"], error_message=kwargs["error_message"], q=kwargs["queue"]
    )


def start_train_model_process(
    polars_serializable_object: bytes,
    task_id: str,
    file_ref: str,
    model_type: str,
    target_column: str,
    feature_columns: list[str],
    params: dict,
    staging_path: str,
    flowfile_flow_id: int,
    flowfile_node_id: flowfile_node_id_type,
) -> None:
    """Spawn the training subprocess.

    Mirrors :func:`start_fuzzy_process`. The trained-model bytes are written to
    *staging_path*; ``handle_task`` will surface ``{sha256, size_bytes, model_type}``
    via the queue so core can finalise the artifact upload.
    """
    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    q = mp_context.Queue(maxsize=1)

    kwargs = {
        "polars_serializable_object": polars_serializable_object,
        "progress": progress,
        "error_message": error_message,
        "queue": q,
        "file_path": file_ref,
        "model_type": model_type,
        "target_column": target_column,
        "feature_columns": feature_columns,
        "params": params or {},
        "staging_path": staging_path,
        "flowfile_flow_id": flowfile_flow_id,
        "flowfile_node_id": flowfile_node_id,
    }

    p: Process = mp_context.Process(target=funcs.train_model_task, kwargs=kwargs)
    p.start()
    process_manager.add_process(task_id, p)
    handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)


def start_apply_model_process(
    polars_serializable_object: bytes,
    task_id: str,
    file_ref: str,
    model_path: str,
    output_column: str,
    flowfile_flow_id: int,
    flowfile_node_id: flowfile_node_id_type,
) -> None:
    """Spawn the apply-model subprocess.

    Writes the scored data to *file_ref* (IPC). ``handle_task`` will surface the
    serialised LazyFrame via the queue so core can deserialise it.
    """
    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    q = mp_context.Queue(maxsize=1)

    kwargs = {
        "polars_serializable_object": polars_serializable_object,
        "progress": progress,
        "error_message": error_message,
        "queue": q,
        "file_path": file_ref,
        "model_path": model_path,
        "output_column": output_column,
        "flowfile_flow_id": flowfile_flow_id,
        "flowfile_node_id": flowfile_node_id,
    }

    p: Process = mp_context.Process(target=funcs.apply_model_task, kwargs=kwargs)
    p.start()
    process_manager.add_process(task_id, p)
    handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)


def start_fuzzy_process(
    left_serializable_object: bytes,
    right_serializable_object: bytes,
    file_ref: str,
    fuzzy_maps: list[models.FuzzyMapping],
    task_id: str,
    flowfile_flow_id: int,
    flowfile_node_id: flowfile_node_id_type,
) -> None:
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
    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    q = mp_context.Queue(maxsize=1)

    args: tuple[
        bytes,
        bytes,
        list[models.FuzzyMapping],
        mp_context.Array,
        str,
        mp_context.Value,
        Queue,
        int,
        flowfile_node_id_type,
    ] = (
        left_serializable_object,
        right_serializable_object,
        fuzzy_maps,
        error_message,
        file_ref,
        progress,
        q,
        flowfile_flow_id,
        flowfile_node_id,
    )

    p: Process = mp_context.Process(target=funcs.fuzzy_join_task, args=args)
    p.start()

    process_manager.add_process(task_id, p)
    handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)
