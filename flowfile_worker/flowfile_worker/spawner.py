import gc
import os
import queue as _queue
import threading
from base64 import b64encode
from multiprocessing import Process
from multiprocessing.queues import Queue
from time import monotonic, sleep
from typing import Any

from flowfile_worker import funcs, models, mp_context, pool, status_dict, status_dict_lock
from flowfile_worker.pool import PoolMember
from flowfile_worker.process_manager import ProcessManager

process_manager = ProcessManager()

flowfile_node_id_type = int | str

# Bound the result drain so a wedged child can't hang the monitor forever, while
# still allowing a large payload's feeder thread time to flush to the pipe.
RESULT_QUEUE_TIMEOUT = 30.0

# Poll the child with exponential backoff: snappy at first, but capped so idle
# long-running jobs don't busy-wait. The 1.0s steady-state cap is deliberate.
_POLL_INITIAL_DELAY = 0.01
_POLL_MAX_DELAY = 1.0
_POLL_BACKOFF = 1.5

# Wall-clock ceiling for a single task. A child that stays alive but never signals
# completion (wedged in a native call, deadlocked) would otherwise spin the monitor
# forever. Generous so real long jobs aren't killed; 0 disables. Env-tunable.
_TASK_TIMEOUT = float(os.environ.get("FLOWFILE_WORKER_TASK_TIMEOUT", "3600"))


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


def unpack_result(result):
    """store/generic_task put (payload, row_count); every other op puts a bare payload."""
    if isinstance(result, tuple) and len(result) == 2:
        return result
    return result, None


def drain_member_envelope(q: Queue, p: Process) -> tuple[bool, Any]:
    """Drain a pool member's completion envelope with a hard wall-clock bound.

    q.get()'s timeout only bounds the poll, not the recv: a member terminated
    mid-put (a raced /cancel_task) can leave a partial message that would block
    the recv unboundedly, so the drain runs on a watchdog thread. No envelope
    within the bound marks the member unfit for reuse.
    """
    box: list = []
    watchdog = threading.Thread(target=lambda: box.append(drain_result_queue(q, p)), daemon=True)
    watchdog.start()
    watchdog.join(RESULT_QUEUE_TIMEOUT + 5.0)
    if not box:
        return False, None
    return pool.unwrap_envelope(box[0])


def handle_task(
    task_id: str,
    p: Process,
    progress: mp_context.Value,
    error_message: mp_context.Array,
    q: Queue,
    member: PoolMember | None = None,
):
    """
    Monitors and manages a running process task, updating its status and handling completion/errors.

    Args:
        task_id (str): Unique identifier for the task
        p (Process): The multiprocessing Process object being monitored
        progress (mp_context.Value): Shared value object tracking task progress (0-100)
        error_message (mp_context.Array): Shared array for storing error messages
        q (Queue): Queue for storing task results
        member (PoolMember | None): Set when *p* is a leased pool member. Its completion
            envelope is then always drained (never joined mid-task), and instead of the
            terminate/join teardown the member is checked back in - reusable only if the
            envelope arrived; cancel, timeout, or a crash retire it.

    Notes:
        - Updates task status in status_dict while process is running
        - Handles task cancellation, completion, and error states
        - Cleans up process resources after completion
    """
    envelope_received = False
    try:
        with status_dict_lock:
            status_dict[task_id].status = "Processing"

        delay = _POLL_INITIAL_DELAY
        deadline = (monotonic() + _TASK_TIMEOUT) if _TASK_TIMEOUT else None
        timed_out = False
        while p.is_alive():
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

            if deadline is not None and monotonic() > deadline:
                timed_out = True
                p.terminate()
                break

            sleep(delay)
            delay = min(delay * _POLL_BACKOFF, _POLL_MAX_DELAY)

        with status_dict_lock:
            cancelled = status_dict[task_id].status == "Cancelled"
        with progress.get_lock():
            final_progress = progress.value

        # Drain the queue BEFORE joining (see drain_result_queue). Only the success path
        # (progress == 100) puts a result; errors travel via the shared Array. A pool
        # member puts one envelope per task (also on task error), drained under a
        # watchdog bound - but a member we terminated (cancel/timeout) or that died is
        # never drained: a partial envelope from a killed writer can block the recv.
        result = None
        number_of_records = None
        if member is not None:
            # This task is over for the process manager either way; unmapping first
            # means a stale /cancel_task can no longer terminate a reusable member.
            process_manager.remove_process(task_id)
            if not cancelled and not timed_out and final_progress in (100, -1):
                envelope_received, payload = drain_member_envelope(q, p)
                if final_progress == 100:
                    result, number_of_records = unpack_result(payload)
        else:
            if not cancelled and final_progress == 100:
                result, number_of_records = unpack_result(drain_result_queue(q, p))
            p.join()

        with status_dict_lock:
            status = status_dict[task_id]
            if status.status != "Cancelled":
                if final_progress == 100:
                    status.status = "Completed"
                    if number_of_records is not None:
                        status.number_of_records = number_of_records
                    if result is not None:
                        # b64-encode bytes for JSON-safe storage in status_dict (REST responses)
                        if isinstance(result, bytes):
                            status.results = b64encode(result).decode("ascii")
                        else:
                            status.results = result
                elif timed_out:
                    status.status = "Error"
                    status.error_message = f"Task exceeded the {_TASK_TIMEOUT:.0f}s time limit and was terminated"
                elif final_progress == -1:
                    # The child signalled an error but the monitor loop may not have observed
                    # it (a child can die before we read progress == -1). Surface a terminal
                    # Error so the core poller doesn't wait on "Processing" forever.
                    status.status = "Error"
                    if not status.error_message:
                        with error_message.get_lock():
                            decoded = error_message.value.decode(errors="replace").rstrip("\x00")
                        status.error_message = decoded or "Task failed"
                else:
                    status.status = "Unknown Error"

    finally:
        if member is not None:
            pool.task_pool.checkin(member, reusable=envelope_received)
        else:
            if p.is_alive():
                p.terminate()
            p.join()
        process_manager.remove_process(task_id)
        del p, progress, error_message
        gc.collect(0)


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
        - Leases a warm pool member when the pool is on and the operation is poolable
        - Otherwise creates shared memory objects and spawns a fresh process (default)
        - Delegates to handle_task for process monitoring either way
    """
    if kwargs is None:
        kwargs = {}
    kwargs["polars_serializable_object"] = polars_serializable_object
    kwargs["file_path"] = file_ref
    kwargs["flowfile_flow_id"] = flowfile_flow_id
    kwargs["flowfile_node_id"] = flowfile_node_id

    member = pool.task_pool.acquire(operation)
    if member is not None:
        try:
            member.submit(operation, kwargs)
            process_manager.add_process(task_id, member.process)
        except Exception:
            # A failed lease must not leak the slot; handle_task owns checkin after this.
            pool.task_pool.checkin(member, reusable=False)
            raise
        handle_task(
            task_id=task_id,
            p=member.process,
            progress=member.progress,
            error_message=member.error_message,
            q=member.result_q,
            member=member,
        )
        return

    process_task = getattr(funcs, operation)
    kwargs["progress"] = mp_context.Value("i", 0)
    kwargs["error_message"] = mp_context.Array("c", 1024)
    kwargs["queue"] = mp_context.Queue(maxsize=1)

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


def start_custom_node_process(
    custom_node_input: "models.CustomNodeExecuteInput",
    file_ref: str,
    task_id: str,
) -> None:
    """Spawn the custom-node subprocess.

    Mirrors :func:`start_fuzzy_process`; the child target lives in
    ``custom_node_runner`` and puts a JSON payload with output paths /
    row counts (plus preview + logs for dry runs) on the queue.
    """
    from flowfile_worker import custom_node_runner

    progress = mp_context.Value("i", 0)
    error_message = mp_context.Array("c", 1024)
    q = mp_context.Queue(maxsize=1)

    kwargs = {
        "node_source": custom_node_input.node_source,
        "class_name": custom_node_input.class_name,
        "settings_values": custom_node_input.settings_values,
        "secrets": custom_node_input.secrets,
        "inputs": list(custom_node_input.inputs),
        "output_names": custom_node_input.output_names,
        "dry_run": custom_node_input.dry_run,
        "row_limit": custom_node_input.row_limit,
        "user_id": custom_node_input.user_id,
        "progress": progress,
        "error_message": error_message,
        "queue": q,
        "file_path": file_ref,
        "flowfile_flow_id": custom_node_input.flowfile_flow_id,
        "flowfile_node_id": custom_node_input.flowfile_node_id,
    }

    p: Process = mp_context.Process(target=custom_node_runner.execute_custom_node_task, kwargs=kwargs)
    p.start()

    process_manager.add_process(task_id, p)
    handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)
