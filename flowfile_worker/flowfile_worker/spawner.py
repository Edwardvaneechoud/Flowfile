"""
Process spawning and lifecycle management for Flowfile Worker.

This module handles:
- Process creation and monitoring
- Progress tracking and error handling
- Resource cleanup and semaphore management
- Task cancellation
"""
from flowfile_worker import worker_state, process_semaphore
import gc
import time
from typing import List, Tuple
from multiprocessing import Process, Queue
from flowfile_worker.process_manager import ProcessManager
from flowfile_worker import models, mp_context, funcs
from flowfile_worker.configs import logger, config
from flowfile_worker.error_handlers import encode_error_message

# Initialize ProcessManager
process_manager = ProcessManager()

flowfile_node_id_type = int | str


def handle_task(task_id: str, p: Process, progress: mp_context.Value, error_message: mp_context.Array, q: Queue) -> None:
    """
    Monitor and manage a running process task, updating its status and handling completion/errors.

    This function implements a polling loop that:
    1. Updates task progress in real-time
    2. Checks for cancellation requests
    3. Handles errors reported by the subprocess
    4. Performs comprehensive cleanup after process completion

    Args:
        task_id: Unique identifier for the task
        p: The multiprocessing Process object being monitored
        progress: Shared value object tracking task progress (0-100, -1 for error)
        error_message: Shared array for storing error messages
        q: Queue for storing task results

    Notes:
        The cleanup sequence is critical for proper resource release.
        Order matters to prevent resource leaks and zombie processes.
    """
    try:
        # Set initial status to Processing and record start time
        worker_state.update_status_field(task_id, 'status', 'Processing')
        worker_state.update_status_field(task_id, 'start_time', time.time())

        # Monitor process with responsive polling
        while p.is_alive():
            # Sleep briefly for responsive cancellation checks
            time.sleep(0.5)

            # Read current progress safely
            with progress.get_lock():
                current_progress = progress.value

            # Update status with current progress
            worker_state.update_status_field(task_id, 'progress', current_progress)

            # Check if the task has been cancelled via status
            status = worker_state.get_status(task_id)
            if status and status.status == "Cancelled":
                logger.info(f"Task {task_id} cancelled, terminating process")
                p.terminate()
                break

            # Check for error condition (progress == -1)
            if current_progress == -1:
                with error_message.get_lock():
                    error_msg = error_message.value.decode().rstrip('\x00')

                worker_state.update_status_field(task_id, 'status', 'Error')
                worker_state.update_status_field(task_id, 'error_message', error_msg)
                logger.error(f"Task {task_id} encountered error: {error_msg}")
                break

        # Wait for process to fully exit (it should be done by now)
        p.join(timeout=2)

        # Record end time
        end_time = time.time()
        worker_state.update_status_field(task_id, 'end_time', end_time)

        # Update final status based on completion state
        status = worker_state.get_status(task_id)
        if status and status.status != "Cancelled":
            if progress.value == 100:
                worker_state.update_status_field(task_id, 'status', 'Completed')
                if not q.empty():
                    try:
                        results = q.get_nowait()
                        worker_state.update_status_field(task_id, 'results', results)
                    except:
                        pass
            elif progress.value != -1:
                worker_state.update_status_field(task_id, 'status', 'Unknown Error')
                logger.warning(f"Task {task_id} ended without completion or error flag")

    finally:
        # CRITICAL CLEANUP SEQUENCE
        # The order matters for proper resource release

        # Step 1: Ensure process is fully terminated
        # Try graceful termination first, then force kill if needed
        try:
            if p.is_alive():
                logger.warning(f"Task {task_id} process still alive, terminating")
                p.terminate()
                p.join(timeout=5)

            if p.is_alive():
                logger.error(f"Task {task_id} process did not terminate, killing")
                p.kill()
                p.join(timeout=2)

            if p.is_alive():
                logger.critical(f"Task {task_id} process could not be killed!")
        except Exception as e:
            logger.error(f"Error terminating process for task {task_id}: {e}")

        # Step 2: CRITICAL - Close the process object to release OS resources
        # This is what allows the resource tracker to exit!
        try:
            p.close()
            logger.debug(f"Closed process for task {task_id}")
        except ValueError:
            # Process already closed or not properly initialized
            pass
        except Exception as e:
            logger.error(f"Error closing process for task {task_id}: {e}")

        # Step 3: Clean up the queue
        try:
            # Drain any remaining items
            while not q.empty():
                try:
                    q.get_nowait()
                except:
                    break

            # Close the queue and join its background thread
            q.close()
            q.join_thread()
            logger.debug(f"Cleaned up queue for task {task_id}")
        except Exception as e:
            logger.error(f"Error cleaning up queue for task {task_id}: {e}")

        # Step 4: Remove from process manager and worker state
        try:
            process_manager.remove_process(task_id)
            worker_state.remove_process(task_id)
        except Exception as e:
            logger.error(f"Error removing process from manager for task {task_id}: {e}")

        # Step 5: Delete all references explicitly
        # Setting to None first helps ensure they're released
        try:
            del progress, error_message, q, p
        except Exception as e:
            logger.error(f"Error deleting references for task {task_id}: {e}")

        # Step 6: Force garbage collection to reclaim memory
        gc.collect()

        logger.debug(f"Completed cleanup for task {task_id}")


def start_process(
    polars_serializable_object: bytes,
    task_id: str,
    operation: models.OperationType,
    file_ref: str,
    flowfile_flow_id: int,
    flowfile_node_id: flowfile_node_id_type,
    kwargs: dict = None
) -> None:
    """
    Start a new process for handling Polars dataframe operations.

    This function:
    1. Acquires a semaphore slot (with timeout)
    2. Creates shared memory objects for IPC
    3. Starts the worker process
    4. Monitors the process until completion
    5. Releases the semaphore slot

    Args:
        polars_serializable_object: Serialized Polars dataframe
        task_id: Unique identifier for the task
        operation: Type of operation to perform
        file_ref: Reference to the file being processed
        flowfile_flow_id: ID of the flow that started the process
        flowfile_node_id: ID of the node that started the process
        kwargs: Additional arguments for the operation

    Notes:
        The semaphore ensures we don't exceed MAX_CONCURRENT_PROCESSES.
        Always releases the semaphore, even if process fails to start.
    """
    if kwargs is None:
        kwargs = {}

    # Acquire semaphore slot (wait up to configured timeout)
    logger.debug(f"Attempting to acquire process slot for task {task_id}")
    acquired = process_semaphore.acquire(
        blocking=True,
        timeout=config.process_acquisition_timeout
    )

    if not acquired:
        # Could not acquire slot within timeout - worker at capacity
        logger.error(f"Could not acquire process slot for task {task_id} - worker at capacity")
        worker_state.update_status_field(task_id, 'status', 'Error')
        worker_state.update_status_field(
            task_id,
            'error_message',
            f"Worker at capacity, could not start task within {config.process_acquisition_timeout} seconds"
        )
        return

    logger.debug(f"Acquired process slot for task {task_id}")

    try:
        # Create shared memory objects for IPC
        process_task = getattr(funcs, operation)
        kwargs['polars_serializable_object'] = polars_serializable_object
        kwargs['progress'] = mp_context.Value('i', 0)
        kwargs['error_message'] = mp_context.Array('c', config.error_message_max_length)
        kwargs['queue'] = Queue(maxsize=1)
        kwargs['file_path'] = file_ref
        kwargs['flowfile_flow_id'] = flowfile_flow_id
        kwargs['flowfile_node_id'] = flowfile_node_id

        # Start the process
        p: Process = mp_context.Process(target=process_task, kwargs=kwargs)
        p.start()
        logger.debug(f"Started process {p.pid} for task {task_id}")

        # Register process in both process_manager and worker_state
        process_manager.add_process(task_id, p)
        worker_state.set_process(task_id, p)

        # Monitor the process until completion
        handle_task(
            task_id=task_id,
            p=p,
            progress=kwargs['progress'],
            error_message=kwargs['error_message'],
            q=kwargs['queue']
        )

    finally:
        # CRITICAL: Always release semaphore, even if something fails
        logger.debug(f"Releasing process slot for task {task_id}")
        process_semaphore.release()


def start_generic_process(
    func_ref: callable,
    task_id: str,
    file_ref: str,
    flowfile_flow_id: int,
    flowfile_node_id: flowfile_node_id_type,
    kwargs: dict = None
) -> None:
    """
    Start a new process for handling generic function execution.

    This is similar to start_process but for arbitrary functions
    rather than predefined Polars operations.

    Args:
        func_ref: Reference to the function to be executed
        task_id: Unique identifier for the task
        file_ref: Reference to the file being processed
        flowfile_flow_id: ID of the flow that started the process
        flowfile_node_id: ID of the node that started the process
        kwargs: Additional arguments for the function

    Notes:
        Uses the generic_task wrapper to execute arbitrary functions.
    """
    kwargs = {} if kwargs is None else kwargs

    # Acquire semaphore slot (wait up to configured timeout)
    logger.debug(f"Attempting to acquire process slot for task {task_id}")
    acquired = process_semaphore.acquire(
        blocking=True,
        timeout=config.process_acquisition_timeout
    )

    if not acquired:
        # Could not acquire slot within timeout - worker at capacity
        logger.error(f"Could not acquire process slot for task {task_id} - worker at capacity")
        worker_state.update_status_field(task_id, 'status', 'Error')
        worker_state.update_status_field(
            task_id,
            'error_message',
            f"Worker at capacity, could not start task within {config.process_acquisition_timeout} seconds"
        )
        return

    logger.debug(f"Acquired process slot for task {task_id}")

    try:
        # Create shared memory objects for IPC
        kwargs['func'] = func_ref
        kwargs['progress'] = mp_context.Value('i', 0)
        kwargs['error_message'] = mp_context.Array('c', config.error_message_max_length)
        kwargs['queue'] = Queue(maxsize=1)
        kwargs['file_path'] = file_ref
        kwargs['flowfile_flow_id'] = flowfile_flow_id
        kwargs['flowfile_node_id'] = flowfile_node_id

        process_task = getattr(funcs, 'generic_task')
        p: Process = mp_context.Process(target=process_task, kwargs=kwargs)
        p.start()
        logger.debug(f"Started generic process {p.pid} for task {task_id}")

        # Register process in both process_manager and worker_state
        process_manager.add_process(task_id, p)
        worker_state.set_process(task_id, p)

        # Monitor the process until completion
        handle_task(
            task_id=task_id,
            p=p,
            progress=kwargs['progress'],
            error_message=kwargs['error_message'],
            q=kwargs['queue']
        )

    finally:
        # CRITICAL: Always release semaphore, even if something fails
        logger.debug(f"Releasing process slot for task {task_id}")
        process_semaphore.release()


def start_fuzzy_process(
    left_serializable_object: bytes,
    right_serializable_object: bytes,
    file_ref: str,
    fuzzy_maps: List[models.FuzzyMapping],
    task_id: str,
    flowfile_flow_id: int,
    flowfile_node_id: flowfile_node_id_type
) -> None:
    """
    Start a new process for performing fuzzy joining operations on two datasets.

    Args:
        left_serializable_object: Serialized left dataframe
        right_serializable_object: Serialized right dataframe
        file_ref: Reference to the file being processed
        fuzzy_maps: List of fuzzy mapping configurations
        task_id: Unique identifier for the task
        flowfile_flow_id: ID of the flow that started the process
        flowfile_node_id: ID of the node that started the process

    Notes:
        Fuzzy joins are computationally expensive operations that
        require special handling for progress tracking.
    """
    # Acquire semaphore slot (wait up to configured timeout)
    logger.debug(f"Attempting to acquire process slot for task {task_id}")
    acquired = process_semaphore.acquire(
        blocking=True,
        timeout=config.process_acquisition_timeout
    )

    if not acquired:
        # Could not acquire slot within timeout - worker at capacity
        logger.error(f"Could not acquire process slot for task {task_id} - worker at capacity")
        worker_state.update_status_field(task_id, 'status', 'Error')
        worker_state.update_status_field(
            task_id,
            'error_message',
            f"Worker at capacity, could not start task within {config.process_acquisition_timeout} seconds"
        )
        return

    logger.debug(f"Acquired process slot for task {task_id}")

    try:
        # Create shared memory objects for IPC
        progress = mp_context.Value('i', 0)
        error_message = mp_context.Array('c', config.error_message_max_length)
        q = Queue(maxsize=1)

        args: Tuple[bytes, bytes, List[models.FuzzyMapping], mp_context.Array, str, mp_context.Value, Queue, int, flowfile_node_id_type] = \
            (left_serializable_object, right_serializable_object, fuzzy_maps, error_message, file_ref, progress, q,
             flowfile_flow_id, flowfile_node_id)

        p: Process = mp_context.Process(target=funcs.fuzzy_join_task, args=args)
        p.start()
        logger.debug(f"Started fuzzy process {p.pid} for task {task_id}")

        # Register process in both process_manager and worker_state
        process_manager.add_process(task_id, p)
        worker_state.set_process(task_id, p)

        # Monitor the process until completion
        handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)

    finally:
        # CRITICAL: Always release semaphore, even if something fails
        logger.debug(f"Releasing process slot for task {task_id}")
        process_semaphore.release()