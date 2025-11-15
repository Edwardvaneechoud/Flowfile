from flowfile_worker import status_dict
import gc
import time
from typing import List, Tuple
from multiprocessing import Process, Queue
from flowfile_worker.process_manager import ProcessManager
from flowfile_worker import models, mp_context, funcs, status_dict_lock, process_semaphore
from flowfile_worker.configs import logger

# Initialize ProcessManager
process_manager = ProcessManager()

flowfile_node_id_type = int | str


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

        # Monitor process with responsive polling
        while p.is_alive():
            # Sleep briefly for responsive cancellation checks
            time.sleep(0.5)

            with progress.get_lock():
                current_progress = progress.value

            with status_dict_lock:
                status_dict[task_id].progress = current_progress

                # Check if the task has been cancelled via status_dict
                if status_dict[task_id].status == "Cancelled":
                    logger.info(f"Task {task_id} cancelled, terminating process")
                    p.terminate()
                    break

            # Check for error condition
            if current_progress == -1:
                with status_dict_lock:
                    status_dict[task_id].status = "Error"
                    with error_message.get_lock():
                        status_dict[task_id].error_message = error_message.value.decode().rstrip('\x00')
                logger.error(f"Task {task_id} encountered error: {status_dict[task_id].error_message}")
                break

        # Wait for process to fully exit (it should be done by now)
        p.join(timeout=2)

        # Update final status
        with status_dict_lock:
            status = status_dict[task_id]
            if status.status != "Cancelled":
                if progress.value == 100:
                    status.status = "Completed"
                    if not q.empty():
                        try:
                            status.results = q.get_nowait()
                        except:
                            pass
                elif progress.value != -1:
                    status_dict[task_id].status = "Unknown Error"
                    logger.warning(f"Task {task_id} ended without completion or error flag")

    finally:
        # CRITICAL CLEANUP SEQUENCE
        # The order matters for proper resource release

        # Step 1: Ensure process is fully terminated
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

        # Step 4: Remove from process manager
        try:
            process_manager.remove_process(task_id)
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

    # Acquire semaphore slot (wait up to 30 seconds)
    logger.debug(f"Attempting to acquire process slot for task {task_id}")
    acquired = process_semaphore.acquire(blocking=True, timeout=30)

    if not acquired:
        # Could not acquire slot within timeout - worker at capacity
        logger.error(f"Could not acquire process slot for task {task_id} - worker at capacity")
        with status_dict_lock:
            status_dict[task_id].status = "Error"
            status_dict[task_id].error_message = "Worker at capacity, could not start task within 30 seconds"
        return

    logger.debug(f"Acquired process slot for task {task_id}")

    try:
        # Create shared memory objects
        process_task = getattr(funcs, operation)
        kwargs['polars_serializable_object'] = polars_serializable_object
        kwargs['progress'] = mp_context.Value('i', 0)
        kwargs['error_message'] = mp_context.Array('c', 1024)
        kwargs['queue'] = Queue(maxsize=1)
        kwargs['file_path'] = file_ref
        kwargs['flowfile_flow_id'] = flowfile_flow_id
        kwargs['flowfile_node_id'] = flowfile_node_id

        # Start the process
        p: Process = mp_context.Process(target=process_task, kwargs=kwargs)
        p.start()
        logger.debug(f"Started process {p.pid} for task {task_id}")

        process_manager.add_process(task_id, p)

        # Monitor the process until completion
        handle_task(task_id=task_id, p=p, progress=kwargs['progress'],
                    error_message=kwargs['error_message'], q=kwargs['queue'])

    finally:
        # CRITICAL: Always release semaphore, even if something fails
        logger.debug(f"Releasing process slot for task {task_id}")
        process_semaphore.release()


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

    # Acquire semaphore slot (wait up to 30 seconds)
    logger.debug(f"Attempting to acquire process slot for task {task_id}")
    acquired = process_semaphore.acquire(blocking=True, timeout=30)

    if not acquired:
        # Could not acquire slot within timeout - worker at capacity
        logger.error(f"Could not acquire process slot for task {task_id} - worker at capacity")
        with status_dict_lock:
            status_dict[task_id].status = "Error"
            status_dict[task_id].error_message = "Worker at capacity, could not start task within 30 seconds"
        return

    logger.debug(f"Acquired process slot for task {task_id}")

    try:
        # Create shared memory objects
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
        logger.debug(f"Started generic process {p.pid} for task {task_id}")

        process_manager.add_process(task_id, p)

        # Monitor the process until completion
        handle_task(task_id=task_id, p=p, progress=kwargs['progress'],
                    error_message=kwargs['error_message'], q=kwargs['queue'])

    finally:
        # CRITICAL: Always release semaphore, even if something fails
        logger.debug(f"Releasing process slot for task {task_id}")
        process_semaphore.release()


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
    # Acquire semaphore slot (wait up to 30 seconds)
    logger.debug(f"Attempting to acquire process slot for task {task_id}")
    acquired = process_semaphore.acquire(blocking=True, timeout=30)

    if not acquired:
        # Could not acquire slot within timeout - worker at capacity
        logger.error(f"Could not acquire process slot for task {task_id} - worker at capacity")
        with status_dict_lock:
            status_dict[task_id].status = "Error"
            status_dict[task_id].error_message = "Worker at capacity, could not start task within 30 seconds"
        return

    logger.debug(f"Acquired process slot for task {task_id}")

    try:
        # Create shared memory objects
        progress = mp_context.Value('i', 0)
        error_message = mp_context.Array('c', 1024)
        q = Queue(maxsize=1)

        args: Tuple[bytes, bytes, List[models.FuzzyMapping], mp_context.Array, str, mp_context.Value, Queue, int, flowfile_node_id_type] = \
            (left_serializable_object, right_serializable_object, fuzzy_maps, error_message, file_ref, progress, q,
             flowfile_flow_id, flowfile_node_id)

        p: Process = mp_context.Process(target=funcs.fuzzy_join_task, args=args)
        p.start()
        logger.debug(f"Started fuzzy process {p.pid} for task {task_id}")

        process_manager.add_process(task_id, p)

        # Monitor the process until completion
        handle_task(task_id=task_id, p=p, progress=progress, error_message=error_message, q=q)

    finally:
        # CRITICAL: Always release semaphore, even if something fails
        logger.debug(f"Releasing process slot for task {task_id}")
        process_semaphore.release()