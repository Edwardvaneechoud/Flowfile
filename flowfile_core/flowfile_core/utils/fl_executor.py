from flowfile_core.configs import logger
from inspect import isfunction
from loky import get_reusable_executor
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
import atexit


# process_executor: Uses loky for process-based parallelism
def process_executor(wait_on_completion: bool = False, max_workers: int = 12):
    max_workers = max_workers if not wait_on_completion else 1

    def executor(f):
        @wraps(f)
        def inner(*args, **kwargs):
            logger.debug(f'Added task {f.__name__} to a process executor')
            logger.debug(f'max_workers: {max_workers}')

            # Create a new executor with the required number of workers
            func_executor = get_reusable_executor(max_workers=max_workers, timeout=2, kill_workers=False, reuse=True)
            r = func_executor.submit(f, *args, **kwargs)
            if wait_on_completion:
                result = r.result()
                logger.info(f'done executing {f.__name__}')
                return result

            logger.info(f'done submitting {f.__name__} to a process executor')
            return r

        return inner

    if isfunction(wait_on_completion):
        f = wait_on_completion
        wait_on_completion = False
        return executor(f)
    return executor


class ExecutorManager:
    def __init__(self):
        self.executor = None

    def get_executor(self, max_workers):
        if self.executor is None or self.executor._max_workers != max_workers:
            if self.executor:
                self.executor.shutdown(wait=False)
            self.executor = ThreadPoolExecutor(max_workers=max_workers)
        return self.executor

    def shutdown(self):
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None


executor_manager = ExecutorManager()


# thread_executor: Uses ThreadPoolExecutor for thread-based parallelism
def thread_executor(wait_on_completion: bool = False, max_workers: int = 12):
    max_workers = max_workers if not wait_on_completion else 1

    def executor(f):
        @wraps(f)
        def inner(*args, **kwargs):
            logger.info(f'Added task {f.__name__} to a thread executor')
            logger.info(f'max_workers: {max_workers}')

            func_executor = executor_manager.get_executor(max_workers)
            r = func_executor.submit(f, *args, **kwargs)

            if wait_on_completion:
                result = r.result()
                logger.info(f'done executing {f.__name__}')
                return result

            logger.info(f'done submitting {f.__name__} to a thread executor')
            return r

        return inner

    if isfunction(wait_on_completion):
        f = wait_on_completion
        wait_on_completion = False
        return executor(f)

    return executor


# Ensure the executor is properly shut down at the end of the program
atexit.register(executor_manager.shutdown)
