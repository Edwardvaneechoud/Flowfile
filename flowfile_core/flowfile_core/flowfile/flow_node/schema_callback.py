
from typing import Callable, Any, Optional, Generic, TypeVar
from concurrent.futures import ThreadPoolExecutor, Future
from flowfile_core.configs import logger


T = TypeVar('T')


class SingleExecutionFuture(Generic[T]):
    """Single execution of a function in a separate thread with caching of the result."""
    executor: ThreadPoolExecutor
    future: Optional[Future[T]]
    func: Callable[[], T]
    on_error: Optional[Callable[[Exception], Any]]
    result_value: Optional[T]

    def __init__(
        self,
        func: Callable[[], T],
        on_error: Optional[Callable[[Exception], Any]] = None
    ) -> None:
        """Initialize with function and optional error handler."""
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.future = None
        self.func = func
        self.on_error = on_error
        self.result_value = None

    def start(self) -> None:
        """Start the function execution if not already started."""
        if not self.future:
            logger.info("single executor function started")
            self.future = self.executor.submit(self.func)

    def cleanup(self) -> None:
        """Clean up resources by clearing the future and shutting down the executor."""
        # if self.future:
        #     self.future = None
        self.executor.shutdown(wait=False)

    def __call__(self) -> Optional[T]:
        """Execute function if not running and return its result."""
        if self.result_value:
            return self.result_value
        if not self.future:
            self.start()
        else:
            logger.info("Function already running or did complete")
        try:
            self.result_value = self.future.result()
            logger.info("Done with the function")
            return self.result_value
        except Exception as e:
            if self.on_error:
                return self.on_error(e)
            else:
                raise e
        finally:
            self.cleanup()

    def reset(self):
        """Reset the future and result value."""
        logger.info("Resetting the future and result value")
        self.result_value = None
        self.future = None

    def __del__(self) -> None:
        """Ensure executor is shut down on deletion."""
        self.cleanup()
