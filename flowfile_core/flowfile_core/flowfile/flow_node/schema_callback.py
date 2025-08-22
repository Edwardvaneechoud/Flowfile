from typing import Callable, Any, Optional, Generic, TypeVar
from concurrent.futures import ThreadPoolExecutor, Future
import threading
from flowfile_core.configs import logger

T = TypeVar('T')


class SingleExecutionFuture(Generic[T]):
    """Thread-safe single execution of a function with result caching.

    Ensures a function is executed at most once even when called from multiple threads.
    Subsequent calls return the cached result.
    """

    func: Callable[[], T]
    on_error: Optional[Callable[[Exception], Any]]
    _lock: threading.RLock
    _executor: Optional[ThreadPoolExecutor]
    _future: Optional[Future[T]]
    _result_value: Optional[T]
    _exception: Optional[Exception]
    _has_completed: bool
    _has_started: bool

    def __init__(
            self,
            func: Callable[[], T],
            on_error: Optional[Callable[[Exception], Any]] = None
    ) -> None:
        """Initialize with function and optional error handler."""
        self.func = func
        self.on_error = on_error

        # Thread safety
        self._lock = threading.RLock()  # RLock allows re-entrant locking

        # Execution state
        self._executor = None
        self._future = None
        self._result_value = None
        self._exception = None
        self._has_completed = False
        self._has_started = False

    def _ensure_executor(self) -> ThreadPoolExecutor:
        """Ensure executor exists, creating if necessary."""
        if self._executor is None or self._executor._shutdown:
            self._executor = ThreadPoolExecutor(max_workers=1)
        return self._executor

    def start(self) -> None:
        """Start the function execution if not already started."""
        with self._lock:
            if self._has_started:
                logger.info("Function already started or completed")
                return

            logger.info("Starting single executor function")
            executor: ThreadPoolExecutor = self._ensure_executor()
            self._future = executor.submit(self._func_wrapper)
            self._has_started = True

    def _func_wrapper(self) -> T:
        """Wrapper to capture the result or exception."""
        try:
            result: T = self.func()
            with self._lock:
                self._result_value = result
                self._has_completed = True
            return result
        except Exception as e:
            with self._lock:
                self._exception = e
                self._has_completed = True
            raise

    def cleanup(self) -> None:
        """Clean up resources by shutting down the executor."""
        with self._lock:
            if self._executor and not self._executor._shutdown:
                self._executor.shutdown(wait=False)

    def __call__(self) -> Optional[T]:
        """Execute function if not running and return its result."""
        with self._lock:
            # If already completed, return cached result or raise cached exception
            if self._has_completed:
                if self._exception:
                    if self.on_error:
                        return self.on_error(self._exception)
                    else:
                        raise self._exception
                return self._result_value

            # Start if not already started
            if not self._has_started:
                self.start()

        # Wait for completion outside the lock to avoid blocking other threads
        if self._future:
            try:
                result: T = self._future.result()
                logger.info("Function completed successfully")
                return result
            except Exception as e:
                logger.error(f"Function raised exception: {e}")
                if self.on_error:
                    return self.on_error(e)
                else:
                    raise

        return None

    def reset(self) -> None:
        """Reset the execution state, allowing the function to be run again."""
        with self._lock:
            logger.info("Resetting single execution future")

            # Cancel any pending execution
            if self._future and not self._future.done():
                self._future.cancel()

            # Clean up old executor
            if self._executor and not self._executor._shutdown:
                self._executor.shutdown(wait=False)

            # Reset state
            self._executor = None
            self._future = None
            self._result_value = None
            self._exception = None
            self._has_completed = False
            self._has_started = False

    def is_running(self) -> bool:
        """Check if the function is currently executing."""
        with self._lock:
            return bool(
                self._has_started and
                not self._has_completed and
                self._future is not None and
                not self._future.done()
            )

    def is_completed(self) -> bool:
        """Check if the function has completed execution."""
        with self._lock:
            return self._has_completed

    def get_result(self) -> Optional[T]:
        """Get the cached result without triggering execution."""
        with self._lock:
            if self._exception:
                if self.on_error:
                    return self.on_error(self._exception)
                else:
                    raise self._exception
            return self._result_value

    def __del__(self) -> None:
        """Ensure executor is shut down on deletion."""
        try:
            self.cleanup()
        except Exception:
            pass
