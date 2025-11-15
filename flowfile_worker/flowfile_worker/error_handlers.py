"""
Error handling module for Flowfile Worker.

This module provides:
- Custom exception hierarchy for worker errors
- Consistent error message encoding
- Decorator for route error handling
"""
from functools import wraps
from typing import Callable
import logging
from fastapi import HTTPException

logger = logging.getLogger(__name__)


# Custom Exception Hierarchy
class WorkerError(Exception):
    """Base exception for all worker-related errors."""
    pass


class TaskProcessingError(WorkerError):
    """Error that occurs during task processing."""
    pass


class ResourceExhaustedError(WorkerError):
    """Error when worker resources are exhausted."""
    pass


class TaskNotFoundError(WorkerError):
    """Error when a requested task cannot be found."""
    pass


class InvalidResultError(WorkerError):
    """Error when task result is invalid or corrupted."""
    pass


class ConfigurationError(WorkerError):
    """Error in worker configuration."""
    pass


# Error Message Encoding Helper
def encode_error_message(error: Exception, max_length: int = 1024) -> bytes:
    """
    Consistently encode error messages with maximum length.
    
    Args:
        error: The exception to encode
        max_length: Maximum length of encoded message in bytes
        
    Returns:
        Encoded error message truncated to max_length
    """
    error_str = str(error)
    return error_str.encode('utf-8')[:max_length]


# Route Error Handler Decorator
def handle_route_errors(func: Callable) -> Callable:
    """
    Decorator for consistent route error handling.
    
    Catches WorkerError exceptions and converts them to appropriate HTTP responses.
    Logs all errors with full context.
    
    Args:
        func: The route function to wrap
        
    Returns:
        Wrapped function with error handling
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except TaskNotFoundError as e:
            logger.warning(f"Task not found: {str(e)}")
            raise HTTPException(status_code=404, detail=str(e))
        except ResourceExhaustedError as e:
            logger.error(f"Resource exhausted: {str(e)}")
            raise HTTPException(status_code=503, detail=str(e))
        except WorkerError as e:
            logger.error(f"Worker error in {func.__name__}: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal server error")
    
    return wrapper


# Synchronous version for non-async routes
def handle_route_errors_sync(func: Callable) -> Callable:
    """
    Decorator for consistent route error handling (synchronous version).
    
    Args:
        func: The route function to wrap
        
    Returns:
        Wrapped function with error handling
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TaskNotFoundError as e:
            logger.warning(f"Task not found: {str(e)}")
            raise HTTPException(status_code=404, detail=str(e))
        except ResourceExhaustedError as e:
            logger.error(f"Resource exhausted: {str(e)}")
            raise HTTPException(status_code=503, detail=str(e))
        except WorkerError as e:
            logger.error(f"Worker error in {func.__name__}: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal server error")
    
    return wrapper