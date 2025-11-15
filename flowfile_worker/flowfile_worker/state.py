"""
State management module for Flowfile Worker.

This module provides thread-safe state management for worker tasks,
replacing global mutable dictionaries with a proper state manager.
"""
from typing import Dict, List, Optional, Any
import threading
import multiprocessing
from flowfile_worker.models import Status


class WorkerState:
    """
    Thread-safe state manager for worker tasks.
    
    Manages:
    - Task status tracking
    - Process references
    - Memory usage tracking
    
    All methods use internal locking for thread safety.
    Uses RLock to support nested locking scenarios.
    """
    
    def __init__(self):
        """Initialize the worker state with empty dictionaries and lock."""
        self._status_dict: Dict[str, Status] = {}
        self._process_dict: Dict[str, multiprocessing.Process] = {}
        self._memory_usage: Dict[str, float] = {}
        self._lock = threading.RLock()  # RLock allows nested locking
    
    # Status management methods
    def get_status(self, task_id: str) -> Optional[Status]:
        """
        Get the status of a task.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            Status object if found, None otherwise
        """
        with self._lock:
            return self._status_dict.get(task_id)
    
    def set_status(self, task_id: str, status: Status) -> None:
        """
        Set or update the status of a task.
        
        Args:
            task_id: Unique identifier for the task
            status: Status object to store
        """
        with self._lock:
            self._status_dict[task_id] = status
    
    def update_status_field(self, task_id: str, field: str, value: Any) -> None:
        """
        Update a specific field of a task's status.
        
        Args:
            task_id: Unique identifier for the task
            field: Name of the field to update
            value: New value for the field
            
        Raises:
            KeyError: If task_id not found
        """
        with self._lock:
            if task_id not in self._status_dict:
                raise KeyError(f"Task {task_id} not found")
            setattr(self._status_dict[task_id], field, value)
    
    def remove_status(self, task_id: str) -> Optional[Status]:
        """
        Remove a task's status.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            The removed Status object, or None if not found
        """
        with self._lock:
            return self._status_dict.pop(task_id, None)
    
    def get_all_task_ids(self) -> List[str]:
        """
        Get all task IDs currently tracked.
        
        Returns:
            List of task IDs
        """
        with self._lock:
            return list(self._status_dict.keys())
    
    def task_exists(self, task_id: str) -> bool:
        """
        Check if a task exists.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            True if task exists, False otherwise
        """
        with self._lock:
            return task_id in self._status_dict
    
    # Process management methods
    def get_process(self, task_id: str) -> Optional[multiprocessing.Process]:
        """
        Get the process for a task.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            Process object if found, None otherwise
        """
        with self._lock:
            return self._process_dict.get(task_id)
    
    def set_process(self, task_id: str, process: multiprocessing.Process) -> None:
        """
        Store a process reference for a task.
        
        Args:
            task_id: Unique identifier for the task
            process: Process object to store
        """
        with self._lock:
            self._process_dict[task_id] = process
    
    def remove_process(self, task_id: str) -> Optional[multiprocessing.Process]:
        """
        Remove a process reference.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            The removed Process object, or None if not found
        """
        with self._lock:
            return self._process_dict.pop(task_id, None)
    
    def get_all_processes(self) -> Dict[str, multiprocessing.Process]:
        """
        Get all process references.
        
        Returns:
            Dictionary of task_id to Process mappings (copy)
        """
        with self._lock:
            return self._process_dict.copy()
    
    # Memory usage tracking methods
    def get_memory_usage(self, task_id: str) -> Optional[float]:
        """
        Get memory usage for a task.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            Memory usage in bytes if found, None otherwise
        """
        with self._lock:
            return self._memory_usage.get(task_id)
    
    def set_memory_usage(self, task_id: str, usage: float) -> None:
        """
        Store memory usage for a task.
        
        Args:
            task_id: Unique identifier for the task
            usage: Memory usage in bytes
        """
        with self._lock:
            self._memory_usage[task_id] = usage
    
    def remove_memory_usage(self, task_id: str) -> Optional[float]:
        """
        Remove memory usage tracking for a task.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            The removed memory usage value, or None if not found
        """
        with self._lock:
            return self._memory_usage.pop(task_id, None)
    
    # Composite operations for atomicity
    def remove_task(self, task_id: str) -> None:
        """
        Remove all state for a task atomically.
        
        This includes status, process reference, and memory usage.
        
        Args:
            task_id: Unique identifier for the task
        """
        with self._lock:
            self._status_dict.pop(task_id, None)
            self._process_dict.pop(task_id, None)
            self._memory_usage.pop(task_id, None)
    
    def get_task_count(self) -> int:
        """
        Get the total number of tracked tasks.
        
        Returns:
            Number of tasks
        """
        with self._lock:
            return len(self._status_dict)
    
    def clear_all(self) -> None:
        """
        Clear all state. Use with caution!
        
        This is primarily for testing or shutdown scenarios.
        """
        with self._lock:
            self._status_dict.clear()
            self._process_dict.clear()
            self._memory_usage.clear()