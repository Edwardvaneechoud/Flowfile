"""
Concrete implementations of monitoring service components.

This module provides concrete implementations following the monitoring service interfaces.
"""
import time
import psutil
from typing import Dict, List, Optional
from datetime import datetime

from .service_interface import (
    IMonitoringService, 
    ISystemMetricsProvider, 
    IProcessInfoProvider
)
from .models import (
    ProcessInfo, 
    HealthStatus, 
    SystemMetrics, 
    ProcessMetrics, 
    MonitoringOverview,
    ServiceHealth
)
from ..state import WorkerState


class SystemMetricsProvider(ISystemMetricsProvider):
    """Concrete implementation of system metrics provider using psutil."""
    
    def get_cpu_count(self) -> int:
        """Get CPU count."""
        return psutil.cpu_count() or 1  # Default to 1 if None
    
    def get_memory_info(self) -> Dict[str, float]:
        """Get memory information in MB."""
        memory = psutil.virtual_memory()
        return {
            'total': memory.total / (1024 * 1024),  # Convert to MB
            'available': memory.available / (1024 * 1024),
            'used': memory.used / (1024 * 1024),
            'percent': memory.percent
        }
    
    def get_disk_usage(self) -> Optional[float]:
        """Get disk usage percentage."""
        try:
            disk = psutil.disk_usage('/')
            return (disk.used / disk.total) * 100
        except Exception:
            return None


class ProcessInfoProvider(IProcessInfoProvider):
    """Concrete implementation of process info provider using worker state."""
    
    def __init__(self, worker_state: WorkerState):
        """Initialize with worker state dependency."""
        self._worker_state = worker_state
    
    def get_all_processes(self) -> List[ProcessInfo]:
        """Get information about all tracked processes (including completed ones)."""
        processes = []
        
        # Get all tasks from status dict (includes completed tasks)
        all_task_ids = self._worker_state.get_all_task_ids()
        all_process_refs = self._worker_state.get_all_processes()
        
        for task_id in all_task_ids:
            process = all_process_refs.get(task_id)
            status = self._worker_state.get_status(task_id)
            
            process_info = ProcessInfo(
                task_id=task_id,
                pid=process.pid if process and hasattr(process, 'pid') else None,
                status=self._get_process_status(task_id),
                start_time=status.start_time if status else None,
                end_time=status.end_time if status else None
            )
            processes.append(process_info)
        
        return processes
    
    def get_process_count_by_status(self, status: str) -> int:
        """Get count of processes with specific status (case-insensitive)."""
        count = 0
        all_task_ids = self._worker_state.get_all_task_ids()
        status_lower = status.lower()
       
        for task_id in all_task_ids:
            if self._get_process_status(task_id).lower() == status_lower:
                count += 1
        
        return count
    
    def _get_process_status(self, task_id: str) -> str:
        """Get the status of a process from worker state."""
        status = self._worker_state.get_status(task_id)
        return status.status if status else "Unknown"


class MonitoringService(IMonitoringService):
    """Concrete implementation of monitoring service."""
    
    def __init__(
        self, 
        worker_state: WorkerState,
        service_start_time: float,
        service_name: str = "flowfile-worker"
    ):
        """Initialize monitoring service with dependencies."""
        self._worker_state = worker_state
        self._service_start_time = service_start_time
        self._service_name = service_name
        self._system_metrics_provider = SystemMetricsProvider()
        self._process_info_provider = ProcessInfoProvider(worker_state)
    
    def get_health_status(self) -> HealthStatus:
        """Get the overall health status of the service."""
        current_time = time.time()
        uptime_seconds = current_time - self._service_start_time
        
        # Determine health status based on system metrics
        memory_info = self._system_metrics_provider.get_memory_info()
        cpu_count = self._system_metrics_provider.get_cpu_count()
        
        # Simple health determination logic
        # "degraded" means the system is under moderate load (80-90% memory)
        # "unhealthy" means the system is under heavy load (>90% memory)
        health_status = "healthy"
        if memory_info['percent'] > 90:
            health_status = "unhealthy"
        elif memory_info['percent'] >= 80:
            health_status = "degraded"
        
        return HealthStatus(
            status=health_status,
            timestamp=current_time,
            service_name=self._service_name,
            uptime_seconds=uptime_seconds
        )
    
    def get_system_metrics(self) -> SystemMetrics:
        """Get current system resource metrics."""
        current_time = time.time()
        memory_info = self._system_metrics_provider.get_memory_info()
        cpu_count = self._system_metrics_provider.get_cpu_count()
        disk_usage = self._system_metrics_provider.get_disk_usage()
        
        return SystemMetrics(
            timestamp=current_time,
            cpu_count=cpu_count,
            memory_total_mb=memory_info['total'],
            memory_available_mb=memory_info['available'],
            memory_used_mb=memory_info['used'],
            memory_usage_percent=memory_info['percent'],
            disk_usage_percent=disk_usage
        )
    
    def get_process_metrics(self) -> ProcessMetrics:
        """Get process-related metrics."""
        current_time = time.time()
        all_processes = self._process_info_provider.get_all_processes()
        
        # Count processes by status
        running_count = 0
        completed_count = 0
        failed_count = 0
        
        for process in all_processes:
            status_lower = process.status.lower()
            if status_lower in ['starting', 'processing']:
                running_count += 1
            elif status_lower == 'completed':
                completed_count += 1
            elif status_lower in ['error', 'unknown error']:
                failed_count += 1
        
        return ProcessMetrics(
            timestamp=current_time,
            total_processes=len(all_processes),
            running_processes=running_count,
            completed_tasks=completed_count,
            failed_tasks=failed_count,
            processes=all_processes
        )
    
    def get_monitoring_overview(self) -> MonitoringOverview:
        """Get comprehensive monitoring overview."""
        health = self.get_health_status()
        system = self.get_system_metrics()
        processes = self.get_process_metrics()
        
        service_info = {
            "service_name": self._service_name,
            "version": "1.0.0",
            "python_version": "3.x",  # Could be enhanced to get actual version
            "platform": "Flowfile Worker"
        }
        
        return MonitoringOverview(
            health=health,
            system=system,
            processes=processes,
            service_info=service_info
        )
    
    def get_service_health(self) -> ServiceHealth:
        """Get simplified service health status."""
        health_status = self.get_health_status()
        
        is_healthy = health_status.status in ["healthy", "degraded"]
        message = f"Service is {health_status.status}"
        
        return ServiceHealth(
            healthy=is_healthy,
            message=message,
            timestamp=health_status.timestamp
        )
    
    def get_running_processes_count(self) -> int:
        """Get count of currently running processes."""
        return self._process_info_provider.get_process_count_by_status("Processing") + \
               self._process_info_provider.get_process_count_by_status("Starting")