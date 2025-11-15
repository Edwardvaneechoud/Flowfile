"""
Monitoring service interfaces for Flowfile Worker.

This module defines abstractions for monitoring operations to enable
dependency inversion and testability.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime
import psutil

from .models import (
    ProcessInfo, 
    HealthStatus, 
    SystemMetrics, 
    ProcessMetrics, 
    MonitoringOverview,
    ServiceHealth
)


class IMonitoringService(ABC):
    """Interface for monitoring service operations."""
    
    @abstractmethod
    def get_health_status(self) -> HealthStatus:
        """Get the overall health status of the service."""
        pass
    
    @abstractmethod
    def get_system_metrics(self) -> SystemMetrics:
        """Get current system resource metrics."""
        pass
    
    @abstractmethod
    def get_process_metrics(self) -> ProcessMetrics:
        """Get process-related metrics."""
        pass
    
    @abstractmethod
    def get_monitoring_overview(self) -> MonitoringOverview:
        """Get comprehensive monitoring overview."""
        pass
    
    @abstractmethod
    def get_service_health(self) -> ServiceHealth:
        """Get simplified service health status."""
        pass
    
    @abstractmethod
    def get_running_processes_count(self) -> int:
        """Get count of currently running processes."""
        pass


class ISystemMetricsProvider(ABC):
    """Interface for system metrics providers."""
    
    @abstractmethod
    def get_cpu_count(self) -> int:
        """Get CPU count."""
        pass
    
    @abstractmethod
    def get_memory_info(self) -> Dict[str, float]:
        """Get memory information in MB."""
        pass
    
    @abstractmethod
    def get_disk_usage(self) -> Optional[float]:
        """Get disk usage percentage."""
        pass


class IProcessInfoProvider(ABC):
    """Interface for process information providers."""
    
    @abstractmethod
    def get_all_processes(self) -> List[ProcessInfo]:
        """Get information about all tracked processes."""
        pass
    
    @abstractmethod
    def get_process_count_by_status(self, status: str) -> int:
        """Get count of processes with specific status."""
        pass