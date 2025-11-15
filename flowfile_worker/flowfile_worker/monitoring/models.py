"""
Monitoring models for Flowfile Worker.

This module defines data transfer objects (DTOs) for monitoring endpoints.
"""
from typing import List, Dict, Optional
from pydantic import BaseModel


class ProcessInfo(BaseModel):
    """Information about a running process."""
    task_id: str
    pid: Optional[int] = None
    status: str
    start_time: Optional[float] = None


class HealthStatus(BaseModel):
    """Health status response model."""
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: float
    service_name: str
    version: str = "1.0.0"
    uptime_seconds: float


class SystemMetrics(BaseModel):
    """System metrics response model."""
    timestamp: float
    cpu_count: int
    memory_total_mb: float
    memory_available_mb: float
    memory_used_mb: float
    memory_usage_percent: float
    disk_usage_percent: Optional[float] = None


class ProcessMetrics(BaseModel):
    """Process-related metrics."""
    timestamp: float
    total_processes: int
    running_processes: int
    completed_tasks: int
    failed_tasks: int
    processes: List[ProcessInfo]


class MonitoringOverview(BaseModel):
    """Comprehensive monitoring overview."""
    health: HealthStatus
    system: SystemMetrics
    processes: ProcessMetrics
    service_info: Dict[str, str]


class ServiceHealth(BaseModel):
    """Simplified health check response."""
    healthy: bool
    message: str
    timestamp: float