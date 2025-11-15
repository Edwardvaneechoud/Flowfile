"""
Monitoring router for Flowfile Worker API.

This module defines monitoring endpoints for health checks, process tracking,
and system resource monitoring.
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict

from .service_interface import IMonitoringService
from .models import (
    HealthStatus, 
    SystemMetrics, 
    ProcessMetrics, 
    MonitoringOverview,
    ServiceHealth
)
from ..configs import logger


# Global dependency to get monitoring service
_monitoring_service_instance = None

def get_monitoring_service() -> IMonitoringService:
    """Dependency to get monitoring service instance."""
    global _monitoring_service_instance
    if _monitoring_service_instance is None:
        from .. import worker_state  # Import here to avoid circular imports
        from .service import MonitoringService
        import time
        _monitoring_service_instance = MonitoringService(
            worker_state=worker_state,
            service_start_time=time.time()
        )
    return _monitoring_service_instance


router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/health", response_model=ServiceHealth)
async def health_check(monitoring_service: IMonitoringService = Depends(get_monitoring_service)):
    """
    Simple health check endpoint.
    
    Returns a basic health status indicating if the service is operational.
    This is the simplest endpoint for load balancers and health checks.
    
    Returns:
        ServiceHealth object with health status and timestamp
    """
    logger.debug("Health check requested")
    try:
        return monitoring_service.get_service_health()
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@router.get("/health/detailed", response_model=HealthStatus)
async def detailed_health_check(monitoring_service: IMonitoringService = Depends(get_monitoring_service)):
    """
    Detailed health check with service information.
    
    Returns comprehensive health status including uptime and service details.
    
    Returns:
        HealthStatus object with detailed health information
    """
    logger.debug("Detailed health check requested")
    try:
        return monitoring_service.get_health_status()
    except Exception as e:
        logger.error(f"Detailed health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@router.get("/processes", response_model=ProcessMetrics)
async def get_processes_info(monitoring_service: IMonitoringService = Depends(get_monitoring_service)):
    """
    Get information about all running processes.
    
    Returns detailed information about all tracked processes including:
    - Total process count
    - Running, completed, and failed task counts
    - Individual process information
    
    Returns:
        ProcessMetrics object with process information
    """
    logger.debug("Process information requested")
    try:
        return monitoring_service.get_process_metrics()
    except Exception as e:
        logger.error(f"Failed to get process information: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get process information")


@router.get("/processes/count")
async def get_running_processes_count(monitoring_service: IMonitoringService = Depends(get_monitoring_service)) -> Dict:
    """
    Get count of currently running processes.
    
    Simple endpoint that returns the number of active/running processes.
    
    Returns:
        Dictionary with running_processes count
    """
    logger.debug("Running processes count requested")
    try:
        count = monitoring_service.get_running_processes_count()
        return {"running_processes": count}
    except Exception as e:
        logger.error(f"Failed to get running processes count: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get process count")


@router.get("/system/metrics", response_model=SystemMetrics)
async def get_system_metrics(monitoring_service: IMonitoringService = Depends(get_monitoring_service)):
    """
    Get current system resource metrics.
    
    Returns system-level metrics including:
    - CPU information
    - Memory usage (total, available, used, percentage)
    - Disk usage (if available)
    
    Returns:
        SystemMetrics object with resource usage information
    """
    logger.debug("System metrics requested")
    try:
        return monitoring_service.get_system_metrics()
    except Exception as e:
        logger.error(f"Failed to get system metrics: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get system metrics")


@router.get("/system/memory")
async def get_memory_usage(monitoring_service: IMonitoringService = Depends(get_monitoring_service)) -> Dict:
    """
    Get current memory usage information.
    
    Simplified endpoint focused specifically on memory/RAM usage.
    
    Returns:
        Dictionary with memory usage details
    """
    logger.debug("Memory usage requested")
    try:
        system_metrics = monitoring_service.get_system_metrics()
        return {
            "memory_total_mb": system_metrics.memory_total_mb,
            "memory_used_mb": system_metrics.memory_used_mb,
            "memory_available_mb": system_metrics.memory_available_mb,
            "memory_usage_percent": system_metrics.memory_usage_percent,
            "timestamp": system_metrics.timestamp
        }
    except Exception as e:
        logger.error(f"Failed to get memory usage: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get memory usage")


@router.get("/overview", response_model=MonitoringOverview)
async def get_monitoring_overview(monitoring_service: IMonitoringService = Depends(get_monitoring_service)):
    """
    Get comprehensive monitoring overview.
    
    Returns a complete snapshot of all monitoring data including:
    - Health status
    - System metrics
    - Process information
    - Service details
    
    This endpoint provides a complete picture of the service status.
    
    Returns:
        MonitoringOverview object with complete monitoring data
    """
    logger.debug("Monitoring overview requested")
    try:
        return monitoring_service.get_monitoring_overview()
    except Exception as e:
        logger.error(f"Failed to get monitoring overview: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get monitoring overview")