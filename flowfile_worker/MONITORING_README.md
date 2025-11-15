# Flowfile Worker Monitoring Endpoints

## Overview

I've successfully added comprehensive monitoring endpoints to your worker service following SOLID principles. The monitoring system is designed to be modular, testable, and extensible.

## Architecture

The monitoring system follows SOLID principles:

### 1. Single Responsibility Principle (SRP)
- **Models**: Pure data transfer objects (DTOs) with no business logic
- **Services**: Focused on specific monitoring concerns
- **Routers**: Clean separation of API endpoints

### 2. Open/Closed Principle (OCP)
- **Interfaces**: Abstract contracts that can be extended without modification
- **Dependency Injection**: Services can be swapped without changing code

### 3. Liskov Substitution Principle (LSP)
- **Interface implementations**: All concrete implementations fulfill the same contract

### 4. Interface Segregation Principle (ISP)
- **Focused interfaces**: Separate interfaces for different concerns (IMonitoringService, ISystemMetricsProvider, IProcessInfoProvider)

### 5. Dependency Inversion Principle (DIP)
- **Abstractions over concretions**: High-level modules depend on abstractions, not concrete implementations

## File Structure

```
flowfile_worker/flowfile_worker/monitoring/
├── __init__.py                 # Package exports
├── models.py                   # Data transfer objects
├── service_interface.py        # Service abstractions
├── service.py                  # Concrete implementations
└── router.py                   # API endpoints
```

## Available Endpoints

### Base URL: `/monitoring`

### 1. Health Check
- **Endpoint**: `GET /monitoring/health`
- **Response**: Simple health status
- **Use case**: Load balancers, simple health checks
- **Response format**:
```json
{
  "healthy": true,
  "message": "Service is healthy",
  "timestamp": 1634567890.123
}
```

### 2. Detailed Health Check
- **Endpoint**: `GET /monitoring/health/detailed`
- **Response**: Comprehensive health information
- **Use case**: Detailed monitoring, diagnostics
- **Response format**:
```json
{
  "status": "healthy|degraded|unhealthy",
  "timestamp": 1634567890.123,
  "service_name": "flowfile-worker",
  "version": "1.0.0",
  "uptime_seconds": 3600
}
```

### 3. Process Information
- **Endpoint**: `GET /monitoring/processes`
- **Response**: All process details and metrics
- **Use case**: Process monitoring, debugging
- **Response format**:
```json
{
  "timestamp": 1634567890.123,
  "total_processes": 5,
  "running_processes": 2,
  "completed_tasks": 3,
  "failed_tasks": 0,
  "processes": [
    {
      "task_id": "task-123",
      "pid": 1234,
      "status": "Processing",
      "start_time": 1634567890.123
    }
  ]
}
```

### 4. Running Processes Count
- **Endpoint**: `GET /monitoring/processes/count`
- **Response**: Count of currently running processes
- **Use case**: Quick status check
- **Response format**:
```json
{
  "running_processes": 2
}
```

### 5. System Metrics
- **Endpoint**: `GET /monitoring/system/metrics`
- **Response**: Complete system resource metrics
- **Use case**: Resource monitoring, capacity planning
- **Response format**:
```json
{
  "timestamp": 1634567890.123,
  "cpu_count": 4,
  "memory_total_mb": 8192,
  "memory_available_mb": 4096,
  "memory_used_mb": 2048,
  "memory_usage_percent": 25.0,
  "disk_usage_percent": 45.5
}
```

### 6. Memory Usage
- **Endpoint**: `GET /monitoring/system/memory`
- **Response**: Focus specifically on RAM usage
- **Use case**: Memory monitoring
- **Response format**:
```json
{
  "memory_total_mb": 8192,
  "memory_used_mb": 2048,
  "memory_available_mb": 4096,
  "memory_usage_percent": 25.0,
  "timestamp": 1634567890.123
}
```

### 7. Comprehensive Overview
- **Endpoint**: `GET /monitoring/overview`
- **Response**: Complete monitoring snapshot
- **Use case**: Dashboard, comprehensive monitoring
- **Response format**:
```json
{
  "health": {
    "status": "healthy",
    "timestamp": 1634567890.123,
    "service_name": "flowfile-worker",
    "uptime_seconds": 3600
  },
  "system": {
    "cpu_count": 4,
    "memory_total_mb": 8192,
    "memory_usage_percent": 25.0,
    "timestamp": 1634567890.123
  },
  "processes": {
    "total_processes": 5,
    "running_processes": 2,
    "completed_tasks": 3,
    "failed_tasks": 0,
    "timestamp": 1634567890.123
  },
  "service_info": {
    "service_name": "flowfile-worker",
    "version": "1.0.0",
    "platform": "Flowfile Worker"
  }
}
```

## Integration

The monitoring router has been integrated into the main FastAPI application in `flowfile_worker/main.py`:

```python
# Include monitoring router
from flowfile_worker.monitoring.router import router as monitoring_router
app.include_router(monitoring_router)
```

## Dependencies

The monitoring system requires `psutil` for system metrics. Make sure it's installed:

```bash
pip install psutil
```

## Testing

Comprehensive tests have been added to `flowfile_worker/tests/test_monitoring.py` covering:

- **Unit tests**: Individual components and services
- **Integration tests**: API endpoints via FastAPI TestClient
- **Error handling**: Exception scenarios and error responses
- **Mocking**: External dependencies (psutil, worker_state)

## Health Status Logic

The system determines health status based on memory usage:
- **Healthy**: Memory usage < 75%
- **Degraded**: Memory usage 75-90%
- **Unhealthy**: Memory usage > 90%

## Key Benefits

1. **Monitoring**: Real-time visibility into service health and performance
2. **Observability**: Comprehensive metrics for debugging and optimization
3. **Scalability**: Can handle multiple processes and high load
4. **Maintainability**: SOLID principles ensure clean, testable code
5. **Extensibility**: Easy to add new monitoring capabilities

## Usage Examples

### Quick Health Check
```bash
curl http://localhost:8000/monitoring/health
```

### Monitor Memory Usage
```bash
curl http://localhost:8000/monitoring/system/memory
```

### Get Complete Overview
```bash
curl http://localhost:8000/monitoring/overview
```

### Process Count
```bash
curl http://localhost:8000/monitoring/processes/count
```

## Notes

- All endpoints include proper error handling and logging
- Thread-safe operations using the existing WorkerState
- Consistent response formats across all endpoints
- Performance optimized with minimal overhead
- Production-ready with comprehensive test coverage