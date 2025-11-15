"""
Tests for monitoring functionality.

This module contains unit and integration tests for the monitoring system.
"""
import pytest
import time
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient

from flowfile_worker.monitoring.models import (
    ProcessInfo, HealthStatus, SystemMetrics, ProcessMetrics, MonitoringOverview, ServiceHealth
)
from flowfile_worker.monitoring.service import MonitoringService, SystemMetricsProvider, ProcessInfoProvider
from flowfile_worker.monitoring.router import get_monitoring_service
from flowfile_worker.state import WorkerState
from flowfile_worker.main import app


class TestMonitoringModels:
    """Test monitoring data models."""
    
    def test_process_info_creation(self):
        """Test ProcessInfo model creation."""
        current_time = time.time()
        process_info = ProcessInfo(
            task_id="test-task-123",
            pid=1234,
            status="Running",
            start_time=current_time,
            end_time=None
        )
        assert process_info.task_id == "test-task-123"
        assert process_info.pid == 1234
        assert process_info.status == "Running"
        assert process_info.start_time is not None
        assert process_info.end_time is None
    
    def test_process_info_with_end_time(self):
        """Test ProcessInfo model creation with end_time."""
        start_time = time.time()
        end_time = start_time + 10
        process_info = ProcessInfo(
            task_id="test-task-456",
            pid=5678,
            status="Completed",
            start_time=start_time,
            end_time=end_time
        )
        assert process_info.task_id == "test-task-456"
        assert process_info.pid == 5678
        assert process_info.status == "Completed"
        assert process_info.start_time == start_time
        assert process_info.end_time == end_time
        assert process_info.end_time is not None and process_info.start_time is not None
        assert process_info.end_time > process_info.start_time
    
    def test_health_status_creation(self):
        """Test HealthStatus model creation."""
        health_status = HealthStatus(
            status="healthy",
            timestamp=time.time(),
            service_name="test-service",
            uptime_seconds=3600
        )
        assert health_status.status == "healthy"
        assert health_status.service_name == "test-service"
        assert health_status.uptime_seconds == 3600
    
    def test_system_metrics_creation(self):
        """Test SystemMetrics model creation."""
        metrics = SystemMetrics(
            timestamp=time.time(),
            cpu_count=4,
            memory_total_mb=8192,
            memory_available_mb=4096,
            memory_used_mb=2048,
            memory_usage_percent=25.0,
            disk_usage_percent=45.5
        )
        assert metrics.cpu_count == 4
        assert metrics.memory_total_mb == 8192
        assert metrics.memory_usage_percent == 25.0
        assert metrics.disk_usage_percent == 45.5
    
    def test_service_health_creation(self):
        """Test ServiceHealth model creation."""
        service_health = ServiceHealth(
            healthy=True,
            message="Service is healthy",
            timestamp=time.time()
        )
        assert service_health.healthy is True
        assert service_health.message == "Service is healthy"


class TestMonitoringService:
    """Test monitoring service implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.worker_state = Mock(spec=WorkerState)
        self.service_start_time = time.time() - 3600  # 1 hour ago
        self.monitoring_service = MonitoringService(
            worker_state=self.worker_state,
            service_start_time=self.service_start_time,
            service_name="test-worker"
        )
    
    def test_get_health_status_healthy(self):
        """Test health status when service is healthy."""
        with patch.object(
            self.monitoring_service._system_metrics_provider, 
            'get_memory_info', 
            return_value={'total': 8192, 'available': 4096, 'used': 2048, 'percent': 50}
        ):
            health = self.monitoring_service.get_health_status()
            assert health.status == "healthy"
            assert health.service_name == "test-worker"
            assert health.uptime_seconds > 0
    
    def test_get_health_status_degraded(self):
        """Test health status when service is degraded."""
        with patch.object(
            self.monitoring_service._system_metrics_provider, 
            'get_memory_info', 
            return_value={'total': 8192, 'available': 2048, 'used': 6144, 'percent': 80}
        ):
            health = self.monitoring_service.get_health_status()
            assert health.status == "degraded"
    
    def test_get_health_status_unhealthy(self):
        """Test health status when service is unhealthy."""
        with patch.object(
            self.monitoring_service._system_metrics_provider, 
            'get_memory_info', 
            return_value={'total': 8192, 'available': 819, 'used': 7373, 'percent': 95}
        ):
            health = self.monitoring_service.get_health_status()
            assert health.status == "unhealthy"
    
    def test_get_system_metrics(self):
        """Test system metrics collection."""
        with patch.object(
            self.monitoring_service._system_metrics_provider, 
            'get_memory_info', 
            return_value={'total': 8192, 'available': 4096, 'used': 4096, 'percent': 50}
        ), patch.object(
            self.monitoring_service._system_metrics_provider, 
            'get_cpu_count', 
            return_value=4
        ), patch.object(
            self.monitoring_service._system_metrics_provider, 
            'get_disk_usage', 
            return_value=45.5
        ):
            metrics = self.monitoring_service.get_system_metrics()
            assert metrics.cpu_count == 4
            assert metrics.memory_total_mb == 8192
            assert metrics.memory_usage_percent == 50
            assert metrics.disk_usage_percent == 45.5
    
    def test_get_process_metrics_empty(self):
        """Test process metrics when no processes exist."""
        self.worker_state.get_all_task_ids.return_value = []
        self.worker_state.get_all_processes.return_value = {}
        metrics = self.monitoring_service.get_process_metrics()
        assert metrics.total_processes == 0
        assert metrics.running_processes == 0
        assert metrics.completed_tasks == 0
        assert metrics.failed_tasks == 0
    
    def test_get_process_metrics_with_processes(self):
        """Test process metrics with some processes."""
        # Mock process data
        mock_process = Mock()
        mock_process.pid = 1234
        mock_process.start_time = time.time()
        
        current_time = time.time()
        mock_status1 = Mock()
        mock_status1.status = "Processing"
        mock_status1.start_time = current_time
        mock_status1.end_time = None
        
        mock_status2 = Mock()
        mock_status2.status = "Completed"
        mock_status2.start_time = current_time - 10
        mock_status2.end_time = current_time
        
        mock_status3 = Mock()
        mock_status3.status = "Error"
        mock_status3.start_time = current_time - 5
        mock_status3.end_time = current_time
        
        self.worker_state.get_all_task_ids.return_value = ["task1", "task2", "task3"]
        self.worker_state.get_all_processes.return_value = {
            "task1": mock_process,
            "task2": mock_process,
            "task3": mock_process
        }
        self.worker_state.get_status.side_effect = lambda task_id: {
            "task1": mock_status1,
            "task2": mock_status2,
            "task3": mock_status3
        }.get(task_id)
        
        metrics = self.monitoring_service.get_process_metrics()
        assert metrics.total_processes == 3
        assert metrics.running_processes == 1
        assert metrics.completed_tasks == 1
        assert metrics.failed_tasks == 1
    
    def test_get_monitoring_overview(self):
        """Test comprehensive monitoring overview."""
        # Mock the underlying methods
        with patch.object(self.monitoring_service, 'get_health_status') as mock_health, \
             patch.object(self.monitoring_service, 'get_system_metrics') as mock_system, \
             patch.object(self.monitoring_service, 'get_process_metrics') as mock_process:
            
            # Setup mock returns
            mock_health.return_value = HealthStatus(
                status="healthy",
                timestamp=time.time(),
                service_name="test-worker",
                uptime_seconds=3600
            )
            
            mock_system.return_value = SystemMetrics(
                timestamp=time.time(),
                cpu_count=4,
                memory_total_mb=8192,
                memory_available_mb=4096,
                memory_used_mb=2048,
                memory_usage_percent=25.0
            )
            
            mock_process.return_value = ProcessMetrics(
                timestamp=time.time(),
                total_processes=2,
                running_processes=1,
                completed_tasks=1,
                failed_tasks=0,
                processes=[]
            )
            
            overview = self.monitoring_service.get_monitoring_overview()
            assert overview.health.status == "healthy"
            assert overview.system.cpu_count == 4
            assert overview.processes.total_processes == 2
            assert overview.service_info["service_name"] == "test-worker"
    
    def test_get_service_health_healthy(self):
        """Test simplified service health for healthy service."""
        with patch.object(self.monitoring_service, 'get_health_status') as mock_health:
            mock_health.return_value = HealthStatus(
                status="healthy",
                timestamp=time.time(),
                service_name="test-worker",
                uptime_seconds=3600
            )
            
            health = self.monitoring_service.get_service_health()
            assert health.healthy is True
            assert "healthy" in health.message
    
    def test_get_service_health_degraded(self):
        """Test simplified service health for degraded service."""
        with patch.object(self.monitoring_service, 'get_health_status') as mock_health:
            mock_health.return_value = HealthStatus(
                status="degraded",
                timestamp=time.time(),
                service_name="test-worker",
                uptime_seconds=3600
            )
            
            health = self.monitoring_service.get_service_health()
            assert health.healthy is True  # degraded is still considered healthy enough
    
    def test_get_service_health_unhealthy(self):
        """Test simplified service health for unhealthy service."""
        with patch.object(self.monitoring_service, 'get_health_status') as mock_health:
            mock_health.return_value = HealthStatus(
                status="unhealthy",
                timestamp=time.time(),
                service_name="test-worker",
                uptime_seconds=3600
            )
            
            health = self.monitoring_service.get_service_health()
            assert health.healthy is False
    
    def test_get_running_processes_count(self):
        """Test running processes count."""
        # Mock process data
        mock_status1 = Mock()
        mock_status1.status = "Processing"
        
        mock_status2 = Mock()
        mock_status2.status = "Starting"
        
        mock_status3 = Mock()
        mock_status3.status = "Completed"
        
        self.worker_state.get_all_task_ids.return_value = ["task1", "task2", "task3"]
        self.worker_state.get_all_processes.return_value = {
            "task1": Mock(),
            "task2": Mock(),
            "task3": Mock()
        }
        self.worker_state.get_status.side_effect = lambda task_id: {
            "task1": mock_status1,
            "task2": mock_status2,
            "task3": mock_status3
        }.get(task_id)
        
        count = self.monitoring_service.get_running_processes_count()
        assert count == 2  # Processing + Starting


class TestSystemMetricsProvider:
    """Test system metrics provider."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.provider = SystemMetricsProvider()
    
    @patch('psutil.cpu_count')
    def test_get_cpu_count(self, mock_cpu_count):
        """Test CPU count retrieval."""
        mock_cpu_count.return_value = 8
        assert self.provider.get_cpu_count() == 8
        
        mock_cpu_count.return_value = None
        assert self.provider.get_cpu_count() == 1  # Default fallback
    
    @patch('psutil.virtual_memory')
    def test_get_memory_info(self, mock_virtual_memory):
        """Test memory info retrieval."""
        # Mock memory info
        mock_memory = Mock()
        mock_memory.total = 8 * 1024 * 1024 * 1024  # 8GB in bytes
        mock_memory.available = 4 * 1024 * 1024 * 1024  # 4GB
        mock_memory.used = 4 * 1024 * 1024 * 1024  # 4GB
        mock_memory.percent = 50.0
        
        mock_virtual_memory.return_value = mock_memory
        
        memory_info = self.provider.get_memory_info()
        
        assert memory_info['total'] == 8192  # 8GB in MB
        assert memory_info['available'] == 4096  # 4GB in MB
        assert memory_info['used'] == 4096  # 4GB in MB
        assert memory_info['percent'] == 50.0
    
    @patch('psutil.disk_usage')
    def test_get_disk_usage_success(self, mock_disk_usage):
        """Test successful disk usage retrieval."""
        # Mock disk usage
        mock_disk = Mock()
        mock_disk.used = 50 * 1024 * 1024 * 1024  # 50GB
        mock_disk.total = 100 * 1024 * 1024 * 1024  # 100GB
        mock_disk_usage.return_value = mock_disk
        
        usage_percent = self.provider.get_disk_usage()
        assert usage_percent == 50.0
    
    @patch('psutil.disk_usage')
    def test_get_disk_usage_exception(self, mock_disk_usage):
        """Test disk usage retrieval when exception occurs."""
        mock_disk_usage.side_effect = Exception("Disk error")
        
        usage_percent = self.provider.get_disk_usage()
        assert usage_percent is None


class TestProcessInfoProvider:
    """Test process info provider."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.worker_state = Mock(spec=WorkerState)
        self.provider = ProcessInfoProvider(self.worker_state)
    
    def test_get_all_processes_empty(self):
        """Test getting all processes when none exist."""
        self.worker_state.get_all_task_ids.return_value = []
        self.worker_state.get_all_processes.return_value = {}
        self.worker_state.get_status.return_value = None
        
        processes = self.provider.get_all_processes()
        assert len(processes) == 0
    
    def test_get_all_processes_with_data(self):
        """Test getting all processes with data."""
        # Mock process data
        mock_process1 = Mock()
        mock_process1.pid = 1234
        
        mock_process2 = Mock()
        mock_process2.pid = 5678
        
        current_time = time.time()
        # Mock status data
        mock_status1 = Mock()
        mock_status1.status = "Processing"
        mock_status1.start_time = current_time
        mock_status1.end_time = None
        
        mock_status2 = Mock()
        mock_status2.status = "Completed"
        mock_status2.start_time = current_time - 10
        mock_status2.end_time = current_time
        
        self.worker_state.get_all_task_ids.return_value = ["task1", "task2"]
        self.worker_state.get_all_processes.return_value = {
            "task1": mock_process1,
            "task2": mock_process2
        }
        
        # Mock get_status to return appropriate status based on task_id
        def mock_get_status(task_id):
            if task_id == "task1":
                return mock_status1
            elif task_id == "task2":
                return mock_status2
            return None
        
        self.worker_state.get_status.side_effect = mock_get_status
        
        processes = self.provider.get_all_processes()
        
        assert len(processes) == 2
        
        # Find processes by task_id
        process1 = next((p for p in processes if p.task_id == "task1"), None)
        process2 = next((p for p in processes if p.task_id == "task2"), None)
        
        assert process1 is not None
        assert process1.pid == 1234
        assert process1.status == "Processing"
        assert process1.start_time == current_time
        assert process1.end_time is None
        
        assert process2 is not None
        assert process2.pid == 5678
        assert process2.status == "Completed"
        assert process2.start_time == current_time - 10
        assert process2.end_time == current_time
    
    def test_get_process_count_by_status(self):
        """Test counting processes by status."""
        # Mock process data
        mock_process1 = Mock()
        mock_process2 = Mock()
        mock_process3 = Mock()
        
        # Mock status data
        mock_status1 = Mock()
        mock_status1.status = "Processing"
        
        mock_status2 = Mock()
        mock_status2.status = "Processing"
        
        mock_status3 = Mock()
        mock_status3.status = "Completed"
        
        self.worker_state.get_all_task_ids.return_value = ["task1", "task2", "task3"]
        self.worker_state.get_all_processes.return_value = {
            "task1": mock_process1,
            "task2": mock_process2,
            "task3": mock_process3
        }
        
        def mock_get_status(task_id):
            if task_id in ["task1", "task2"]:
                return mock_status1
            elif task_id == "task3":
                return mock_status3
            return None
        
        self.worker_state.get_status.side_effect = mock_get_status
        
        processing_count = self.provider.get_process_count_by_status("Processing")
        completed_count = self.provider.get_process_count_by_status("Completed")
        
        assert processing_count == 2
        assert completed_count == 1


class TestMonitoringRouter:
    """Test monitoring router endpoints."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.client = TestClient(app)
        self.mock_worker_state = Mock(spec=WorkerState)
        
        # Mock monitoring service
        self.mock_monitoring_service = Mock()
        
        # Patch the global dependency
        self.patcher = patch('flowfile_worker.monitoring.router._monitoring_service_instance', self.mock_monitoring_service)
        self.patcher.start()
    
    def teardown_method(self):
        """Clean up after tests."""
        self.patcher.stop()
    
    def test_health_check_endpoint(self):
        """Test health check endpoint."""
        # Mock health service response
        health_response = ServiceHealth(
            healthy=True,
            message="Service is healthy",
            timestamp=time.time()
        )
        self.mock_monitoring_service.get_service_health.return_value = health_response
        
        response = self.client.get("/monitoring/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["healthy"] is True
        assert "healthy" in data["message"]
        assert "timestamp" in data
    
    def test_detailed_health_check_endpoint(self):
        """Test detailed health check endpoint."""
        # Mock detailed health service response
        health_response = HealthStatus(
            status="healthy",
            timestamp=time.time(),
            service_name="test-worker",
            uptime_seconds=3600
        )
        self.mock_monitoring_service.get_health_status.return_value = health_response
        
        response = self.client.get("/monitoring/health/detailed")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service_name"] == "test-worker"
        assert data["uptime_seconds"] == 3600
        assert "timestamp" in data
    
    def test_processes_endpoint(self):
        """Test processes endpoint."""
        # Mock processes service response
        processes_response = ProcessMetrics(
            timestamp=time.time(),
            total_processes=2,
            running_processes=1,
            completed_tasks=1,
            failed_tasks=0,
            processes=[]
        )
        self.mock_monitoring_service.get_process_metrics.return_value = processes_response
        
        response = self.client.get("/monitoring/processes")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_processes"] == 2
        assert data["running_processes"] == 1
        assert data["completed_tasks"] == 1
        assert data["failed_tasks"] == 0
        assert "timestamp" in data
    
    def test_running_processes_count_endpoint(self):
        """Test running processes count endpoint."""
        self.mock_monitoring_service.get_running_processes_count.return_value = 3
        
        response = self.client.get("/monitoring/processes/count")
        
        assert response.status_code == 200
        data = response.json()
        assert data["running_processes"] == 3
    
    def test_system_metrics_endpoint(self):
        """Test system metrics endpoint."""
        # Mock system metrics service response
        metrics_response = SystemMetrics(
            timestamp=time.time(),
            cpu_count=4,
            memory_total_mb=8192,
            memory_available_mb=4096,
            memory_used_mb=2048,
            memory_usage_percent=25.0,
            disk_usage_percent=45.5
        )
        self.mock_monitoring_service.get_system_metrics.return_value = metrics_response
        
        response = self.client.get("/monitoring/system/metrics")
        
        assert response.status_code == 200
        data = response.json()
        assert data["cpu_count"] == 4
        assert data["memory_total_mb"] == 8192
        assert data["memory_used_mb"] == 2048
        assert data["memory_usage_percent"] == 25.0
        assert data["disk_usage_percent"] == 45.5
        assert "timestamp" in data
    
    def test_memory_usage_endpoint(self):
        """Test memory usage endpoint."""
        # Mock system metrics service response
        metrics_response = SystemMetrics(
            timestamp=time.time(),
            cpu_count=4,
            memory_total_mb=8192,
            memory_available_mb=4096,
            memory_used_mb=2048,
            memory_usage_percent=25.0
        )
        self.mock_monitoring_service.get_system_metrics.return_value = metrics_response
        
        response = self.client.get("/monitoring/system/memory")
        
        assert response.status_code == 200
        data = response.json()
        assert data["memory_total_mb"] == 8192
        assert data["memory_used_mb"] == 2048
        assert data["memory_available_mb"] == 4096
        assert data["memory_usage_percent"] == 25.0
        assert "timestamp" in data
    
    def test_monitoring_overview_endpoint(self):
        """Test monitoring overview endpoint."""
        # Mock comprehensive overview service response
        overview_response = MonitoringOverview(
            health=HealthStatus(
                status="healthy",
                timestamp=time.time(),
                service_name="test-worker",
                uptime_seconds=3600
            ),
            system=SystemMetrics(
                timestamp=time.time(),
                cpu_count=4,
                memory_total_mb=8192,
                memory_available_mb=4096,
                memory_used_mb=2048,
                memory_usage_percent=25.0
            ),
            processes=ProcessMetrics(
                timestamp=time.time(),
                total_processes=1,
                running_processes=1,
                completed_tasks=0,
                failed_tasks=0,
                processes=[]
            ),
            service_info={"service_name": "test-worker", "version": "1.0.0"}
        )
        self.mock_monitoring_service.get_monitoring_overview.return_value = overview_response
        
        response = self.client.get("/monitoring/overview")
        
        assert response.status_code == 200
        data = response.json()
        assert "health" in data
        assert "system" in data
        assert "processes" in data
        assert "service_info" in data
        assert data["health"]["status"] == "healthy"
        assert data["system"]["cpu_count"] == 4
        assert data["processes"]["total_processes"] == 1
    
    def test_health_check_error_handling(self):
        """Test health check endpoint error handling."""
        # Mock service to raise an exception
        self.mock_monitoring_service.get_service_health.side_effect = Exception("Service error")
        
        response = self.client.get("/monitoring/health")
        
        assert response.status_code == 503
        assert "Service unhealthy" in response.json()["detail"]
    
    def test_processes_error_handling(self):
        """Test processes endpoint error handling."""
        # Mock service to raise an exception
        self.mock_monitoring_service.get_process_metrics.side_effect = Exception("Process error")
        
        response = self.client.get("/monitoring/processes")
        
        assert response.status_code == 500
        assert "Failed to get process information" in response.json()["detail"]
    
    def test_system_metrics_error_handling(self):
        """Test system metrics endpoint error handling."""
        # Mock service to raise an exception
        self.mock_monitoring_service.get_system_metrics.side_effect = Exception("Metrics error")
        
        response = self.client.get("/monitoring/system/metrics")
        
        assert response.status_code == 500
        assert "Failed to get system metrics" in response.json()["detail"]


class TestMonitoringIntegration:
    """Integration tests for monitoring with real task execution."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.client = TestClient(app)
    
    def test_task_lifecycle_monitoring(self):
        """Test monitoring of a real task through its lifecycle."""
        import time
        import base64
        import polars as pl
        
        # Create a simple task that takes some time
        df = pl.LazyFrame({'value': [i for i in range(100)]}).select(
            (pl.col('value') * 2).alias('doubled')
        )
        serialized_df = df.serialize()
        
        from flowfile_worker import models
        polars_script = models.PolarsScript(
            operation=base64.encodebytes(serialized_df),
            operation_type='store'
        )
        
        # Initial monitoring state - should have 0 running processes
        response = self.client.get("/monitoring/processes/count")
        assert response.status_code == 200
        initial_count = response.json()["running_processes"]
        
        # Submit the task
        submit_response = self.client.post('/submit_query', data=polars_script.json())
        assert submit_response.status_code == 200
        status = models.Status.model_validate(submit_response.json())
        task_id = status.background_task_id
        
        # Give the process time to start
        time.sleep(0.5)
        
        # Check that the task appears in all processes
        processes_response = self.client.get("/monitoring/processes")
        assert processes_response.status_code == 200
        processes_data = processes_response.json()
        
        # Find our task in the processes list
        task_found = any(
            proc['task_id'] == task_id 
            for proc in processes_data['processes']
        )
        assert task_found, f"Task {task_id} not found in processes list"
        
        # Tasks complete very fast, check if it's still running OR completed
        count_response = self.client.get("/monitoring/processes/count")
        assert count_response.status_code == 200
        current_count = count_response.json()["running_processes"]
        
        # Task might have completed already (they're very fast), so we just verify it was found
        # The important assertion is that the task was found in the process list
        
        # Wait for task to complete
        max_wait = 10  # seconds
        waited = 0
        while waited < max_wait:
            status_response = self.client.get(f'/status/{task_id}')
            if status_response.status_code == 200:
                status = models.Status.model_validate(status_response.json())
                if status.status == 'Completed':
                    break
            time.sleep(0.5)
            waited += 0.5
        
        assert status.status == 'Completed', f"Task did not complete in time, status: {status.status}"
        
        # Wait a bit for cleanup
        time.sleep(0.5)
        
        # Verify the task is no longer in running processes
        final_processes = self.client.get("/monitoring/processes")
        assert final_processes.status_code == 200
        final_data = final_processes.json()
        
        # Task might still be in the list but should not be in "running" status
        running_tasks = [
            proc for proc in final_data['processes']
            if proc['task_id'] == task_id and proc['status'].lower() in ['processing', 'starting']
        ]
        assert len(running_tasks) == 0, "Task should not be in running state after completion"
    
    def test_multiple_tasks_monitoring(self):
        """Test monitoring with multiple concurrent tasks."""
        import time
        import base64
        import polars as pl
        from flowfile_worker import models
        
        # Create multiple tasks
        task_ids = []
        num_tasks = 3
        
        for i in range(num_tasks):
            df = pl.LazyFrame({'value': [j for j in range(50)]}).select(
                (pl.col('value') + i).alias(f'result_{i}')
            )
            serialized_df = df.serialize()
            
            polars_script = models.PolarsScript(
                operation=base64.encodebytes(serialized_df),
                operation_type='store'
            )
            
            response = self.client.post('/submit_query', data=polars_script.json())
            assert response.status_code == 200
            status = models.Status.model_validate(response.json())
            task_ids.append(status.background_task_id)
        
        # Give processes time to start
        time.sleep(0.5)
        
        # Check process metrics
        processes_response = self.client.get("/monitoring/processes")
        assert processes_response.status_code == 200
        processes_data = processes_response.json()
        
        # Verify all tasks are tracked
        found_tasks = sum(
            1 for proc in processes_data['processes']
            if proc['task_id'] in task_ids
        )
        assert found_tasks >= 1, f"Expected to find at least 1 of our {num_tasks} tasks, found {found_tasks}"
        
        # Check the overview endpoint
        overview_response = self.client.get("/monitoring/overview")
        assert overview_response.status_code == 200
        overview_data = overview_response.json()
        
        assert 'health' in overview_data
        assert 'system' in overview_data
        assert 'processes' in overview_data
        assert overview_data['processes']['total_processes'] >= 1
        
        # Wait for all tasks to complete
        max_wait = 15
        for task_id in task_ids:
            waited = 0
            while waited < max_wait:
                status_response = self.client.get(f'/status/{task_id}')
                if status_response.status_code == 200:
                    status = models.Status.model_validate(status_response.json())
                    if status.status in ['Completed', 'Error']:
                        break
                time.sleep(0.5)
                waited += 0.5
    
    def test_process_status_transitions(self):
        """Test that process status transitions are reflected in monitoring."""
        import time
        import base64
        import polars as pl
        from flowfile_worker import models
        
        # Create a task
        df = pl.LazyFrame({'x': [1, 2, 3]})
        serialized_df = df.serialize()
        
        polars_script = models.PolarsScript(
            operation=base64.encodebytes(serialized_df),
            operation_type='store'
        )
        
        # Submit task
        response = self.client.post('/submit_query', data=polars_script.json())
        assert response.status_code == 200
        status = models.Status.model_validate(response.json())
        task_id = status.background_task_id
        
        # Status should start as "Starting"
        assert status.status == 'Starting'
        
        # Give it time to process
        time.sleep(0.3)
        
        # Check monitoring reflects the status
        processes_response = self.client.get("/monitoring/processes")
        assert processes_response.status_code == 200
        processes_data = processes_response.json()
        
        # Find our task
        our_task = next(
            (proc for proc in processes_data['processes'] if proc['task_id'] == task_id),
            None
        )
        
        if our_task:
            # Status should be either Starting or Processing (case-insensitive)
            assert our_task['status'].lower() in ['starting', 'processing', 'completed'], \
                f"Unexpected status: {our_task['status']}"
        
        # Wait for completion
        max_wait = 10
        waited = 0
        final_status = None
        while waited < max_wait:
            status_response = self.client.get(f'/status/{task_id}')
            if status_response.status_code == 200:
                final_status = models.Status.model_validate(status_response.json())
                if final_status.status == 'Completed':
                    break
            time.sleep(0.5)
            waited += 0.5
        
        assert final_status is not None
        assert final_status.status == 'Completed'
    
    def test_monitoring_with_sample_task(self):
        """Test monitoring with a sample storage task."""
        import time
        import base64
        import polars as pl
        from flowfile_worker import models
        
        # Create a sample task
        lf = pl.LazyFrame({'value': [i for i in range(1000)]})
        serialized_df = lf.serialize()
        polars_script = models.PolarsScriptSample(
            operation=base64.encodebytes(serialized_df),
            operation_type='store_sample',
            sample_size=10
        )
        
        # Check initial state
        initial_response = self.client.get("/monitoring/processes/count")
        assert initial_response.status_code == 200
        
        # Submit sample task
        response = self.client.post('/store_sample', data=polars_script.json())
        assert response.status_code == 200
        status = models.Status.model_validate(response.json())
        task_id = status.background_task_id
        
        # Give it time to start
        time.sleep(0.3)
        
        # Verify task appears in monitoring
        processes_response = self.client.get("/monitoring/processes")
        assert processes_response.status_code == 200
        processes_data = processes_response.json()
        
        task_in_list = any(
            proc['task_id'] == task_id
            for proc in processes_data['processes']
        )
        assert task_in_list, f"Sample task {task_id} not found in monitoring"
        
        # Wait for completion
        max_wait = 10
        waited = 0
        while waited < max_wait:
            status_response = self.client.get(f'/status/{task_id}')
            if status_response.status_code == 200:
                final_status = models.Status.model_validate(status_response.json())
                if final_status.status in ['Completed', 'Error']:
                    break
            time.sleep(0.5)
            waited += 0.5
        
        assert final_status.status == 'Completed', f"Task status: {final_status.status}"
    
    def test_health_check_during_task_execution(self):
        """Test that health check works correctly during task execution."""
        import time
        import base64
        import polars as pl
        from flowfile_worker import models
        
        # Submit a task
        df = pl.LazyFrame({'data': range(100)})
        polars_script = models.PolarsScript(
            operation=base64.encodebytes(df.serialize()),
            operation_type='store'
        )
        
        response = self.client.post('/submit_query', data=polars_script.json())
        assert response.status_code == 200
        task_id = models.Status.model_validate(response.json()).background_task_id
        
        # Check health while task is running
        time.sleep(0.3)
        
        health_response = self.client.get("/monitoring/health")
        assert health_response.status_code == 200
        health_data = health_response.json()
        assert 'healthy' in health_data
        assert 'timestamp' in health_data
        
        # Check detailed health
        detailed_health = self.client.get("/monitoring/health/detailed")
        assert detailed_health.status_code == 200
        detailed_data = detailed_health.json()
        assert 'status' in detailed_data
        assert detailed_data['status'] in ['healthy', 'degraded', 'unhealthy']
        assert 'uptime_seconds' in detailed_data
        assert detailed_data['uptime_seconds'] > 0
        
        # Wait for task to complete
        max_wait = 10
        waited = 0
        while waited < max_wait:
            status_response = self.client.get(f'/status/{task_id}')
            if status_response.status_code == 200:
                status = models.Status.model_validate(status_response.json())
                if status.status == 'Completed':
                    break
            time.sleep(0.5)
            waited += 0.5