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
        process_info = ProcessInfo(
            task_id="test-task-123",
            pid=1234,
            status="Running",
            start_time=time.time()
        )
        assert process_info.task_id == "test-task-123"
        assert process_info.pid == 1234
        assert process_info.status == "Running"
        assert process_info.start_time is not None
    
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
        
        mock_status1 = Mock()
        mock_status1.status = "Processing"
        
        mock_status2 = Mock()
        mock_status2.status = "Completed"
        
        mock_status3 = Mock()
        mock_status3.status = "Error"
        
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
        self.worker_state.get_all_processes.return_value = {}
        self.worker_state.get_status.return_value = None
        
        processes = self.provider.get_all_processes()
        assert len(processes) == 0
    
    def test_get_all_processes_with_data(self):
        """Test getting all processes with data."""
        # Mock process data
        mock_process1 = Mock()
        mock_process1.pid = 1234
        mock_process1.start_time = time.time()
        
        mock_process2 = Mock()
        mock_process2.pid = 5678
        mock_process2.start_time = time.time()
        
        # Mock status data
        mock_status1 = Mock()
        mock_status1.status = "Processing"
        
        mock_status2 = Mock()
        mock_status2.status = "Completed"
        
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
        
        assert process2 is not None
        assert process2.pid == 5678
        assert process2.status == "Completed"
    
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