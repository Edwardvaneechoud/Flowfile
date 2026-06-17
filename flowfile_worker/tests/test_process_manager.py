"""Tests for process_manager module."""

from multiprocessing import Process
from unittest.mock import MagicMock, patch

from flowfile_worker.process_manager import ProcessManager


class TestProcessManager:
    """Test ProcessManager class."""

    def test_init(self):
        pm = ProcessManager()
        assert isinstance(pm.process_dict, dict)
        assert len(pm.process_dict) == 0

    def test_add_process(self):
        pm = ProcessManager()
        mock_process = MagicMock(spec=Process)
        pm.add_process("task1", mock_process)
        assert "task1" in pm.process_dict
        assert pm.process_dict["task1"] is mock_process

    def test_get_process_exists(self):
        pm = ProcessManager()
        mock_process = MagicMock(spec=Process)
        pm.add_process("task1", mock_process)
        result = pm.get_process("task1")
        assert result is mock_process

    def test_get_process_not_exists(self):
        pm = ProcessManager()
        result = pm.get_process("nonexistent")
        assert result is None

    def test_remove_process(self):
        pm = ProcessManager()
        mock_process = MagicMock(spec=Process)
        pm.add_process("task1", mock_process)
        pm.remove_process("task1")
        assert "task1" not in pm.process_dict

    def test_remove_process_not_exists(self):
        pm = ProcessManager()
        # Should not raise
        pm.remove_process("nonexistent")

    def test_cancel_process_exists(self):
        pm = ProcessManager()
        mock_process = MagicMock(spec=Process)
        pm.add_process("task1", mock_process)
        result = pm.cancel_process("task1")
        assert result is True
        mock_process.terminate.assert_called_once()
        mock_process.join.assert_called_once()
        assert "task1" not in pm.process_dict

    def test_cancel_process_not_exists(self):
        pm = ProcessManager()
        result = pm.cancel_process("nonexistent")
        assert result is False

    def test_multiple_processes(self):
        pm = ProcessManager()
        p1 = MagicMock(spec=Process)
        p2 = MagicMock(spec=Process)
        p3 = MagicMock(spec=Process)

        pm.add_process("task1", p1)
        pm.add_process("task2", p2)
        pm.add_process("task3", p3)

        assert len(pm.process_dict) == 3

        pm.cancel_process("task2")
        assert len(pm.process_dict) == 2
        assert "task2" not in pm.process_dict

    def test_thread_safety(self):
        """Test that the lock attribute exists for thread safety."""
        pm = ProcessManager()
        assert hasattr(pm, "lock")
