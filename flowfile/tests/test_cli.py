"""
Tests for CLI 'flowfile run flow' command.

Run with:
    pytest flowfile/tests/test_cli.py -v
"""

import logging
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from flowfile.__main__ import run_flow


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def capture_logs():
    """Capture logs from the PipelineHandler logger."""
    # Get the logger used by flowfile_core
    logger = logging.getLogger("PipelineHandler")

    # Store original settings
    original_handlers = logger.handlers.copy()
    original_level = logger.level
    original_propagate = logger.propagate

    # Create a handler to capture log messages
    log_capture = []

    class ListHandler(logging.Handler):
        def emit(self, record):
            log_capture.append(self.format(record))

    handler = ListHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(message)s'))

    # Add our handler
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    yield log_capture

    # Restore original settings
    logger.handlers = original_handlers
    logger.level = original_level
    logger.propagate = original_propagate


@pytest.fixture
def simple_flow_yaml(temp_dir: Path) -> Path:
    """Create a simple flow YAML file for testing."""
    flow_data = {
        'flowfile_version': '0.5.0',
        'flowfile_id': 1,
        'flowfile_name': 'test_flow',
        'flowfile_settings': {
            'description': 'Test flow for CLI',
            'execution_mode': 'Development',
            'execution_location': 'local',
            'auto_save': False,
            'show_detailed_progress': False,
        },
        'nodes': [
            {
                'id': 1,
                'type': 'manual_input',
                'is_start_node': True,
                'description': 'Test input',
                'x_position': 0,
                'y_position': 0,
                'inputs': None,
                'outputs': [],
                'setting_input': {
                    'cache_results': False,
                    'raw_data_format': {
                        'columns': [
                            {'name': 'id', 'data_type': 'Int64'},
                            {'name': 'name', 'data_type': 'String'},
                        ],
                        'data': [
                            [1, 2, 3],
                            ['Alice', 'Bob', 'Charlie'],
                        ],
                    },
                },
            },
        ],
    }

    yaml_path = temp_dir / 'test_flow.yaml'
    with open(yaml_path, 'w') as f:
        yaml.dump(flow_data, f)

    return yaml_path


class TestRunFlowCommand:
    """Tests for the 'flowfile run flow' CLI command."""

    def test_run_flow_file_not_found(self, capture_logs):
        """Test error when file doesn't exist."""
        result = run_flow('/nonexistent/path/flow.yaml')

        assert result == 1
        log_text = '\n'.join(capture_logs)
        assert 'File not found' in log_text

    def test_run_flow_unsupported_format(self, temp_dir: Path, capture_logs):
        """Test error for unsupported file format."""
        txt_path = temp_dir / 'flow.txt'
        txt_path.write_text('not a flow')

        result = run_flow(str(txt_path))

        assert result == 1
        log_text = '\n'.join(capture_logs)
        assert 'Unsupported file format' in log_text

    def test_run_flow_invalid_yaml(self, temp_dir: Path, capture_logs):
        """Test error when YAML is invalid or not a valid flow."""
        invalid_yaml = temp_dir / 'invalid.yaml'
        invalid_yaml.write_text('not: a: valid: flow')

        result = run_flow(str(invalid_yaml))

        assert result == 1
        log_text = '\n'.join(capture_logs)
        assert 'Error loading flow' in log_text

    def test_run_flow_success(self, simple_flow_yaml: Path, capture_logs):
        """Test successful flow execution."""
        result = run_flow(str(simple_flow_yaml))

        assert result == 0

        # Check log output contains expected messages
        log_text = '\n'.join(capture_logs)
        assert 'Loading flow from:' in log_text
        assert 'Running flow:' in log_text
        assert 'Nodes:' in log_text
        assert 'Flow completed successfully' in log_text
        assert 'Nodes completed:' in log_text

    def test_run_flow_json_format(self, temp_dir: Path, capture_logs):
        """Test running a flow from JSON format."""
        import json

        flow_data = {
            'flowfile_version': '0.5.0',
            'flowfile_id': 2,
            'flowfile_name': 'json_test_flow',
            'flowfile_settings': {
                'description': None,
                'execution_mode': 'Development',
                'execution_location': 'local',
                'auto_save': False,
                'show_detailed_progress': False,
            },
            'nodes': [
                {
                    'id': 1,
                    'type': 'manual_input',
                    'is_start_node': True,
                    'description': '',
                    'x_position': 0,
                    'y_position': 0,
                    'inputs': None,
                    'outputs': [],
                    'setting_input': {
                        'cache_results': False,
                        'raw_data_format': {
                            'columns': [
                                {'name': 'value', 'data_type': 'Int64'},
                            ],
                            'data': [[1, 2, 3]],
                        },
                    },
                },
            ],
        }

        json_path = temp_dir / 'test_flow.json'
        with open(json_path, 'w') as f:
            json.dump(flow_data, f)

        result = run_flow(str(json_path))

        assert result == 0
        log_text = '\n'.join(capture_logs)
        assert 'Flow completed successfully' in log_text

    def test_run_flow_yml_extension(self, temp_dir: Path):
        """Test that .yml extension is supported."""
        flow_data = {
            'flowfile_version': '0.5.0',
            'flowfile_id': 3,
            'flowfile_name': 'yml_test',
            'flowfile_settings': {
                'description': None,
                'execution_mode': 'Development',
                'execution_location': 'local',
                'auto_save': False,
                'show_detailed_progress': False,
            },
            'nodes': [
                {
                    'id': 1,
                    'type': 'manual_input',
                    'is_start_node': True,
                    'description': '',
                    'x_position': 0,
                    'y_position': 0,
                    'inputs': None,
                    'outputs': [],
                    'setting_input': {
                        'cache_results': False,
                        'raw_data_format': {
                            'columns': [{'name': 'x', 'data_type': 'Int64'}],
                            'data': [[1]],
                        },
                    },
                },
            ],
        }

        yml_path = temp_dir / 'test.yml'
        with open(yml_path, 'w') as f:
            yaml.dump(flow_data, f)

        result = run_flow(str(yml_path))

        assert result == 0

    def test_run_flow_displays_duration(self, simple_flow_yaml: Path, capture_logs):
        """Test that execution duration is displayed on success."""
        result = run_flow(str(simple_flow_yaml))

        assert result == 0
        # Should show duration in seconds
        log_text = '\n'.join(capture_logs)
        assert ' in ' in log_text and 's' in log_text

    def test_run_flow_shows_node_count(self, simple_flow_yaml: Path, capture_logs):
        """Test that node count is displayed."""
        result = run_flow(str(simple_flow_yaml))

        assert result == 0
        log_text = '\n'.join(capture_logs)
        assert 'Nodes: 1' in log_text


class TestRunFlowWithFailures:
    """Tests for flow execution failure scenarios."""

    def test_run_flow_empty_flow(self, temp_dir: Path, capture_logs):
        """Test running a flow with no nodes."""
        flow_data = {
            'flowfile_version': '0.5.0',
            'flowfile_id': 10,
            'flowfile_name': 'empty_flow',
            'flowfile_settings': {
                'description': None,
                'execution_mode': 'Development',
                'execution_location': 'local',
                'auto_save': False,
                'show_detailed_progress': False,
            },
            'nodes': [],
        }

        yaml_path = temp_dir / 'empty_flow.yaml'
        with open(yaml_path, 'w') as f:
            yaml.dump(flow_data, f)

        result = run_flow(str(yaml_path))

        # Empty flow should still succeed (0 nodes to execute)
        assert result == 0
        log_text = '\n'.join(capture_logs)
        assert 'Nodes: 0' in log_text


class TestCLIArgParsing:
    """Tests for CLI argument parsing."""

    def test_main_shows_flow_command_in_help(self, capsys):
        """Test that 'flowfile run flow' appears in help output."""
        from flowfile.__main__ import main

        # Call main with no arguments to show help
        with patch('sys.argv', ['flowfile']):
            main()

        captured = capsys.readouterr()
        assert 'flowfile run flow' in captured.out
        assert 'my_pipeline.yaml' in captured.out


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
