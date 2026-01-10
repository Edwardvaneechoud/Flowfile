"""
Tests for CLI 'flowfile run flow' command.

Run with:
    pytest flowfile/tests/test_cli.py -v
"""

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

    def test_run_flow_file_not_found(self, capsys):
        """Test error when file doesn't exist."""
        result = run_flow('/nonexistent/path/flow.yaml')

        assert result == 1
        captured = capsys.readouterr()
        assert 'Error: File not found' in captured.out

    def test_run_flow_unsupported_format(self, temp_dir: Path, capsys):
        """Test error for unsupported file format."""
        txt_path = temp_dir / 'flow.txt'
        txt_path.write_text('not a flow')

        result = run_flow(str(txt_path))

        assert result == 1
        captured = capsys.readouterr()
        assert 'Unsupported file format' in captured.out
        assert '.txt' in captured.out

    def test_run_flow_invalid_yaml(self, temp_dir: Path, capsys):
        """Test error when YAML is invalid or not a valid flow."""
        invalid_yaml = temp_dir / 'invalid.yaml'
        invalid_yaml.write_text('not: a: valid: flow')

        result = run_flow(str(invalid_yaml))

        assert result == 1
        captured = capsys.readouterr()
        assert 'Error loading flow' in captured.out

    def test_run_flow_success(self, simple_flow_yaml: Path, capsys):
        """Test successful flow execution."""
        result = run_flow(str(simple_flow_yaml))

        assert result == 0
        captured = capsys.readouterr()

        # Check output contains expected messages
        assert 'Loading flow from:' in captured.out
        assert 'Running flow:' in captured.out
        assert 'Nodes:' in captured.out
        assert 'Flow completed successfully' in captured.out
        assert 'Nodes completed:' in captured.out

    def test_run_flow_json_format(self, temp_dir: Path, capsys):
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
        captured = capsys.readouterr()
        assert 'Flow completed successfully' in captured.out

    def test_run_flow_yml_extension(self, temp_dir: Path, capsys):
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

    def test_run_flow_displays_duration(self, simple_flow_yaml: Path, capsys):
        """Test that execution duration is displayed on success."""
        result = run_flow(str(simple_flow_yaml))

        assert result == 0
        captured = capsys.readouterr()
        # Should show duration in seconds
        assert 'in ' in captured.out and 's' in captured.out

    def test_run_flow_shows_node_count(self, simple_flow_yaml: Path, capsys):
        """Test that node count is displayed."""
        result = run_flow(str(simple_flow_yaml))

        assert result == 0
        captured = capsys.readouterr()
        assert 'Nodes: 1' in captured.out


class TestRunFlowWithFailures:
    """Tests for flow execution failure scenarios."""

    def test_run_flow_empty_flow(self, temp_dir: Path, capsys):
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
        captured = capsys.readouterr()
        assert 'Nodes: 0' in captured.out


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
