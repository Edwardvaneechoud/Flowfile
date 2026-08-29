"""
Tests for CLI 'flowfile run flow' command.

Run with:
    pytest flowfile/tests/test_cli.py -v
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
import yaml

from flowfile.__main__ import run_flow
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.schemas import input_schema, schemas, transform_schema


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
        assert 'File not found' in captured.err

    def test_run_flow_unsupported_format(self, temp_dir: Path, capsys):
        """Test error for unsupported file format."""
        txt_path = temp_dir / 'flow.txt'
        txt_path.write_text('not a flow')

        result = run_flow(str(txt_path))

        assert result == 1
        captured = capsys.readouterr()
        assert 'Unsupported file format' in captured.err

    def test_run_flow_invalid_yaml(self, temp_dir: Path, capsys):
        """Test error when YAML is invalid or not a valid flow."""
        invalid_yaml = temp_dir / 'invalid.yaml'
        invalid_yaml.write_text('not: a: valid: flow')

        result = run_flow(str(invalid_yaml))

        assert result == 1
        captured = capsys.readouterr()
        assert 'Error loading flow' in captured.err

    def test_run_flow_success(self, simple_flow_yaml: Path, capsys):
        """Test successful flow execution."""
        result = run_flow(str(simple_flow_yaml))
        assert result == 0

        captured = capsys.readouterr()
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

    def test_run_flow_displays_duration(self, simple_flow_yaml: Path, capsys):
        """Test that execution duration is displayed on success."""
        result = run_flow(str(simple_flow_yaml))

        assert result == 0
        captured = capsys.readouterr()
        assert ' in ' in captured.out and 's' in captured.out

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


def _new_graph(flow_id: int):
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name='cli_gate_flow',
            path='.',
            execution_mode='Development',
            execution_location='local',
        )
    )
    return handler.get_flow(flow_id)


def _add_node(graph, node_type: str, node_id: int) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type)
    )


def _connect(graph, from_id: int, to_id: int) -> None:
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id))


def _add_manual_input(graph, node_id: int, data: list[dict]) -> None:
    _add_node(graph, 'manual_input', node_id)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(data),
        )
    )


def _add_gate(graph, node_id: int, depending_on_id: int, **gate_kwargs) -> None:
    _add_node(graph, 'gate', node_id)
    _connect(graph, depending_on_id, node_id)
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            is_setup=True,
            gate_input=transform_schema.GateInput(**gate_kwargs),
        )
    )


def _add_rename_select(graph, node_id: int, depending_on_id: int, old: str, new: str) -> None:
    _add_node(graph, 'select', node_id)
    _connect(graph, depending_on_id, node_id)
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            select_input=[transform_schema.SelectInput(old_name=old, new_name=new)],
            keep_missing=True,
        )
    )


def _add_csv_output(graph, node_id: int, depending_on_id: int, directory: Path, name: str) -> None:
    _add_node(graph, 'output', node_id)
    _connect(graph, depending_on_id, node_id)
    graph.add_output(
        input_schema.NodeOutput(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            output_settings=input_schema.OutputSettings(
                name=name,
                directory=str(directory),
                file_type='csv',
                write_mode='overwrite',
                table_settings=input_schema.OutputCsvTable(),
            ),
        )
    )


def _write_diamond_flow(tmp_path: Path) -> tuple[Path, Path]:
    """The gate diamond from the docs: env flips which branch reaches the union.

    1 manual_input -> 2 gate(env == prod) -> 3 select(a -> a_prod) -> 6 union
    1 manual_input -> 4 gate(env != prod) -> 5 select(a -> a_dev) -> 6
    6 union -> 7 output result.csv (the observable: which column got written)
    """
    graph = _new_graph(flow_id=90)
    graph.flow_settings.parameters.append(FlowParameter(name='env', default_value='prod', type='string'))
    _add_manual_input(graph, node_id=1, data=[{'a': 1}, {'a': 2}, {'a': 3}])
    _add_gate(graph, node_id=2, depending_on_id=1, parameter='env', operator='equals', value='prod')
    _add_rename_select(graph, node_id=3, depending_on_id=2, old='a', new='a_prod')
    _add_gate(graph, node_id=4, depending_on_id=1, parameter='env', operator='not_equals', value='prod')
    _add_rename_select(graph, node_id=5, depending_on_id=4, old='a', new='a_dev')
    _add_node(graph, 'union', 6)
    _connect(graph, 3, 6)
    _connect(graph, 5, 6)
    graph.add_union(input_schema.NodeUnion(flow_id=graph.flow_id, node_id=6, depending_on_ids=[3, 5]))
    out_path = tmp_path / 'result.csv'
    _add_csv_output(graph, node_id=7, depending_on_id=6, directory=tmp_path, name='result.csv')
    yaml_path = tmp_path / 'gate_diamond.yaml'
    graph.save_flow(str(yaml_path))
    return yaml_path, out_path


def _write_boolean_gate_flow(tmp_path: Path) -> tuple[Path, Path]:
    """1 manual_input -> 2 gate(full is_true) -> 3 output result.csv.

    The 'full' parameter is declared boolean with default 'false', so the
    output file only appears when an override opens the gate.
    """
    graph = _new_graph(flow_id=91)
    graph.flow_settings.parameters.append(FlowParameter(name='full', default_value='false', type='boolean'))
    _add_manual_input(graph, node_id=1, data=[{'a': 1}, {'a': 2}, {'a': 3}])
    _add_gate(graph, node_id=2, depending_on_id=1, parameter='full', operator='is_true')
    out_path = tmp_path / 'result.csv'
    _add_csv_output(graph, node_id=3, depending_on_id=2, directory=tmp_path, name='result.csv')
    yaml_path = tmp_path / 'bool_gate.yaml'
    graph.save_flow(str(yaml_path))
    return yaml_path, out_path


class TestRunFlowParamOverrides:
    """``--param`` overrides flipping gates on a headless run (the documented way).

    Findings on typing, pinned by the tests below:

    * Overrides arrive as raw ``key=value`` strings; ``run_flow`` writes the raw
      string into the matching parameter's ``default_value``. Coercion happens
      later, at gate evaluation, driven by the type the flow file declares for
      the parameter — a string param compares as a string, a boolean param is
      parsed from its literal.
    * Boolean parameters accept the engine's literal sets case-insensitively:
      ``true/1/yes/on`` and ``false/0/no/off``. Any other token (e.g.
      ``banana``) makes ``GateInput.evaluate`` raise, the gate node fails
      loudly, and ``run_flow`` exits 1 — it never silently routes a branch.
    * An override naming a parameter the flow does not declare is appended as a
      NEW string-typed parameter (as documented in docs/users/deployment/cli.md).
    """

    def test_default_parameter_routes_the_prod_branch(self, tmp_path: Path):
        yaml_path, out_path = _write_diamond_flow(tmp_path)

        assert run_flow(str(yaml_path)) == 0

        written = pl.read_csv(out_path)
        assert written.columns == ['a_prod']
        assert written['a_prod'].to_list() == [1, 2, 3]

    def test_string_param_override_flips_the_gate(self, tmp_path: Path, capsys):
        yaml_path, out_path = _write_diamond_flow(tmp_path)

        assert run_flow(str(yaml_path), param_overrides=['env=dev']) == 0

        captured = capsys.readouterr()
        assert "Parameter overrides: {'env': 'dev'}" in captured.out
        written = pl.read_csv(out_path)
        assert written.columns == ['a_dev']
        assert written['a_dev'].to_list() == [1, 2, 3]

    @pytest.mark.parametrize('raw', ['true', 'TRUE', '1', 'yes', 'on'])
    def test_boolean_truthy_literals_open_the_gate(self, tmp_path: Path, raw: str):
        yaml_path, out_path = _write_boolean_gate_flow(tmp_path)

        assert run_flow(str(yaml_path), param_overrides=[f'full={raw}']) == 0

        assert out_path.exists()
        assert pl.read_csv(out_path)['a'].to_list() == [1, 2, 3]

    @pytest.mark.parametrize('raw', [None, 'false', '0', 'off'])
    def test_boolean_falsy_literals_keep_the_gate_closed_and_stay_green(self, tmp_path: Path, raw):
        """A deliberately gated-off output is a green skip: exit 0, no file."""
        yaml_path, out_path = _write_boolean_gate_flow(tmp_path)
        overrides = None if raw is None else [f'full={raw}']

        assert run_flow(str(yaml_path), param_overrides=overrides) == 0

        assert not out_path.exists()

    def test_invalid_boolean_override_fails_the_run_loudly(self, tmp_path: Path, capsys):
        yaml_path, out_path = _write_boolean_gate_flow(tmp_path)

        assert run_flow(str(yaml_path), param_overrides=['full=banana']) == 1

        captured = capsys.readouterr()
        assert 'boolean' in captured.err
        assert not out_path.exists()


class TestCLIArgParsing:
    """Tests for CLI argument parsing."""

    def test_main_shows_flow_command_in_help(self, capsys):
        """Test that 'flowfile run flow' appears in help output."""
        from flowfile.__main__ import main

        with patch('sys.argv', ['flowfile']):
            main()

        captured = capsys.readouterr()
        assert 'flowfile run flow' in captured.out
        assert 'my_pipeline.yaml' in captured.out


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
