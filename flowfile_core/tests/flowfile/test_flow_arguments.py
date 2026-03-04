"""Tests for flow-level arguments: models, resolution, template substitution, and field binding."""

import pytest

from flowfile_core.schemas.flow_args import (
    FlowArgument,
    apply_field_binding,
    coerce_arg_value,
    resolve_flow_arguments,
    substitute_template,
)


# ============================================================================
# FlowArgument model tests
# ============================================================================


class TestFlowArgument:
    def test_valid_argument(self):
        arg = FlowArgument(name="input_path", arg_type="string", default="/data/input.csv")
        assert arg.name == "input_path"
        assert arg.arg_type == "string"
        assert arg.default == "/data/input.csv"

    def test_invalid_name_with_spaces(self):
        with pytest.raises(ValueError, match="valid identifier"):
            FlowArgument(name="input path")

    def test_invalid_name_starts_with_digit(self):
        with pytest.raises(ValueError, match="valid identifier"):
            FlowArgument(name="1input")

    def test_name_with_underscores(self):
        arg = FlowArgument(name="my_input_path_2")
        assert arg.name == "my_input_path_2"

    def test_required_argument(self):
        arg = FlowArgument(name="required_arg", required=True)
        assert arg.required is True

    def test_options_list(self):
        arg = FlowArgument(name="mode", options=["fast", "slow", "medium"])
        assert arg.options == ["fast", "slow", "medium"]


# ============================================================================
# coerce_arg_value tests
# ============================================================================


class TestCoerceArgValue:
    def test_string_coercion(self):
        assert coerce_arg_value(123, "string") == "123"
        assert coerce_arg_value("hello", "string") == "hello"

    def test_number_int_coercion(self):
        assert coerce_arg_value("42", "number") == 42
        assert isinstance(coerce_arg_value("42", "number"), int)

    def test_number_float_coercion(self):
        assert coerce_arg_value("3.14", "number") == 3.14
        assert isinstance(coerce_arg_value("3.14", "number"), float)

    def test_number_invalid(self):
        with pytest.raises(ValueError, match="Cannot convert"):
            coerce_arg_value("not_a_number", "number")

    def test_boolean_true(self):
        assert coerce_arg_value("true", "boolean") is True
        assert coerce_arg_value("1", "boolean") is True
        assert coerce_arg_value("yes", "boolean") is True
        assert coerce_arg_value(True, "boolean") is True

    def test_boolean_false(self):
        assert coerce_arg_value("false", "boolean") is False
        assert coerce_arg_value("0", "boolean") is False
        assert coerce_arg_value("no", "boolean") is False

    def test_boolean_invalid(self):
        with pytest.raises(ValueError, match="Cannot convert"):
            coerce_arg_value("maybe", "boolean")

    def test_list_from_string(self):
        assert coerce_arg_value("a, b, c", "list") == ["a", "b", "c"]

    def test_list_passthrough(self):
        assert coerce_arg_value(["x", "y"], "list") == ["x", "y"]

    def test_none_passthrough(self):
        assert coerce_arg_value(None, "string") is None


# ============================================================================
# resolve_flow_arguments tests
# ============================================================================


class TestResolveFlowArguments:
    def test_basic_resolution(self):
        args = [
            FlowArgument(name="path", arg_type="string", default="/data/default.csv"),
            FlowArgument(name="threshold", arg_type="number", default=0.5),
        ]
        result = resolve_flow_arguments(args, {"path": "/data/custom.csv"})
        assert result == {"path": "/data/custom.csv", "threshold": 0.5}

    def test_required_missing(self):
        args = [FlowArgument(name="path", arg_type="string", required=True)]
        with pytest.raises(ValueError, match="Required flow argument 'path'"):
            resolve_flow_arguments(args, {})

    def test_required_provided(self):
        args = [FlowArgument(name="path", arg_type="string", required=True)]
        result = resolve_flow_arguments(args, {"path": "/data/file.csv"})
        assert result == {"path": "/data/file.csv"}

    def test_unknown_argument(self):
        args = [FlowArgument(name="path", arg_type="string")]
        with pytest.raises(ValueError, match="Unknown flow arguments"):
            resolve_flow_arguments(args, {"nonexistent": "value"})

    def test_type_coercion(self):
        args = [FlowArgument(name="threshold", arg_type="number")]
        result = resolve_flow_arguments(args, {"threshold": "0.8"})
        assert result == {"threshold": 0.8}

    def test_optional_defaults_to_none(self):
        args = [FlowArgument(name="optional_param", arg_type="string")]
        result = resolve_flow_arguments(args, {})
        assert result == {"optional_param": None}

    def test_empty_args_empty_values(self):
        result = resolve_flow_arguments([], {})
        assert result == {}


# ============================================================================
# substitute_template tests
# ============================================================================


class TestSubstituteTemplate:
    def test_basic_substitution(self):
        text = "SELECT * FROM {{table_name}} WHERE id > {{threshold}}"
        args = {"table_name": "users", "threshold": 10}
        result = substitute_template(text, args)
        assert result == "SELECT * FROM users WHERE id > 10"

    def test_unrecognised_placeholder_left_as_is(self):
        text = "Hello {{name}}, welcome to {{unknown}}"
        args = {"name": "World"}
        result = substitute_template(text, args)
        assert result == "Hello World, welcome to {{unknown}}"

    def test_no_placeholders(self):
        text = "No placeholders here"
        result = substitute_template(text, {"key": "value"})
        assert result == "No placeholders here"

    def test_empty_args(self):
        text = "Hello {{name}}"
        result = substitute_template(text, {})
        assert result == "Hello {{name}}"

    def test_multiline_code(self):
        code = """import polars as pl
df = pl.read_csv("{{input_path}}")
result = df.filter(pl.col("value") > {{threshold}})
"""
        args = {"input_path": "/data/input.csv", "threshold": 42}
        result = substitute_template(code, args)
        assert '/data/input.csv' in result
        assert '42' in result
        assert '{{' not in result


# ============================================================================
# apply_field_binding tests
# ============================================================================


class TestApplyFieldBinding:
    def test_simple_field(self):
        class Obj:
            name = "old"
        obj = Obj()
        apply_field_binding(obj, "name", "new")
        assert obj.name == "new"

    def test_nested_field(self):
        class Inner:
            path = "/old/path"

        class Outer:
            received_file = Inner()

        obj = Outer()
        apply_field_binding(obj, "received_file.path", "/new/path")
        assert obj.received_file.path == "/new/path"

    def test_deeply_nested(self):
        class Level3:
            value = 0

        class Level2:
            child = Level3()

        class Level1:
            child = Level2()

        obj = Level1()
        apply_field_binding(obj, "child.child.value", 42)
        assert obj.child.child.value == 42

    def test_nonexistent_field(self):
        class Obj:
            pass
        obj = Obj()
        with pytest.raises(AttributeError):
            apply_field_binding(obj, "nonexistent.field", "value")


# ============================================================================
# Integration: FlowArgument in schema models
# ============================================================================


class TestFlowArgumentInSchemas:
    def test_flow_settings_has_flow_arguments(self):
        from flowfile_core.schemas.schemas import FlowfileSettings

        settings = FlowfileSettings()
        assert settings.flow_arguments == []

    def test_flow_settings_with_arguments(self):
        from flowfile_core.schemas.schemas import FlowfileSettings

        args = [
            FlowArgument(name="input_path", arg_type="string", default="/data/input.csv"),
            FlowArgument(name="threshold", arg_type="number", default=0.5),
        ]
        settings = FlowfileSettings(flow_arguments=args)
        assert len(settings.flow_arguments) == 2
        assert settings.flow_arguments[0].name == "input_path"

    def test_flow_settings_serialization_roundtrip(self):
        from flowfile_core.schemas.schemas import FlowfileSettings

        args = [FlowArgument(name="col", arg_type="string", default="id", required=True)]
        settings = FlowfileSettings(flow_arguments=args)
        dumped = settings.model_dump()
        loaded = FlowfileSettings.model_validate(dumped)
        assert len(loaded.flow_arguments) == 1
        assert loaded.flow_arguments[0].name == "col"
        assert loaded.flow_arguments[0].required is True

    def test_node_base_has_argument_bindings(self):
        from flowfile_core.schemas.input_schema import NodeBase

        # NodeBase is abstract-ish but we can test the field exists
        assert "argument_bindings" in NodeBase.model_fields

    def test_flow_settings_has_table_io_fields(self):
        from flowfile_core.schemas.schemas import FlowfileSettings

        settings = FlowfileSettings()
        assert settings.num_table_inputs is None
        assert settings.num_table_outputs is None

    def test_flow_settings_table_io_explicit(self):
        from flowfile_core.schemas.schemas import FlowfileSettings

        settings = FlowfileSettings(num_table_inputs=2, num_table_outputs=3)
        assert settings.num_table_inputs == 2
        assert settings.num_table_outputs == 3

    def test_flow_settings_table_io_serialization_roundtrip(self):
        from flowfile_core.schemas.schemas import FlowfileSettings

        settings = FlowfileSettings(num_table_inputs=1, num_table_outputs=2)
        dumped = settings.model_dump()
        loaded = FlowfileSettings.model_validate(dumped)
        assert loaded.num_table_inputs == 1
        assert loaded.num_table_outputs == 2

    def test_flow_graph_config_has_table_io_fields(self):
        from flowfile_core.schemas.schemas import FlowGraphConfig

        config = FlowGraphConfig()
        assert config.num_table_inputs is None
        assert config.num_table_outputs is None


# ============================================================================
# _count_table_io tests
# ============================================================================


class TestCountTableIO:
    """Tests for auto-detection of table inputs/outputs from flow structure."""

    def _make_flow_info(self, nodes: list[dict]):
        """Create a minimal flow_info-like object with a data dict."""
        from types import SimpleNamespace

        data = {}
        for n in nodes:
            setting_input = None
            if n.get("is_flow_input") or n.get("is_flow_output"):
                setting_input = SimpleNamespace(
                    is_flow_input=n.get("is_flow_input", False),
                    is_flow_output=n.get("is_flow_output", False),
                )
            node = SimpleNamespace(
                id=n["id"],
                type=n.get("type", "manual_input"),
                left_input_id=n.get("left_input_id"),
                right_input_id=n.get("right_input_id"),
                input_ids=n.get("input_ids", []),
                outputs=n.get("outputs", []),
                is_flow_input=n.get("is_flow_input", False),
                is_flow_output=n.get("is_flow_output", False),
                setting_input=setting_input,
            )
            data[n["id"]] = node
        return SimpleNamespace(data=data)

    def test_single_manual_input_single_output(self):
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "manual_input", "outputs": [2]},
            {"id": 2, "type": "filter", "input_ids": [1], "outputs": []},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 1
        assert num_out == 1

    def test_two_manual_inputs(self):
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "manual_input", "outputs": [3]},
            {"id": 2, "type": "manual_input", "outputs": [3]},
            {"id": 3, "type": "join", "input_ids": [1, 2], "outputs": []},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 2
        assert num_out == 1

    def test_reader_nodes_not_counted_as_inputs(self):
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "read", "outputs": [2]},
            {"id": 2, "type": "filter", "input_ids": [1], "outputs": []},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 0
        assert num_out == 1

    def test_mixed_reader_and_manual_input(self):
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "read", "outputs": [3]},
            {"id": 2, "type": "manual_input", "outputs": [3]},
            {"id": 3, "type": "join", "input_ids": [1, 2], "outputs": []},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 1  # only manual_input
        assert num_out == 1

    def test_multiple_terminal_nodes(self):
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "manual_input", "outputs": [2, 3]},
            {"id": 2, "type": "filter", "input_ids": [1], "outputs": []},
            {"id": 3, "type": "select", "input_ids": [1], "outputs": []},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 1
        assert num_out == 2

    def test_no_nodes_returns_zero_one(self):
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 0
        assert num_out == 1  # minimum 1 output

    def test_all_data_source_types_excluded(self):
        from flowfile_core.flowfile.node_designer.subflow_node import (
            _DATA_SOURCE_NODE_TYPES,
            _count_table_io,
        )

        for source_type in _DATA_SOURCE_NODE_TYPES:
            flow_info = self._make_flow_info([
                {"id": 1, "type": source_type, "outputs": []},
            ])
            num_in, num_out = _count_table_io(flow_info)
            assert num_in == 0, f"{source_type} should not be counted as input"

    def test_explicit_is_flow_input_flag(self):
        """When is_flow_input is set, it takes priority over heuristic."""
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "read", "outputs": [2], "is_flow_input": True},
            {"id": 2, "type": "filter", "input_ids": [1], "outputs": []},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 1  # read node is explicitly marked
        assert num_out == 1

    def test_explicit_is_flow_output_flag(self):
        """When is_flow_output is set, it takes priority over heuristic."""
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "manual_input", "outputs": [2, 3]},
            {"id": 2, "type": "filter", "input_ids": [1], "outputs": [], "is_flow_output": True},
            {"id": 3, "type": "select", "input_ids": [1], "outputs": []},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 1
        assert num_out == 1  # only node 2 is marked as output

    def test_explicit_flags_on_reader_node(self):
        """A reader marked is_flow_input should be counted even though it's a reader."""
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "read_csv", "outputs": [3], "is_flow_input": True},
            {"id": 2, "type": "read_csv", "outputs": [3]},
            {"id": 3, "type": "join", "input_ids": [1, 2], "outputs": []},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 1  # only the flagged one

    def test_both_flags_explicit(self):
        """Both is_flow_input and is_flow_output can be explicitly set."""
        from flowfile_core.flowfile.node_designer.subflow_node import _count_table_io

        flow_info = self._make_flow_info([
            {"id": 1, "type": "read", "outputs": [2], "is_flow_input": True},
            {"id": 2, "type": "filter", "input_ids": [1], "outputs": [3]},
            {"id": 3, "type": "output", "input_ids": [2], "outputs": [], "is_flow_output": True},
        ])
        num_in, num_out = _count_table_io(flow_info)
        assert num_in == 1
        assert num_out == 1


class TestFlowInputOutputFlags:
    """Tests for is_flow_input/is_flow_output on NodeBase and FlowfileNode."""

    def test_node_base_has_is_flow_input(self):
        from flowfile_core.schemas.input_schema import NodeBase

        assert "is_flow_input" in NodeBase.model_fields

    def test_node_base_has_is_flow_output(self):
        from flowfile_core.schemas.input_schema import NodeBase

        assert "is_flow_output" in NodeBase.model_fields

    def test_flowfile_node_has_flags(self):
        from flowfile_core.schemas.schemas import FlowfileNode

        node = FlowfileNode(id=1, type="read")
        assert node.is_flow_input is False
        assert node.is_flow_output is False

    def test_flowfile_node_flags_serialization(self):
        from flowfile_core.schemas.schemas import FlowfileNode

        node = FlowfileNode(id=1, type="read", is_flow_input=True, is_flow_output=True)
        dumped = node.model_dump()
        assert dumped["is_flow_input"] is True
        assert dumped["is_flow_output"] is True

        loaded = FlowfileNode.model_validate(dumped)
        assert loaded.is_flow_input is True
        assert loaded.is_flow_output is True
