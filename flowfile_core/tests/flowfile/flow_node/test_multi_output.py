import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_node.multi_output import (
    DEFAULT_OUTPUT_HANDLE,
    NamedOutputs,
    output_handle,
    output_handle_index,
)


class TestOutputHandleHelpers:
    def test_default_handle_is_output_zero(self):
        assert DEFAULT_OUTPUT_HANDLE == "output-0"

    def test_output_handle_formats_index(self):
        assert output_handle(0) == "output-0"
        assert output_handle(3) == "output-3"

    def test_output_handle_index_parses(self):
        assert output_handle_index("output-0") == 0
        assert output_handle_index("output-7") == 7

    def test_output_handle_index_rejects_bad_input(self):
        with pytest.raises(ValueError):
            output_handle_index("input-0")
        with pytest.raises(ValueError):
            output_handle_index("output-foo")

    def test_helpers_round_trip(self):
        for i in (0, 1, 5, 42):
            assert output_handle_index(output_handle(i)) == i


class TestNamedOutputs:
    def _engines(self):
        return {
            "alpha": FlowDataEngine([{"a": 1}]),
            "beta": FlowDataEngine([{"b": "x"}]),
        }

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            NamedOutputs({})

    def test_preserves_label_order(self):
        no = NamedOutputs(self._engines())
        assert no.labels == ["alpha", "beta"]

    def test_by_handle_maps_positional(self):
        no = NamedOutputs(self._engines())
        by_handle = no.by_handle()
        assert list(by_handle.keys()) == ["output-0", "output-1"]
        # Labels are dropped on the positional map — routing is by position.
        assert by_handle["output-0"] is no.engines[0]
        assert by_handle["output-1"] is no.engines[1]

    def test_default_returns_first_engine(self):
        no = NamedOutputs(self._engines())
        assert no.default() is no.engines[0]

    def test_engines_in_order(self):
        no = NamedOutputs(self._engines())
        assert [e.columns[0] for e in no.engines] == ["a", "b"]
