"""Export must not mutate the live flow or the process: parameter isolation, memoised
formula translation, and validation that never imports the top-level ``flowfile`` package."""

import os
import sys

import pytest

from flowfile_core.flowfile.code_generator import code_generator as cg
from flowfile_core.flowfile.code_generator.code_generator import (
    FlowGraphCodeConverter,
    FlowGraphToFlowFrameConverter,
    export_flow_to_flowframe,
    export_flow_to_polars,
)
from flowfile_core.flowfile.code_generator.project_exporter import export_flow_to_project
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.schemas import input_schema, schemas, transform_schema

_SINGLE_FILE_ENV = ("FLOWFILE_SINGLE_FILE_MODE", "FLOWFILE_WORKER_PORT")


def _manual_input(flow: FlowGraph, node_id: int = 1) -> None:
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData(
                columns=[input_schema.MinimalFieldInfo(name="a", data_type="Integer")], data=[[1, 2, 3, 4, 5]]
            ),
        )
    )


def _base_flow(flow_id: int = 1) -> FlowGraph:
    settings = schemas.FlowSettings(
        flow_id=flow_id, execution_mode="Performance", execution_location="local", path="/tmp/test_flow"
    )
    return FlowGraph(flow_settings=settings, name="export_hygiene")


def _formula(flow: FlowGraph, node_id: int, depends_on: int, name: str, function: str, data_type: str) -> None:
    flow.add_formula(
        input_schema.NodeFormula(
            flow_id=flow.flow_id,
            node_id=node_id,
            depending_on_id=depends_on,
            function=transform_schema.FunctionInput(
                field=transform_schema.FieldInput(name=name, data_type=data_type), function=function
            ),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))


def _parameterised_flow() -> FlowGraph:
    """Filter, formula and Polars-code nodes that all reference flow parameters."""
    flow = _base_flow()
    flow.flow_settings.parameters = [
        FlowParameter(name="x", default_value="2", type="integer"),
        FlowParameter(name="limit", default_value="3", type="integer"),
    ]
    _manual_input(flow)
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[a] > ${x}"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    _formula(flow, 3, 2, "scaled", "[a] * ${x}", "Integer")
    flow.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=1,
            node_id=4,
            depending_on_ids=[3],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="output_df = input_df.head(${limit})"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(3, 4))
    return flow


def _live_snapshot(flow: FlowGraph) -> dict[int, tuple[object, str, object]]:
    return {node.node_id: (node.setting_input, node.setting_input.model_dump_json(), node._hash) for node in flow.nodes}


_EXPORTERS = [
    pytest.param(export_flow_to_polars, id="polars"),
    pytest.param(export_flow_to_flowframe, id="flowframe"),
    pytest.param(export_flow_to_project, id="project"),
]


@pytest.mark.parametrize("export", _EXPORTERS)
def test_export_never_writes_sentinels_into_live_settings(export, monkeypatch):
    flow = _parameterised_flow()
    seen_during_export: list[str] = []
    original = FlowGraphCodeConverter._generate_node_code

    def spying(self, node):
        original(self, node)
        seen_during_export.extend(n.setting_input.model_dump_json() for n in self.flow_graph.nodes)

    monkeypatch.setattr(FlowGraphCodeConverter, "_generate_node_code", spying)
    export(flow)

    assert seen_during_export
    assert not [dump for dump in seen_during_export if "__FF_PARAM_" in dump]


@pytest.mark.parametrize("export", _EXPORTERS)
def test_failed_export_leaves_live_settings_untouched(export, monkeypatch):
    flow = _parameterised_flow()
    before = _live_snapshot(flow)

    def boom(self, *args, **kwargs):
        raise RuntimeError("handler failed mid-export")

    monkeypatch.setattr(FlowGraphCodeConverter, "_handle_polars_code", boom)
    monkeypatch.setattr(FlowGraphToFlowFrameConverter, "_handle_polars_code", boom)
    with pytest.raises(RuntimeError, match="mid-export"):
        export(flow)

    after = _live_snapshot(flow)
    assert after.keys() == before.keys()
    for node_id, (settings, dump, node_hash) in before.items():
        assert after[node_id][0] is settings
        assert after[node_id][1] == dump
        assert after[node_id][2] == node_hash


@pytest.mark.parametrize("export", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])
def test_parameterised_export_still_emits_function_arguments(export):
    flow = _parameterised_flow()
    code = export(flow)

    assert "__FF_PARAM_" not in code and "${" not in code
    assert ".head(limit)" in code if export is export_flow_to_polars else '"output_df = input_df.head({limit})"' in code
    namespace: dict = {}
    exec(code, namespace)
    for kwargs, expected in (({}, [6, 8, 10]), ({"x": 3, "limit": 1}, [12])):
        result = namespace["run_etl_pipeline"](**kwargs)
        frame = result.data if hasattr(result, "data") else result
        assert frame.collect()["scaled"].to_list() == expected


def test_formula_translation_is_memoised_across_exports():
    flow = _base_flow()
    _manual_input(flow)
    _formula(flow, 2, 1, "doubled", "[a] * 2", "Auto")
    _formula(flow, 3, 2, "shifted", "[a] + 10", "Auto")
    cg._try_translate_to_ff_code.cache_clear()

    first = export_flow_to_flowframe(flow)
    second = export_flow_to_flowframe(flow)

    info = cg._try_translate_to_ff_code.cache_info()
    assert first == second
    assert info.misses == 2
    assert info.hits >= 2


def test_validation_namespace_mirrors_the_flowfile_package(monkeypatch):
    """Every validation name is the very object ``flowfile`` exports, and the expression surface
    ``flowfile`` re-exports is covered in full; names neither package has stay absent."""
    for key in _SINGLE_FILE_ENV:
        if key in os.environ:
            monkeypatch.setenv(key, os.environ[key])
        else:
            monkeypatch.delenv(key, raising=False)
    import flowfile

    namespace = cg._ff_validation_namespace()
    names = set(vars(namespace))
    assert names == set(cg.FF_VALIDATION_NAMES)
    for name in names:
        assert name in flowfile.__all__
        assert getattr(namespace, name) is getattr(flowfile, name), name

    surface_modules = ("flowfile_frame.expr", "flowfile_frame.selectors", "polars.datatypes")
    expected = {
        name
        for name in flowfile.__all__
        if getattr(getattr(flowfile, name), "__module__", "").startswith(surface_modules)
    }
    assert expected == names
    for name in ("coalesce", "concat_str", "concat_list", "first", "last", "corr", "cov", "implode"):
        assert hasattr(namespace, name) == hasattr(flowfile, name), name


def test_export_does_not_import_flowfile_or_touch_environ(monkeypatch):
    monkeypatch.delitem(sys.modules, "flowfile", raising=False)
    for key in _SINGLE_FILE_ENV:
        monkeypatch.delenv(key, raising=False)
    cg._try_translate_to_ff_code.cache_clear()
    cg._ff_validation_namespace.cache_clear()
    flow = _base_flow()
    _manual_input(flow)
    _formula(flow, 2, 1, "doubled", "[a] * 2", "Auto")
    _formula(flow, 3, 2, "as_text", "[a] + 1", "String")
    environ_before = dict(os.environ)

    code = export_flow_to_flowframe(flow)

    assert "fl.col(" in code
    assert dict(os.environ) == environ_before
    assert "flowfile" not in sys.modules
