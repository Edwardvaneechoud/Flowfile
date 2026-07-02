"""Contract tests: the node registry is the single source of truth and its
derived views match frozen snapshots of the pre-registry catalogs."""

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.node_registry import BUILTIN_REGISTRY, InputArity, get_node_spec

# Frozen snapshot of NODE_TYPE_TO_SETTINGS_CLASS before it became a derived
# view (node_type -> settings class name). Guards against silent drift.
SETTINGS_MAP_SNAPSHOT = {
    "manual_input": "NodeManualInput",
    "filter": "NodeFilter",
    "formula": "NodeFormula",
    "dynamic_rename": "NodeDynamicRename",
    "select": "NodeSelect",
    "sort": "NodeSort",
    "record_id": "NodeRecordId",
    "sample": "NodeSample",
    "random_split": "NodeRandomSplit",
    "unique": "NodeUnique",
    "group_by": "NodeGroupBy",
    "window_functions": "NodeWindowFunctions",
    "pivot": "NodePivot",
    "unpivot": "NodeUnpivot",
    "text_to_rows": "NodeTextToRows",
    "graph_solver": "NodeGraphSolver",
    "python_script": "NodePythonScript",
    "polars_code": "NodePolarsCode",
    "sql_query": "NodeSqlQuery",
    "join": "NodeJoin",
    "cross_join": "NodeCrossJoin",
    "fuzzy_match": "NodeFuzzyMatch",
    "record_count": "NodeRecordCount",
    "explore_data": "NodeExploreData",
    "union": "NodeUnion",
    "output": "NodeOutput",
    "api_response": "NodeApiResponse",
    "read": "NodeRead",
    "database_reader": "NodeDatabaseReader",
    "database_writer": "NodeDatabaseWriter",
    "cloud_storage_reader": "NodeCloudStorageReader",
    "cloud_storage_writer": "NodeCloudStorageWriter",
    "catalog_reader": "NodeCatalogReader",
    "catalog_writer": "NodeCatalogWriter",
    "kafka_source": "NodeKafkaSource",
    "google_analytics_reader": "NodeGoogleAnalyticsReader",
    "rest_api_reader": "NodeRestApiReader",
    "external_source": "NodeExternalSource",
    "promise": "NodePromise",
    "user_defined": "UserDefinedNode",
    "train_model": "NodeTrainModel",
    "apply_model": "NodeApplyModel",
    "evaluate_model": "NodeEvaluateModel",
    "wait_for": "NodeWaitFor",
}

NODES_WITH_DEFAULTS_SNAPSHOT = {"sample", "sort", "union", "select", "record_count"}


def test_settings_class_map_matches_snapshot():
    derived = {k: v.__name__ for k, v in BUILTIN_REGISTRY.settings_class_map().items()}
    assert derived == SETTINGS_MAP_SNAPSHOT


def test_schemas_module_exposes_derived_map():
    from flowfile_core.schemas import schemas

    assert {k: v.__name__ for k, v in schemas.NODE_TYPE_TO_SETTINGS_CLASS.items()} == SETTINGS_MAP_SNAPSHOT
    from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS

    assert NODE_TYPE_TO_SETTINGS_CLASS is schemas.NODE_TYPE_TO_SETTINGS_CLASS


def test_nodes_with_defaults_derived():
    from flowfile_core.configs import node_store

    assert BUILTIN_REGISTRY.node_types_with_default_settings() == NODES_WITH_DEFAULTS_SNAPSHOT
    assert node_store.nodes_with_defaults == NODES_WITH_DEFAULTS_SNAPSHOT
    assert node_store.check_if_has_default_setting("sort")
    assert not node_store.check_if_has_default_setting("filter")


def test_ai_classification_map_derived():
    from flowfile_core.ai.tools import classification

    derived = classification._NODE_CLASS_MAP
    assert derived == BUILTIN_REGISTRY.ai_classification_map()
    # Spot-check the buckets that drive executor routing.
    assert classification.classify_node_type("manual_input") == "source"
    assert classification.classify_node_type("pivot") == "dynamic"
    assert classification.classify_node_type("filter") == "static"
    assert classification.classify_node_type("promise") == "passthrough"
    assert classification.classify_node_type("never_heard_of_it") == "dynamic"


def test_every_template_has_exactly_one_spec():
    from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

    nodes_list, node_dict, node_defaults = get_all_standard_nodes()
    for template in nodes_list:
        spec = get_node_spec(template.item)
        assert spec is not None, f"template {template.item} has no NodeSpec"
        assert spec.template == template
    assert "polars_lazy_frame" in node_dict
    assert "polars_lazy_frame" not in node_defaults
    assert {t.item for t in nodes_list} == set(node_defaults)


def test_every_drawer_spec_has_matching_add_method():
    for spec in BUILTIN_REGISTRY:
        if spec.template is None or not spec.drawer_visible:
            continue
        if spec.node_type == "user_defined":
            continue
        assert hasattr(FlowGraph, f"add_{spec.node_type}"), (
            f"NodeSpec {spec.node_type} has no FlowGraph.add_{spec.node_type} method"
        )


def test_node_defaults_match_templates():
    from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

    _, _, node_defaults = get_all_standard_nodes()
    for item, default in node_defaults.items():
        spec = get_node_spec(item)
        assert default.node_name == spec.template.name
        assert default.node_type == spec.template.node_type
        assert default.transform_type == spec.template.transform_type
        assert bool(default.has_default_settings) == spec.has_default_settings


def test_input_arity_derivation():
    assert get_node_spec("read").input_arity is InputArity.SOURCE
    assert get_node_spec("filter").input_arity is InputArity.SINGLE
    assert get_node_spec("join").input_arity is InputArity.DOUBLE
    assert get_node_spec("union").input_arity is InputArity.MULTI


def test_routes_node_model_resolution_matches_registry():
    from flowfile_core.routes.routes import get_node_model

    for spec in BUILTIN_REGISTRY:
        if spec.settings_class is None:
            continue
        ref = get_node_model("node" + spec.node_type.replace("_", ""), node_type=spec.node_type)
        assert ref is spec.settings_class, f"{spec.node_type} resolved to {ref}"
    # Legacy reflective fallback still resolves types outside the settings map.
    from flowfile_core.schemas import input_schema

    assert get_node_model("nodedatasource", node_type="datasource") is input_schema.NodeDatasource
