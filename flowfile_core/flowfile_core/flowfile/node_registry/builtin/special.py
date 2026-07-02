"""Settings-only and dict-only node types (no drawer entry).

Generated from the pre-registry catalogs (NodeTemplate list, settings map,
AI classification map); maintained by hand from here on.
"""

from flowfile_core.flowfile.node_registry.spec import NodeSpec
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import NodeTemplate

SPECS: list[NodeSpec] = [
    NodeSpec(
        node_type="promise",
        settings_class=input_schema.NodePromise,
        ai_classification="passthrough",
    ),
    NodeSpec(
        node_type="user_defined",
        settings_class=input_schema.UserDefinedNode,
        ai_classification="dynamic",
    ),
    NodeSpec(
        node_type="polars_lazy_frame",
        settings_class=None,
        template=NodeTemplate(
            name="LazyFrame node",
            item="polars_lazy_frame",
            input=0,
            output=1,
            image="",
            node_type="input",
            transform_type="other",
            node_group="special",
            laziness="lazy",
        ),
        drawer_visible=False,
    ),
    # Legacy alias for the read node; its NodeDatasource settings class is
    # resolved via the reflective fallback in routes.get_node_model and must
    # stay out of the settings map (AI tools generate one tool per map entry).
    NodeSpec(
        node_type="datasource",
        settings_class=None,
    ),
]
