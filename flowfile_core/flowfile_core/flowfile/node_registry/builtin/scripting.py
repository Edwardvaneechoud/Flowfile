"""Code-execution nodes (Python, Polars code, SQL).

Generated from the pre-registry catalogs (NodeTemplate list, settings map,
AI classification map); maintained by hand from here on.
"""

from flowfile_core.flowfile.node_registry.spec import NodeSpec
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import NodeTag, NodeTemplate

SPECS: list[NodeSpec] = [
    NodeSpec(
        node_type="polars_code",
        settings_class=input_schema.NodePolarsCode,
        template=NodeTemplate(
            name="Polars code",
            item="polars_code",
            input=10,
            output=1,
            image="polars_code.svg",
            multi=True,
            node_type="process",
            transform_type="narrow",
            node_group="transform",
            can_be_start=True,
            drawer_title="Polars Code",
            drawer_intro="Write custom Polars DataFrame transformations",
            laziness="conditional",
            tags=[
                NodeTag.POLARS,
                NodeTag.CODE,
                NodeTag.PYTHON,
                NodeTag.SCRIPT,
                NodeTag.CUSTOM,
                NodeTag.DATAFRAME,
                NodeTag.TRANSFORM,
            ],
        ),
        ai_classification="dynamic",
    ),
    NodeSpec(
        node_type="python_script",
        settings_class=input_schema.NodePythonScript,
        template=NodeTemplate(
            name="Python Script",
            item="python_script",
            input=10,
            output=1,
            image="python_code.svg",
            multi=True,
            node_type="process",
            transform_type="narrow",
            node_group="transform",
            can_be_start=True,
            drawer_title="Python Script",
            drawer_intro="Execute Python code on an isolated kernel container",
            tags=[NodeTag.PYTHON, NodeTag.CODE, NodeTag.SCRIPT, NodeTag.KERNEL, NodeTag.CUSTOM, NodeTag.TRANSFORM],
        ),
        ai_classification="dynamic",
    ),
    NodeSpec(
        node_type="sql_query",
        settings_class=input_schema.NodeSqlQuery,
        template=NodeTemplate(
            name="SQL Query",
            item="sql_query",
            input=10,
            output=1,
            image="sql_query.svg",
            multi=True,
            node_type="process",
            transform_type="narrow",
            node_group="transform",
            can_be_start=True,
            drawer_title="SQL Query",
            drawer_intro="Write SQL queries against connected data sources",
            laziness="lazy",
            tags=[NodeTag.SQL, NodeTag.QUERY, NodeTag.DUCKDB],
        ),
        ai_classification="dynamic",
    ),
]
