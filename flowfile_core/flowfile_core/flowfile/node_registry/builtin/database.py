"""Database source and sink nodes.

Generated from the pre-registry catalogs (NodeTemplate list, settings map,
AI classification map); maintained by hand from here on.
"""

from flowfile_core.flowfile.node_registry.spec import NodeSpec
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import NodeTag, NodeTemplate

SPECS: list[NodeSpec] = [
    NodeSpec(
        node_type="database_reader",
        settings_class=input_schema.NodeDatabaseReader,
        template=NodeTemplate(
            name="Read from Database",
            item="database_reader",
            input=0,
            output=1,
            image="database_reader.svg",
            node_type="input",
            transform_type="other",
            node_group="input",
            drawer_title="Database Reader",
            drawer_intro="Load data from database tables or queries",
            tags=[
                NodeTag.DATABASE,
                NodeTag.SQL,
                NodeTag.POSTGRES,
                NodeTag.MYSQL,
                NodeTag.SQL_SERVER,
                NodeTag.SNOWFLAKE,
                NodeTag.ORACLE,
                NodeTag.SQLITE,
                NodeTag.REDSHIFT,
                NodeTag.BIGQUERY,
                NodeTag.QUERY,
                NodeTag.TABLE,
            ],
        ),
        ai_classification="source",
    ),
    NodeSpec(
        node_type="database_writer",
        settings_class=input_schema.NodeDatabaseWriter,
        template=NodeTemplate(
            name="Write to Database",
            item="database_writer",
            input=1,
            output=0,
            image="database_writer.svg",
            node_type="output",
            transform_type="other",
            node_group="output",
            drawer_title="Database Writer",
            drawer_intro="Save data to database tables",
            tags=[
                NodeTag.DATABASE,
                NodeTag.SQL,
                NodeTag.POSTGRES,
                NodeTag.MYSQL,
                NodeTag.SNOWFLAKE,
                NodeTag.REDSHIFT,
                NodeTag.BIGQUERY,
                NodeTag.TABLE,
            ],
        ),
        ai_classification="static",
    ),
]
