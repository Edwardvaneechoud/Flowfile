"""External streaming/API source nodes.

Generated from the pre-registry catalogs (NodeTemplate list, settings map,
AI classification map); maintained by hand from here on.
"""

from flowfile_core.flowfile.node_registry.spec import NodeSpec
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import NodeTag, NodeTemplate

SPECS: list[NodeSpec] = [
    NodeSpec(
        node_type="external_source",
        settings_class=input_schema.NodeExternalSource,
        template=NodeTemplate(
            name="External source",
            item="external_source",
            input=0,
            output=1,
            image="external_source.svg",
            node_type="input",
            transform_type="other",
            node_group="input",
            prod_ready=False,
            drawer_title="External Source",
            drawer_intro="Connect to external data sources and APIs",
            tags=[NodeTag.API, NodeTag.REST, NodeTag.HTTP, NodeTag.EXTERNAL],
        ),
        ai_classification="source",
    ),
    NodeSpec(
        node_type="google_analytics_reader",
        settings_class=input_schema.NodeGoogleAnalyticsReader,
        template=NodeTemplate(
            name="Google Analytics",
            item="google_analytics_reader",
            input=0,
            output=1,
            image="google_analytics.svg",
            node_type="input",
            transform_type="other",
            node_group="input",
            drawer_title="Google Analytics",
            drawer_intro="Load reports from a Google Analytics 4 property",
            tags=[NodeTag.GOOGLE_ANALYTICS, NodeTag.GA4, NodeTag.ANALYTICS],
        ),
        ai_classification="source",
    ),
    NodeSpec(
        node_type="kafka_source",
        settings_class=input_schema.NodeKafkaSource,
        template=NodeTemplate(
            name="Kafka Source",
            item="kafka_source",
            input=0,
            output=1,
            image="kafka_source.svg",
            node_type="input",
            transform_type="other",
            node_group="input",
            drawer_title="Kafka Source",
            drawer_intro="Read data from a Kafka or Redpanda topic",
            tags=[NodeTag.KAFKA, NodeTag.REDPANDA, NodeTag.STREAMING, NodeTag.TOPIC],
        ),
        ai_classification="source",
    ),
    NodeSpec(
        node_type="rest_api_reader",
        settings_class=input_schema.NodeRestApiReader,
        template=NodeTemplate(
            name="REST API",
            item="rest_api_reader",
            input=0,
            output=1,
            image="rest_api_reader.svg",
            node_type="input",
            transform_type="other",
            node_group="input",
            drawer_title="REST API",
            drawer_intro="Read JSON data from a REST API with auth and pagination",
            tags=[NodeTag.REST, NodeTag.API, NodeTag.HTTP, NodeTag.JSON, NodeTag.PAGINATION],
        ),
        ai_classification="source",
    ),
]
