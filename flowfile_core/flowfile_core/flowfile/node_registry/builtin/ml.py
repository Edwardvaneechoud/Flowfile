"""Machine-learning nodes.

Generated from the pre-registry catalogs (NodeTemplate list, settings map,
AI classification map); maintained by hand from here on.
"""

from flowfile_core.flowfile.node_registry.spec import NodeSpec
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import NodeTag, NodeTemplate

SPECS: list[NodeSpec] = [
    NodeSpec(
        node_type="apply_model",
        settings_class=input_schema.NodeApplyModel,
        template=NodeTemplate(
            name="Apply Model",
            item="apply_model",
            input=1,
            output=1,
            image="apply_model.svg",
            node_type="process",
            transform_type="wide",
            node_group="ml",
            drawer_title="Apply ML Model",
            drawer_intro="Score data with an upstream Train Model node, or with a trained model from the catalog",
            tags=[NodeTag.ML, NodeTag.MACHINE_LEARNING, NodeTag.PREDICT, NodeTag.SCORE, NodeTag.MODEL],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="evaluate_model",
        settings_class=input_schema.NodeEvaluateModel,
        template=NodeTemplate(
            name="Evaluate Model",
            item="evaluate_model",
            input=1,
            output=1,
            image="evaluate_model.svg",
            node_type="process",
            transform_type="narrow",
            node_group="ml",
            drawer_title="Evaluate Model",
            drawer_intro="Compare actual vs predicted columns and compute quality metrics",
            tags=[NodeTag.ML, NodeTag.MACHINE_LEARNING, NodeTag.EVALUATE, NodeTag.METRICS, NodeTag.MODEL],
        ),
        ai_classification="static",
    ),
    NodeSpec(
        node_type="train_model",
        settings_class=input_schema.NodeTrainModel,
        template=NodeTemplate(
            name="Train Model",
            item="train_model",
            input=1,
            output=1,
            image="train_model.svg",
            node_type="process",
            transform_type="other",
            node_group="ml",
            drawer_title="Train ML Model",
            drawer_intro="Fit a regression or classification model; optionally save it to the catalog",
            tags=[
                NodeTag.ML,
                NodeTag.MACHINE_LEARNING,
                NodeTag.TRAIN,
                NodeTag.MODEL,
                NodeTag.REGRESSION,
                NodeTag.CLASSIFICATION,
            ],
        ),
        ai_classification="static",
    ),
]
