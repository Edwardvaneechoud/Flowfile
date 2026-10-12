"""What the transform and model operations take: one frozen dataclass per ``OperationKind`` a node runs whole.

They travel like the write payloads (``write_payloads``): under the single key ``PAYLOAD_KEY`` of the ``OpPayload``,
read with ``unpack``, the same dataclass for every backend. ``LocalBackend`` runs the operation in this process; a
backend that crosses a process builds what its transport needs from the dataclass. A frame crosses as the
``pl.LazyFrame`` itself, which such a backend serialises when it sends it. Some fields are in-process hooks (a node
logger, the class a custom node was placed with): they are read in core and never sent anywhere, so no field carries
a secret, decrypted or not.

Every payload names the flow and node the compute files the job and its logs under, and a ``job_id``:
``cancel(JobHandle(backend.id, job_id))`` stops the operation while it runs, and a backend that runs it as a task of
its own files the task under that id. A caller that passes an id derived from the node's hash has a rerun's task
replace the previous run's rather than add another.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl

from flowfile_core.configs.flow_logger import NodeLogger
from flowfile_core.flowfile.execution.write_payloads import PAYLOAD_KEY, new_job_id, unpack
from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase
from flowfile_core.schemas.input_schema import NodeFuzzyMatch, NodeRandomSplit

__all__ = [
    "PAYLOAD_KEY",
    "CustomNodePayload",
    "FuzzyJoinPayload",
    "MlApplyPayload",
    "MlTrainPayload",
    "RandomSplitPayload",
    "unpack",
]


@dataclass(frozen=True, kw_only=True)
class FuzzyJoinPayload:
    """Fuzzy-match the rows of one plan against another's.

    Attributes:
        left: The left rows.
        right: The right rows.
        settings: The node's settings; its ``join_input`` (copied, never changed) holds the fuzzy mappings and the
            column selections of both sides.
        node_logger: The node's logger, which an in-process match reports its progress to; read in core only.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running match by, and the id a worker task for it is filed under.
    """

    left: pl.LazyFrame
    right: pl.LazyFrame
    settings: NodeFuzzyMatch
    node_logger: NodeLogger | None = None
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True, kw_only=True)
class RandomSplitPayload:
    """Partition a plan's rows at random into the node's labelled splits.

    Attributes:
        plan: The rows to split.
        settings: The node's settings: its ``splits`` (a name and a percentage each, in output order) and its
            ``seed``; without a seed every run draws a new one.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running split by, and the id a worker task for it is filed under.
    """

    plan: pl.LazyFrame
    settings: NodeRandomSplit
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True, kw_only=True)
class CustomNodePayload:
    """Run a custom node's ``process`` on its inputs.

    Attributes:
        inputs: The node's input rows, in wiring order.
        custom_node: The instance the node was placed with; read in core only. An in-process run builds a fresh
            instance of its class from ``settings_values``; a backend that sends the node elsewhere resolves the
            secrets its ``SecretSelector`` fields select to their ``$ffsec$`` ciphertext from it, so they are looked
            up only where they are sent.
        node_source: The source of the node's file, ``None`` for a class defined without one; such a node can only
            run in core.
        class_name: The class to run from ``node_source``.
        settings_values: The node's settings values by section, with the flow's parameters resolved.
        output_names: The node's declared outputs, in output order.
        user_id: The user the node runs as, whose secrets it reads; ``None`` runs it without a secret context.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running node by, and the id a worker task for it is filed under.
    """

    inputs: tuple[pl.LazyFrame, ...]
    custom_node: CustomNodeBase
    node_source: str | None
    class_name: str | None
    settings_values: dict[str, Any]
    output_names: list[str]
    user_id: int | None
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True, kw_only=True)
class MlTrainPayload:
    """Fit a model on a plan's rows and write it to a file.

    Attributes:
        plan: The training rows.
        staging_path: The file the trained model is written to.
        model_type: The trainer to fit (``shared.ml.trainers``).
        target_column: The column the model predicts.
        feature_columns: The columns the model reads.
        params: The trainer's parameters.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running fit by, and the id a worker task for it is filed under.
    """

    plan: pl.LazyFrame
    staging_path: str
    model_type: str
    target_column: str
    feature_columns: list[str]
    params: dict[str, Any]
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)


@dataclass(frozen=True, kw_only=True)
class MlApplyPayload:
    """Score a plan's rows with a trained model, adding its prediction as a column.

    Attributes:
        plan: The rows to score.
        model_path: The model file to score them with.
        output_column: The column the prediction is written to.
        flow_id: The flow the compute files the job and its logs under.
        node_id: The node the compute files the job and its logs under.
        job_id: The id ``cancel`` reaches the running scoring by, and the id a worker task for it is filed under.
    """

    plan: pl.LazyFrame
    model_path: str
    output_column: str
    flow_id: int
    node_id: int | str
    job_id: str = field(default_factory=new_job_id)
