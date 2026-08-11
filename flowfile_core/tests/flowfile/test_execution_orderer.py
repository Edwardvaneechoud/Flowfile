"""Tests for execution_orderer: ExecutionStage, ExecutionPlan, and parallel stage grouping."""

from unittest.mock import MagicMock, PropertyMock

import pytest

from flowfile_core.flowfile.util.execution_orderer import (
    ExecutionPlan,
    ExecutionStage,
    compute_execution_plan,
    determine_execution_order,
)
from flowfile_core.schemas.schemas import FlowGraphConfig, FlowSettings


def _make_node(node_id: int, leads_to=None, is_correct: bool = True):
    """Create a minimal mock FlowNode with the given id and outgoing edges."""
    node = MagicMock()
    type(node).node_id = PropertyMock(return_value=node_id)
    type(node).is_correct = PropertyMock(return_value=is_correct)
    node.leads_to_nodes = leads_to or []
    node.__repr__ = lambda self: f"Node({node_id})"
    node.__str__ = lambda self: f"Node({node_id})"
    return node


# ExecutionStage


class TestExecutionStage:
    def test_len(self):
        n1, n2 = _make_node(1), _make_node(2)
        stage = ExecutionStage(nodes=[n1, n2])
        assert len(stage) == 2

    def test_iter(self):
        n1, n2 = _make_node(1), _make_node(2)
        stage = ExecutionStage(nodes=[n1, n2])
        assert list(stage) == [n1, n2]

    def test_empty_stage(self):
        stage = ExecutionStage(nodes=[])
        assert len(stage) == 0
        assert list(stage) == []


# ExecutionPlan


class TestExecutionPlan:
    def test_all_nodes_flattens_stages(self):
        n1, n2, n3 = _make_node(1), _make_node(2), _make_node(3)
        plan = ExecutionPlan(
            skip_nodes=[],
            stages=[ExecutionStage(nodes=[n1, n2]), ExecutionStage(nodes=[n3])],
        )
        assert plan.all_nodes == [n1, n2, n3]

    def test_node_count(self):
        n1, n2, n3 = _make_node(1), _make_node(2), _make_node(3)
        plan = ExecutionPlan(
            skip_nodes=[],
            stages=[ExecutionStage(nodes=[n1, n2]), ExecutionStage(nodes=[n3])],
        )
        assert plan.node_count == 3

    def test_empty_plan(self):
        plan = ExecutionPlan(skip_nodes=[], stages=[])
        assert plan.all_nodes == []
        assert plan.node_count == 0


# determine_execution_order — stage grouping


class TestDetermineExecutionOrder:
    def test_single_node(self):
        """A single node with no edges produces one stage."""
        n1 = _make_node(1)
        stages = determine_execution_order([n1])
        assert len(stages) == 1
        assert list(stages[0]) == [n1]

    def test_independent_nodes_same_stage(self):
        """Three independent nodes should all be in one stage."""
        n1, n2, n3 = _make_node(1), _make_node(2), _make_node(3)
        stages = determine_execution_order([n1, n2, n3])
        assert len(stages) == 1
        assert set(stages[0]) == {n1, n2, n3}

    def test_linear_chain(self):
        """A → B → C should produce three sequential stages."""
        n3 = _make_node(3)
        n2 = _make_node(2, leads_to=[n3])
        n1 = _make_node(1, leads_to=[n2])
        stages = determine_execution_order([n1, n2, n3])
        assert len(stages) == 3
        assert list(stages[0]) == [n1]
        assert list(stages[1]) == [n2]
        assert list(stages[2]) == [n3]

    def test_diamond_graph(self):
        """
        Diamond: A → B, A → C, B → D, C → D
        Expected stages: [A], [B, C], [D]
        """
        n4 = _make_node(4)
        n2 = _make_node(2, leads_to=[n4])
        n3 = _make_node(3, leads_to=[n4])
        n1 = _make_node(1, leads_to=[n2, n3])
        stages = determine_execution_order([n1, n2, n3, n4])
        assert len(stages) == 3
        assert list(stages[0]) == [n1]
        assert set(stages[1]) == {n2, n3}
        assert list(stages[2]) == [n4]

    def test_two_independent_chains(self):
        """
        Two independent chains: A → B and C → D
        Stage 0: [A, C] (parallel), Stage 1: [B, D] (parallel)
        """
        n2 = _make_node(2)
        n1 = _make_node(1, leads_to=[n2])
        n4 = _make_node(4)
        n3 = _make_node(3, leads_to=[n4])
        stages = determine_execution_order([n1, n2, n3, n4])
        assert len(stages) == 2
        stage0_ids = {n.node_id for n in stages[0]}
        stage1_ids = {n.node_id for n in stages[1]}
        assert stage0_ids == {1, 3}
        assert stage1_ids == {2, 4}

    def test_wide_fan_out(self):
        """
        A → B, A → C, A → D
        Stage 0: [A], Stage 1: [B, C, D]
        """
        n2, n3, n4 = _make_node(2), _make_node(3), _make_node(4)
        n1 = _make_node(1, leads_to=[n2, n3, n4])
        stages = determine_execution_order([n1, n2, n3, n4])
        assert len(stages) == 2
        assert list(stages[0]) == [n1]
        assert set(stages[1]) == {n2, n3, n4}

    def test_fan_in(self):
        """
        A → D, B → D, C → D
        Stage 0: [A, B, C] (parallel), Stage 1: [D]
        """
        n4 = _make_node(4)
        n1 = _make_node(1, leads_to=[n4])
        n2 = _make_node(2, leads_to=[n4])
        n3 = _make_node(3, leads_to=[n4])
        stages = determine_execution_order([n1, n2, n3, n4])
        assert len(stages) == 2
        assert set(stages[0]) == {n1, n2, n3}
        assert list(stages[1]) == [n4]

    def test_cycle_detection(self):
        """A cycle should raise an exception."""
        n2 = _make_node(2)
        n1 = _make_node(1, leads_to=[n2])
        # Create a cycle: n2 → n1 (but n1 already leads to n2)
        n2.leads_to_nodes = [n1]
        with pytest.raises(Exception, match="Cycle detected"):
            determine_execution_order([n1, n2])

    def test_flow_starts_respected(self):
        """When flow_starts is provided, only those zero-in-degree nodes seed the queue."""
        n1, n2 = _make_node(1), _make_node(2)
        # If flow_starts omits an independent node, that node is unreachable and
        # excluded from the stages — not misreported as a cycle.
        stages = determine_execution_order([n1, n2], flow_starts=[n1])
        assert len(stages) == 1
        assert list(stages[0]) == [n1]

        # When flow_starts includes all independent nodes, both appear in stage 0
        stages = determine_execution_order([n1, n2], flow_starts=[n1, n2])
        assert len(stages) == 1
        assert set(stages[0]) == {n1, n2}

    def test_unreachable_chain_excluded_without_cycle_error(self):
        """A disconnected chain not in flow_starts is excluded, not reported as a cycle.

        Mirrors a flow where an upstream connection was removed: the orphaned
        nodes keep their internal edges but are unreachable from the starts.
        """
        n2 = _make_node(2)
        n1 = _make_node(1, leads_to=[n2])
        n5 = _make_node(5)
        n4 = _make_node(4, leads_to=[n5])
        n3 = _make_node(3, leads_to=[n4])
        stages = determine_execution_order([n1, n2, n3, n4, n5], flow_starts=[n1])
        staged_ids = {node.node_id for stage in stages for node in stage}
        assert staged_ids == {1, 2}

    def test_cycle_still_detected_with_flow_starts(self):
        """A genuine cycle raises even when valid flow_starts are provided."""
        n2, n3 = _make_node(2), _make_node(3)
        n1 = _make_node(1, leads_to=[n2])
        n2.leads_to_nodes = [n3]
        n3.leads_to_nodes = [n2]
        with pytest.raises(Exception, match="Cycle detected"):
            determine_execution_order([n1, n2, n3], flow_starts=[n1])

    def test_flow_starts_with_chain(self):
        """flow_starts seeds the queue; downstream nodes are discovered normally."""
        n2 = _make_node(2)
        n1 = _make_node(1, leads_to=[n2])
        stages = determine_execution_order([n1, n2], flow_starts=[n1])
        assert len(stages) == 2
        assert list(stages[0]) == [n1]
        assert list(stages[1]) == [n2]


# compute_execution_plan


class TestComputeExecutionPlan:
    def test_returns_execution_plan_type(self):
        n1 = _make_node(1)
        plan = compute_execution_plan([n1])
        assert isinstance(plan, ExecutionPlan)

    def test_skip_nodes_populated_for_incorrect_nodes(self):
        """Incorrect nodes should appear in skip_nodes."""
        n_bad = _make_node(10, is_correct=False)
        n_good = _make_node(20)
        plan = compute_execution_plan([n_bad, n_good])
        skip_ids = {n.node_id for n in plan.skip_nodes}
        assert 10 in skip_ids
        assert len(plan.stages) >= 1

    def test_downstream_of_incorrect_skipped(self):
        """Nodes downstream of incorrect nodes should be skipped."""
        n_downstream = _make_node(20)
        n_bad = _make_node(10, leads_to=[n_downstream], is_correct=False)
        plan = compute_execution_plan([n_bad, n_downstream])
        skip_ids = {n.node_id for n in plan.skip_nodes}
        assert 10 in skip_ids
        assert 20 in skip_ids
        assert plan.node_count == 0

    def test_skip_propagates_through_entire_downstream_chain(self):
        """An incorrect node skips its whole downstream closure, not just one level."""
        n4 = _make_node(4)
        n3 = _make_node(3, leads_to=[n4])
        n2 = _make_node(2, leads_to=[n3])
        n_bad = _make_node(1, leads_to=[n2], is_correct=False)
        plan = compute_execution_plan([n_bad, n2, n3, n4])
        skip_ids = {n.node_id for n in plan.skip_nodes}
        assert skip_ids == {1, 2, 3, 4}
        staged_ids = {node.node_id for stage in plan.stages for node in stage}
        assert staged_ids == set()

    def test_disconnected_branch_skipped_while_connected_dag_runs(self):
        """Repro of a mid-flow disconnect: the orphaned chain is skipped, the rest staged.

        Chain 1 → 2 with a separate orphaned chain 3 → 4 → 5 whose head lost its
        input (is_correct False). Previously this raised a false 'Cycle detected'.
        """
        n2 = _make_node(2)
        n1 = _make_node(1, leads_to=[n2])
        n5 = _make_node(5)
        n4 = _make_node(4, leads_to=[n5])
        n3 = _make_node(3, leads_to=[n4], is_correct=False)
        plan = compute_execution_plan([n1, n2, n3, n4, n5], flow_starts=[n1])
        skip_ids = {n.node_id for n in plan.skip_nodes}
        assert skip_ids == {3, 4, 5}
        staged_ids = {node.node_id for stage in plan.stages for node in stage}
        assert staged_ids == {1, 2}

    def test_diamond_with_one_bad_branch(self):
        """A → B(bad) → D and A → C → D: B and D skip, A and C stay runnable.

        Skipped nodes must not leak back into the stages through their live
        edges from runnable nodes — node_count is the run-progress denominator.
        """
        n_d = _make_node(4)
        n_c = _make_node(3, leads_to=[n_d])
        n_b = _make_node(2, leads_to=[n_d], is_correct=False)
        n_a = _make_node(1, leads_to=[n_b, n_c])
        plan = compute_execution_plan([n_a, n_b, n_c, n_d], flow_starts=[n_a])
        skip_ids = {n.node_id for n in plan.skip_nodes}
        assert skip_ids == {2, 4}
        staged_ids = {node.node_id for stage in plan.stages for node in stage}
        assert staged_ids == {1, 3}
        assert plan.node_count == 2


# max_parallel_workers setting


class TestMaxParallelWorkersSetting:
    def test_default_value(self):
        config = FlowGraphConfig(name="test", path=".")
        assert config.max_parallel_workers == 4

    def test_custom_value(self):
        config = FlowGraphConfig(name="test", path=".", max_parallel_workers=8)
        assert config.max_parallel_workers == 8

    def test_minimum_is_one(self):
        with pytest.raises(Exception):
            FlowGraphConfig(name="test", path=".", max_parallel_workers=0)

    def test_disable_parallelism(self):
        config = FlowGraphConfig(name="test", path=".", max_parallel_workers=1)
        assert config.max_parallel_workers == 1

    def test_inherited_by_flow_settings(self):
        settings = FlowSettings(name="test", path=".", max_parallel_workers=2)
        assert settings.max_parallel_workers == 2

    def test_from_flow_settings_input(self):
        config = FlowGraphConfig(name="test", path=".", max_parallel_workers=6)
        settings = FlowSettings.from_flow_settings_input(config)
        assert settings.max_parallel_workers == 6
