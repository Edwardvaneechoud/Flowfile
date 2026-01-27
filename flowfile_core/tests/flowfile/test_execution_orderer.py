"""Tests for execution_orderer: ExecutionStage, ExecutionPlan, and parallel stage grouping."""

from unittest.mock import MagicMock, PropertyMock

import pytest

from flowfile_core.flowfile.util.execution_orderer import (
    ExecutionPlan,
    ExecutionStage,
    compute_execution_plan,
    determine_execution_order,
)


def _make_node(node_id: int, leads_to=None, is_correct: bool = True):
    """Create a minimal mock FlowNode with the given id and outgoing edges."""
    node = MagicMock()
    type(node).node_id = PropertyMock(return_value=node_id)
    type(node).is_correct = PropertyMock(return_value=is_correct)
    node.leads_to_nodes = leads_to or []
    node.__repr__ = lambda self: f"Node({node_id})"
    node.__str__ = lambda self: f"Node({node_id})"
    return node


# ---------------------------------------------------------------------------
# ExecutionStage
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# ExecutionPlan
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# determine_execution_order — stage grouping
# ---------------------------------------------------------------------------


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
        # If flow_starts omits an independent node, that node is unreachable
        # and the cycle check raises. This is existing behaviour.
        with pytest.raises(Exception, match="Cycle detected"):
            determine_execution_order([n1, n2], flow_starts=[n1])

        # When flow_starts includes all independent nodes, both appear in stage 0
        stages = determine_execution_order([n1, n2], flow_starts=[n1, n2])
        assert len(stages) == 1
        assert set(stages[0]) == {n1, n2}

    def test_flow_starts_with_chain(self):
        """flow_starts seeds the queue; downstream nodes are discovered normally."""
        n2 = _make_node(2)
        n1 = _make_node(1, leads_to=[n2])
        stages = determine_execution_order([n1, n2], flow_starts=[n1])
        assert len(stages) == 2
        assert list(stages[0]) == [n1]
        assert list(stages[1]) == [n2]


# ---------------------------------------------------------------------------
# compute_execution_plan
# ---------------------------------------------------------------------------


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
        # No stages should contain skipped nodes
        assert plan.node_count == 0
