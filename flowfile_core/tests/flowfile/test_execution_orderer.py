"""Tests for execution_orderer: ExecutionStage, ExecutionPlan, parallel stage grouping,
and dependency-aware scheduling support (NodeDependencyGraph)."""

from unittest.mock import MagicMock, PropertyMock

import pytest

from flowfile_core.flowfile.util.execution_orderer import (
    ExecutionPlan,
    ExecutionStage,
    NodeDependencyGraph,
    build_dependency_graph,
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


# ---------------------------------------------------------------------------
# max_parallel_workers setting
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# NodeDependencyGraph / build_dependency_graph
# ---------------------------------------------------------------------------


class TestBuildDependencyGraph:
    def test_single_node(self):
        """A single node has zero pending predecessors and no successors."""
        n1 = _make_node(1)
        graph = build_dependency_graph([n1])
        assert graph.pending_count[1] == 0
        assert graph.successors.get(1, []) == []
        assert graph.initial_ready == [1]

    def test_linear_chain(self):
        """A → B → C: each non-root node has pending_count == 1."""
        n3 = _make_node(3)
        n2 = _make_node(2, leads_to=[n3])
        n1 = _make_node(1, leads_to=[n2])
        graph = build_dependency_graph([n1, n2, n3])

        assert graph.pending_count[1] == 0
        assert graph.pending_count[2] == 1
        assert graph.pending_count[3] == 1
        assert graph.successors[1] == [2]
        assert graph.successors[2] == [3]
        assert graph.initial_ready == [1]

    def test_diamond_graph(self):
        """
        Diamond: A → B, A → C, B → D, C → D
        D has pending_count == 2 (waits for both B and C).
        """
        n4 = _make_node(4)
        n2 = _make_node(2, leads_to=[n4])
        n3 = _make_node(3, leads_to=[n4])
        n1 = _make_node(1, leads_to=[n2, n3])
        graph = build_dependency_graph([n1, n2, n3, n4])

        assert graph.pending_count[1] == 0
        assert graph.pending_count[2] == 1
        assert graph.pending_count[3] == 1
        assert graph.pending_count[4] == 2
        assert set(graph.successors[1]) == {2, 3}
        assert graph.initial_ready == [1]

    def test_independent_nodes(self):
        """Three independent nodes are all immediately ready."""
        n1, n2, n3 = _make_node(1), _make_node(2), _make_node(3)
        graph = build_dependency_graph([n1, n2, n3])

        assert all(graph.pending_count[nid] == 0 for nid in [1, 2, 3])
        assert set(graph.initial_ready) == {1, 2, 3}

    def test_fan_in(self):
        """A → D, B → D, C → D: D waits for 3 predecessors."""
        n4 = _make_node(4)
        n1 = _make_node(1, leads_to=[n4])
        n2 = _make_node(2, leads_to=[n4])
        n3 = _make_node(3, leads_to=[n4])
        graph = build_dependency_graph([n1, n2, n3, n4])

        assert graph.pending_count[4] == 3
        assert set(graph.initial_ready) == {1, 2, 3}

    def test_fan_out(self):
        """A → B, A → C, A → D: B, C, D each have pending_count == 1."""
        n2, n3, n4 = _make_node(2), _make_node(3), _make_node(4)
        n1 = _make_node(1, leads_to=[n2, n3, n4])
        graph = build_dependency_graph([n1, n2, n3, n4])

        assert graph.pending_count[1] == 0
        for nid in [2, 3, 4]:
            assert graph.pending_count[nid] == 1
        assert set(graph.successors[1]) == {2, 3, 4}

    def test_out_of_plan_edges_ignored(self):
        """Edges to nodes not in the plan are not counted."""
        n_external = _make_node(99)
        n1 = _make_node(1, leads_to=[n_external])
        graph = build_dependency_graph([n1])

        assert graph.pending_count[1] == 0
        # n_external should not appear in successors or node_map
        assert 99 not in graph.node_map
        assert graph.successors.get(1, []) == []

    def test_node_map_populated(self):
        """node_map contains all planned nodes keyed by ID."""
        n1, n2 = _make_node(1), _make_node(2)
        graph = build_dependency_graph([n1, n2])
        assert set(graph.node_map.keys()) == {1, 2}
        assert graph.node_map[1] is n1
        assert graph.node_map[2] is n2


class TestExecutionPlanDependencyGraph:
    def test_dependency_graph_property(self):
        """ExecutionPlan.dependency_graph returns a valid NodeDependencyGraph."""
        n3 = _make_node(3)
        n2 = _make_node(2, leads_to=[n3])
        n1 = _make_node(1, leads_to=[n2])
        plan = ExecutionPlan(
            skip_nodes=[],
            stages=[
                ExecutionStage(nodes=[n1]),
                ExecutionStage(nodes=[n2]),
                ExecutionStage(nodes=[n3]),
            ],
        )
        graph = plan.dependency_graph
        assert isinstance(graph, NodeDependencyGraph)
        assert set(graph.node_map.keys()) == {1, 2, 3}
        assert graph.pending_count[1] == 0
        assert graph.pending_count[2] == 1
        assert graph.pending_count[3] == 1

    def test_user_scenario_asymmetric_stages(self):
        """Reproduces the original issue: fast node 2 should not block node 3.

        Graph:
            1 (read) → 2 (select) → 3 (sort) → 5 (union) → 6, 7, 8
            1 (read) → 9 (sort) → 5 (union)

        With stage-based execution, node 3 waits for node 9 (both in stage 1).
        With dependency-aware scheduling, node 3 can start as soon as node 2
        completes, because node 3 only depends on node 2.

        This test verifies the dependency graph reflects that node 3 has
        pending_count == 1 (only node 2), not blocked by node 9.
        """
        n6 = _make_node(6)
        n7 = _make_node(7)
        n8 = _make_node(8)
        n5 = _make_node(5, leads_to=[n6, n7, n8])
        n3 = _make_node(3, leads_to=[n5])
        n9 = _make_node(9, leads_to=[n5])
        n2 = _make_node(2, leads_to=[n3])
        n1 = _make_node(1, leads_to=[n2, n9])

        graph = build_dependency_graph([n1, n2, n3, n5, n6, n7, n8, n9])

        # Node 3 depends ONLY on node 2
        assert graph.pending_count[3] == 1
        # Node 5 (union) depends on both 3 and 9
        assert graph.pending_count[5] == 2
        # Node 9 depends ONLY on node 1
        assert graph.pending_count[9] == 1
        # Only node 1 is initially ready
        assert graph.initial_ready == [1]
        # Successors of node 2 include node 3 (not node 9)
        assert graph.successors[2] == [3]
        # Successors of node 9 include node 5
        assert graph.successors[9] == [5]
