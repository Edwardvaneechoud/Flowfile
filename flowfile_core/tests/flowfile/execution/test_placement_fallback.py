"""A node the backend it is placed on cannot run runs on the default backend instead, with an exchange store.

Under the mixed policy (``FLOWFILE_EXCHANGE_URI`` set) a node whose backend lacks what it needs goes to the policy's
fallback, ``registry.fallback_backend_id()``, when that backend can run it: an Auto node and one whose own Compute
backend names the backend alike, a node type its builder pins never. Its edges then cross through the store like any
other boundary, the run logs a warning per moved node and its row says where it ran and why. A node neither backend can
run is refused with both reasons; a backend refused whole still refuses every node, and without a store nothing moves,
the refusal naming the store as what would move it. ``local`` in a process that serves runs no node, so its nodes fall
back too. Placement reads each node's settings through a resolved copy and never changes the node.
"""

from pathlib import Path

import pytest

from flowfile_core.flowfile.execution import exchange, placement, registry
from flowfile_core.flowfile.execution.errors import VersionMismatch
from flowfile_core.flowfile.execution.placement import (
    SERVING_LOCAL,
    Fallback,
    MixedPlacementPolicy,
    Refusal,
    SingleBackendPolicy,
    place,
    remedy_for,
)
from tests.flowfile.execution import test_placement
from tests.flowfile.execution.test_exchange_store import _BUCKET
from tests.flowfile.execution.test_placement import (
    BACKEND_ID,
    add_manual_input,
    everything,
    fuzzy_flow,
    make_graph,
    model_flow,
    parameter_read_flow,
    url_read_flow,
    without,
)

declare = test_placement.declare
csv_url = test_placement.csv_url
protocol_on = test_placement.protocol_on

TEAM, DEFAULT = "worker:team", "worker"
NO_FUZZY = "it needs the fuzzy_join operation"


@pytest.fixture
def store(monkeypatch):
    """An exchange store configured, so a run places each node by the mixed policy."""
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")


@pytest.fixture
def serving(monkeypatch):
    monkeypatch.setattr(registry, "_serving", True)


@pytest.fixture
def no_s3_client(monkeypatch) -> list:
    """The S3 clients the run's removal makes, recorded instead of made."""
    made: list = []
    monkeypatch.setattr(exchange, "build_s3_client", lambda *args, **kwargs: made.append(args))
    return made


def test_a_node_its_backend_cannot_run_moves_to_the_fallback_and_its_inputs_cross():
    graph = fuzzy_flow(8601)

    placed = place(
        graph,
        {TEAM: without(everything(), operation="fuzzy_join"), DEFAULT: everything()},
        MixedPlacementPolicy(TEAM, fallback_id=DEFAULT),
    )

    assert placed.assignment == {1: TEAM, 2: TEAM, 3: DEFAULT}
    assert placed.fallbacks == [Fallback(3, TEAM, DEFAULT, NO_FUZZY)]
    assert placed.boundaries == [(1, 3), (2, 3)]
    assert placed.refusals == []


def test_a_node_set_to_a_backend_of_its_own_moves_too():
    graph = fuzzy_flow(8602)
    graph.get_node(3).setting_input.execution_location = TEAM

    placed = place(
        graph,
        {TEAM: without(everything(), operation="fuzzy_join"), DEFAULT: everything()},
        MixedPlacementPolicy(DEFAULT, fallback_id=DEFAULT),
    )

    assert placed.assignment == {1: DEFAULT, 2: DEFAULT, 3: DEFAULT}
    assert placed.fallbacks == [Fallback(3, TEAM, DEFAULT, NO_FUZZY)]
    assert placed.boundaries == []


def test_a_node_neither_backend_can_run_is_refused_with_both_reasons():
    graph = fuzzy_flow(8603)
    short = without(everything(), operation="fuzzy_join")

    placed = place(graph, {TEAM: short, DEFAULT: short}, MixedPlacementPolicy(TEAM, fallback_id=DEFAULT))

    assert placed.fallbacks == []
    assert placed.refusals == [Refusal(3, TEAM, f'{NO_FUZZY}; on the default backend "{DEFAULT}": {NO_FUZZY}')]


@pytest.mark.parametrize(
    "policy",
    [MixedPlacementPolicy(TEAM, fallback_id=DEFAULT), MixedPlacementPolicy(TEAM), SingleBackendPolicy(TEAM)],
    ids=["fallback-not-in-hand", "no-fallback", "single-policy"],
)
def test_nothing_moves_without_a_fallback_in_hand(policy):
    graph = fuzzy_flow(8604)

    placed = place(graph, {TEAM: without(everything(), operation="fuzzy_join")}, policy)

    assert placed.fallbacks == []
    assert placed.refusals == [Refusal(3, TEAM, NO_FUZZY)]


def test_a_node_type_its_builder_pins_never_moves(monkeypatch):
    monkeypatch.setitem(placement._PINNED_BACKENDS, "fuzzy_match", TEAM)
    graph = fuzzy_flow(8605)

    placed = place(
        graph,
        {TEAM: without(everything(), operation="fuzzy_join"), DEFAULT: everything()},
        MixedPlacementPolicy(DEFAULT, fallback_id=DEFAULT),
    )

    assert placed.fallbacks == []
    assert placed.refusals == [Refusal(3, TEAM, NO_FUZZY)]


@pytest.mark.parametrize("reverse", [False, True], ids=["in-order", "reversed"])
def test_a_train_model_node_that_falls_back_takes_its_apply_model_node_along(reverse):
    graph = model_flow(8606)
    nodes = list(reversed(graph.nodes)) if reverse else list(graph.nodes)

    placed = place(
        graph,
        {TEAM: without(everything(), operation="ml_train"), DEFAULT: everything()},
        MixedPlacementPolicy(TEAM, fallback_id=DEFAULT),
        nodes=nodes,
    )

    assert placed.assignment == {1: TEAM, 2: DEFAULT, 3: DEFAULT}
    assert [fallback.node_id for fallback in placed.fallbacks] == ([3, 2] if reverse else [2, 3])
    apply_moved = next(fallback for fallback in placed.fallbacks if fallback.node_id == 3)
    assert apply_moved.reason == (
        f'it scores the model Train Model node 2 fits and holds on backend "{DEFAULT}"; place both on one backend'
    )
    assert placed.refusals == []


def test_local_in_a_process_that_serves_falls_back_with_a_store_and_is_refused_without(serving):
    graph = make_graph(8607)
    add_manual_input(graph, 1, [{"a": 1}])
    graph.get_node(1).setting_input.execution_location = registry.LOCAL_BACKEND_ID

    mixed = place(graph, {DEFAULT: everything()}, MixedPlacementPolicy(DEFAULT, fallback_id=DEFAULT))
    single = place(graph, {DEFAULT: everything()}, SingleBackendPolicy(registry.LOCAL_BACKEND_ID))

    assert mixed.fallbacks == [Fallback(1, registry.LOCAL_BACKEND_ID, DEFAULT, SERVING_LOCAL)]
    assert mixed.assignment == {1: DEFAULT}
    assert single.refusals == [Refusal(1, registry.LOCAL_BACKEND_ID, SERVING_LOCAL)]


def test_placement_reads_a_parameter_without_changing_the_node(tmp_path: Path):
    (tmp_path / "rows.csv").write_text("n\n1\n")
    graph = parameter_read_flow(8608, "${dir}/rows.csv", str(tmp_path))
    node = graph.get_node(1)
    before = (node.setting_input.model_dump(), node._hash)

    placed = place(
        graph,
        {TEAM: without(everything(), scheme="file"), DEFAULT: everything()},
        MixedPlacementPolicy(TEAM, fallback_id=DEFAULT),
    )

    assert placed.fallbacks == [Fallback(1, TEAM, DEFAULT, "it reads local files")]
    assert (node.setting_input.model_dump(), node._hash) == before
    assert node.setting_input.received_file.path == "${dir}/rows.csv"


def test_the_single_policy_names_the_store_as_what_would_move_the_node():
    graph = fuzzy_flow(8609)
    node = graph.get_node(3)

    remedy = remedy_for(node, SingleBackendPolicy(TEAM), fallback_id=DEFAULT)

    assert remedy == (
        "To run it elsewhere, set the flow's Execution location under Flow Settings, or configure an exchange store "
        f'(Settings → Execution → Backends), which moves a node its backend cannot run to the default backend '
        f'"{DEFAULT}" on its own.'
    )
    assert remedy_for(node, SingleBackendPolicy(TEAM)) == test_placement.FLOW_REMEDY


def test_the_fallback_is_the_default_backend_when_serving_and_the_embedded_worker_otherwise(monkeypatch):
    previous = registry.set_default_override("worker:headless")
    try:
        monkeypatch.setattr(registry, "_serving", False)
        assert registry.fallback_backend_id() == registry.LOCAL_BACKEND_ID
        monkeypatch.setattr(registry, "_serving", True)
        assert registry.fallback_backend_id() == "worker:headless"
    finally:
        registry.set_default_override(previous)


def test_a_run_reads_over_http_on_the_fallback_when_its_backend_does_not(
    protocol_on, declare, store, no_s3_client, csv_url
):
    backend = declare(without(everything(), scheme="http"))
    graph = url_read_flow(8610, csv_url)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    (row,) = run_info.node_step_result
    assert (row.backend_id, row.fallback_from) == (registry.LOCAL_BACKEND_ID, BACKEND_ID)
    assert row.fallback_reason == "it reads http URLs"
    assert backend.work == []
    assert no_s3_client == []
    flow_log = Path(graph.flow_logger.log_file_path).read_text()
    assert (
        'backend "declaring" cannot run it: it reads http URLs; '
        'it runs on the default backend "local" instead'
    ) in flow_log, flow_log
    assert "1 node(s) run on the default backend local instead of the backend their settings name: 1" in flow_log


def test_a_backend_refused_whole_still_refuses_every_node_with_a_store(protocol_on, declare, store, no_s3_client):
    declare(VersionMismatch("polars_version: core 1, worker 2"))
    graph = fuzzy_flow(8611)

    run_info = graph.run_graph()

    assert run_info.success is False
    assert {result.node_id for result in run_info.node_step_result} == {1, 2, 3}
    for result in run_info.node_step_result:
        assert result.error.startswith('cannot run on backend "declaring": polars_version: core 1, worker 2.')
        assert result.compute_error == "Refusal"
