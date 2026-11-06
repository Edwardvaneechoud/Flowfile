"""On a server joined to its hub with keys of its own, the editor's calls to one of the hub's servers (a step's schema,
a node fetch, a column's statistics) carry the token minted with the signed-in person's own sign-in through the hub.

The setting is T75's two-person end to end (``test_server_run_tokens_e2e``): core in process as the joined server, a
hub that mints each person's ``run`` token and records whom for, and the hub's server ``team`` as a second worker that
takes only those tokens. The hub-filled row ``worker:team`` belongs to ``ada``; ``dee``'s flow runs on it. A pivot's
columns are its pivot column's values, so predicting them reads the data on the server. ``dee``'s schema comes through
``dee``'s token, never ``ada``'s; once ``dee`` signs out, the editor shows the sentence that says to sign in through the
hub again instead of a step with no columns or a 500, and signing in again brings the columns back.
"""

import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.routes import routes
from flowfile_core.schemas import input_schema, transform_schema
from tests.conftest import _set_protocol, protocol_states
from tests.flowfile.execution.graph_helpers import add_manual_input, add_promise, connect
from tests.hub import test_server_run_tokens_e2e as e2e
from tests.hub.test_server_run_tokens_e2e import BACKEND, DEE, REFUSED, client, errors, run, sign_in
from tests.test_hub_sign_in import bearer

protocol = protocol_states()
node_file = e2e.node_file
signer, server, hub, people, subjects = e2e.signer, e2e.server, e2e.hub, e2e.people, e2e.subjects
PIVOTED = ["name", "x", "y"]


@pytest.fixture
def pivot_flow(tmp_path):
    """Opens a flow in a person's session on the hub's server ``team``: manual input → pivot on ``cat``."""
    opened = []

    def open_one(user_id: int, name: str):
        flow_id = routes.flow_file_handler.add_flow(
            name=name, flow_path=str(tmp_path / f"{name}.yaml"), user_id=user_id
        )
        opened.append(flow_id)
        flow = routes.flow_file_handler.get_flow(flow_id)
        flow.flow_settings.execution_location = BACKEND
        flow.flow_settings.execution_mode = "Development"
        add_manual_input(flow, 1, [{"name": "a", "cat": "x", "v": 1}, {"name": "b", "cat": "y", "v": 2}])
        add_promise(flow, 2, "pivot")
        connect(flow, 1, 2)
        flow.add_pivot(
            input_schema.NodePivot(
                flow_id=flow.flow_id,
                node_id=2,
                depending_on_id=1,
                pivot_input=transform_schema.PivotInput(
                    index_columns=["name"], pivot_column="cat", value_col="v", aggregations=["sum"]
                ),
            )
        )
        return flow

    yield open_one
    for flow_id in opened:
        routes.flow_file_handler.delete_flow(flow_id)


@pytest.fixture
def hub_server_default():
    """The hub's server ``team`` as this server's default backend too, where a pivot's prediction reads its values
    (this server's own worker is not started here)."""
    previous = registry.set_default_override(BACKEND)
    yield
    registry.set_default_override(previous)


def node_view(flow, access_token: str) -> dict:
    viewed = client.get("/node", params={"flow_id": flow.flow_id, "node_id": 2}, headers=bearer(access_token))
    assert viewed.status_code == 200, viewed.text
    return viewed.json()


def columns(view: dict) -> list[str]:
    return [column["name"] for column in view["main_output"]["table_schema"]]


def test_a_steps_schema_on_the_hubs_server_is_read_with_the_persons_own_token(
    protocol, hub, people, subjects, pivot_flow, hub_server_default
):
    _, dee_token = people["dee"]
    dee_name = hub.people[DEE]["username"]

    view = node_view(pivot_flow(people["dee"][0], "dees_flow"), dee_token)

    assert columns(view) == PIVOTED
    assert view["prediction_warning"] is None
    assert subjects and set(subjects) == {dee_name}
    assert hub.minted == [dee_name]


def test_a_person_signed_out_of_the_hub_gets_the_sentence_and_the_columns_after_signing_in_again(
    protocol, hub, people, subjects, pivot_flow, hub_server_default
):
    dee, dee_token = people["dee"]
    dee_name = hub.people[DEE]["username"]
    flow = pivot_flow(dee, "dees_flow")
    assert client.post("/auth/logout", headers=bearer(dee_token)).status_code == 204

    refused = node_view(flow, dee_token)
    sent_while_signed_out, minted_while_signed_out = list(subjects), list(hub.minted)
    again, token_again = sign_in(hub, DEE, dee_name)
    view = node_view(flow, token_again)

    assert columns(refused) == []
    assert refused["prediction_warning"] == REFUSED
    assert sent_while_signed_out == [] and minted_while_signed_out == []
    assert again == dee
    assert columns(view) == PIVOTED
    assert view["prediction_warning"] is None
    assert set(subjects) == {dee_name} and hub.minted == [dee_name]


def test_a_fetch_for_a_person_signed_out_of_the_hub_is_refused_with_the_sentence(
    protocol, hub, people, subjects, pivot_flow
):
    dee, dee_token = people["dee"]
    flow = pivot_flow(dee, "dees_flow")
    assert client.post("/auth/logout", headers=bearer(dee_token)).status_code == 204

    fetched = client.post(
        "/node/trigger_fetch_data", params={"flow_id": flow.flow_id, "node_id": 2}, headers=bearer(dee_token)
    )

    assert fetched.status_code == 200, fetched.text
    assert errors(flow.get_run_info()) == [(1, REFUSED), (2, REFUSED)]
    assert subjects == [] and hub.minted == []


def test_a_columns_statistics_are_read_with_the_persons_token_and_refused_with_the_sentence_once_signed_out(
    hub, people, subjects, pivot_flow, monkeypatch
):
    _set_protocol(monkeypatch, True)
    dee, dee_token = people["dee"]
    dee_name = hub.people[DEE]["username"]
    flow = pivot_flow(dee, "dees_flow")
    assert run(flow, dee_token).success is True
    subjects.clear()
    params = {"flow_id": flow.flow_id, "node_id": 2, "column_name": "x"}

    stats = client.get("/node/column_stats", params=params, headers=bearer(dee_token))
    read_with = list(subjects)
    assert client.post("/auth/logout", headers=bearer(dee_token)).status_code == 204
    subjects.clear()
    refused = client.get("/node/column_stats", params=params, headers=bearer(dee_token))

    assert stats.status_code == 200, stats.text
    assert read_with and set(read_with) == {dee_name}
    assert refused.status_code == 403
    assert refused.json()["detail"] == REFUSED
    assert subjects == []
