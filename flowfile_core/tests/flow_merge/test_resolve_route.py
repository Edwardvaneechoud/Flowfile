"""Settling the conflicts a flow holds through ``POST /editor/merge/resolve/``."""

import threading
import time

from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.routes import flow_merge
from flowfile_core.schemas import input_schema, schemas


def _client() -> TestClient:
    c = TestClient(main.app)
    c.headers["Authorization"] = f"Bearer {c.post('/auth/token').json()['access_token']}"
    return c


def _holding(user_id: int, conflicts: list[dict]):
    """An open flow that holds ``conflicts`` with the person's own side on the canvas."""
    flow_id = create_unique_id()
    flow_file_handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="held", path="", execution_location="local"), user_id=user_id
    )
    flow = flow_file_handler.get_flow(flow_id)
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    flow.merge_conflicts = {"sides": {"ours": "main", "theirs": "origin/main"}, "shown": "ours", "conflicts": conflicts}
    return flow


def _conflict(step: int, **extra) -> dict:
    return {"key": f"step:{step}", "kind": "both_changed", "message": "both changed", **extra}


def test_two_settles_at_once_never_put_a_settled_conflict_back(monkeypatch):
    first, second = _client(), _client()
    user_id = first.get("/auth/users/me").json()["id"]
    flow = _holding(user_id, [_conflict(2, label="step 2 sample"), _conflict(3, label="step 3 filter")])
    real_edit_flow = flow_merge.edit_flow
    writing, go_on = threading.Event(), threading.Event()

    def paused_before_the_write(*args, **kwargs):
        writing.set()
        assert go_on.wait(10)
        return real_edit_flow(*args, **kwargs)

    monkeypatch.setattr(flow_merge, "edit_flow", paused_before_the_write)
    answers = {}

    def settle(api: TestClient, key: str) -> None:
        answers[key] = api.post("/editor/merge/resolve/", json={"flow_id": flow.flow_id, "key": key, "take": "ours"})

    threads = [
        threading.Thread(target=settle, args=(first, "step:2")),
        threading.Thread(target=settle, args=(second, "step:3")),
    ]
    try:
        threads[0].start()
        assert writing.wait(10)
        threads[1].start()
        time.sleep(0.5)  # the second request reads the held set now, unless the first one's lock keeps it out
        go_on.set()
        for thread in threads:
            thread.join(20)
        assert {key: answer.status_code for key, answer in answers.items()} == {"step:2": 200, "step:3": 200}
        assert flow.merge_conflicts is None
    finally:
        go_on.set()
        flow_file_handler.delete_flow(flow.flow_id, user_id)


def test_a_held_conflict_without_a_label_is_named_by_its_key(monkeypatch):
    api = _client()
    user_id = api.get("/auth/users/me").json()["id"]
    flow = _holding(user_id, [_conflict(2)])
    real_edit_flow = flow_merge.edit_flow
    described = []

    def noting(flow_, description, *args, **kwargs):
        described.append(description)
        return real_edit_flow(flow_, description, *args, **kwargs)

    monkeypatch.setattr(flow_merge, "edit_flow", noting)
    try:
        settled = api.post("/editor/merge/resolve/", json={"flow_id": flow.flow_id, "key": "step:2", "take": "ours"})
        assert settled.status_code == 200, settled.text
        assert described == ["Settle step:2: take main"]
    finally:
        flow_file_handler.delete_flow(flow.flow_id, user_id)
