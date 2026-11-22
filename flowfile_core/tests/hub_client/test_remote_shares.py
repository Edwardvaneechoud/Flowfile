"""Sharing a row of the hub's catalog from this machine: core's own ``/shares`` routes, answered by the hub.

A request naming a hub row (a negative ``resource_id``, or a negative grant id in the path) goes to the hub's mount of
the same route as the signed-in person, the ids as the hub's own; the groups offered are the hub's. This machine's own
sharing is off (electron), and a request that names no hub row is still this machine's to refuse.
"""

import json

import httpx
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.hub_client import client
from tests.hub_client.conftest import HUB_URL, FakeHubWithCatalog

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}
NOT_YOURS = "Resource not found"


class SharingHub(FakeHubWithCatalog):
    """The catalog fake plus core's ``/shares`` routes as the hub mounts them: ``groups`` are the teams and Everyone
    it shares with, ``shares`` its grants, and a resource in ``unmanageable`` is core's 404."""

    def __init__(self):
        super().__init__()
        self.groups = [{"id": 1, "name": "Everyone", "created_by": 1}, {"id": 3, "name": "Finance", "created_by": 1}]
        self.shares: list[dict] = []
        self.unmanageable: set[int] = set()
        self.share_calls: list[tuple[str, str, str, object]] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if not path.startswith("/shares"):
            return super().handle(request)
        body = json.loads(request.content) if request.content else None
        self.share_calls.append((request.method, path, request.url.query.decode(), body))
        if self._bearer_user(request) is None:
            return httpx.Response(401, json={"detail": "Could not validate credentials"})
        named = body if request.method == "POST" else dict(request.url.params)
        if "resource_id" in named and int(named["resource_id"]) in self.unmanageable:
            return httpx.Response(404, json={"detail": NOT_YOURS})
        if path == "/shares/groups":
            return httpx.Response(200, json=self.groups)
        if path == "/shares" and request.method == "GET":
            wanted = (named["resource_type"], int(named["resource_id"]))
            return httpx.Response(
                200, json=[s for s in self.shares if (s["resource_type"], s["resource_id"]) == wanted]
            )
        if path == "/shares":
            group = next((g for g in self.groups if g["id"] == body["group_id"]), None)
            if group is None:
                return httpx.Response(404, json={"detail": "Group not found"})
            share = {**body, "id": len(self.shares) + 7, "group_name": group["name"], "granted_by": 1}
            self.shares.append(share)
            return httpx.Response(200, json=share)
        grant_id = int(path.rsplit("/", 1)[1])
        self.shares = [s for s in self.shares if s["id"] != grant_id]
        return httpx.Response(204)


@pytest.fixture
def hub(monkeypatch) -> SharingHub:
    fake = SharingHub()
    monkeypatch.setattr(client, "_module_http", fake.http())
    return fake


@pytest.fixture
def connected(store, hub) -> TestClient:
    c = TestClient(main.app)
    c.headers["Authorization"] = f"Bearer {c.post('/auth/token').json()['access_token']}"
    assert c.post("/hub_client/connection", json=CONNECT).status_code == 200
    return c


def test_a_hub_table_is_shared_with_the_hubs_groups_and_the_grant_removed_on_the_hub(connected, hub):
    named = {"resource_type": "catalog_table", "resource_id": -101}
    groups = connected.get("/shares/groups", params=named)
    assert groups.status_code == 200 and [(g["id"], g["name"]) for g in groups.json()] == [(-1, "Everyone"), (-3, "Finance")]
    assert hub.share_calls[-1] == ("GET", "/shares/groups", "resource_type=catalog_table&resource_id=101", None)

    made = connected.post("/shares", json={**named, "group_id": -3, "permission": "use"})
    assert made.status_code == 200
    assert hub.share_calls[-1][3] == {"resource_type": "catalog_table", "resource_id": 101, "group_id": 3, "permission": "use"}
    grant = made.json()
    assert (grant["id"], grant["resource_id"], grant["group_id"], grant["group_name"]) == (-7, -101, -3, "Finance")

    listed = connected.get("/shares", params=named)
    assert listed.status_code == 200 and [s["id"] for s in listed.json()] == [-7]

    removed = connected.delete(f"/shares/{grant['id']}")
    assert removed.status_code == 204 and removed.content == b""
    assert hub.share_calls[-1][:2] == ("DELETE", "/shares/7") and hub.shares == []


@pytest.mark.parametrize("resource_type", ["flow", "catalog_namespace"])
def test_a_hub_flow_and_a_hub_schema_are_shared_the_same_way(connected, hub, resource_type):
    made = connected.post("/shares", json={"resource_type": resource_type, "resource_id": -5, "group_id": -1})
    assert made.status_code == 200 and hub.shares[0]["resource_type"] == resource_type and hub.shares[0]["resource_id"] == 5


def test_the_hubs_refusal_is_answered_as_the_hub_gave_it(connected, hub):
    hub.unmanageable = {101}
    refused = connected.get("/shares", params={"resource_type": "catalog_table", "resource_id": -101})
    assert refused.status_code == 404 and refused.json() == {"detail": NOT_YOURS}


def test_a_request_that_names_no_hub_row_stays_this_machines_own(connected, hub):
    local = connected.get("/shares", params={"resource_type": "catalog_table", "resource_id": 101})
    assert local.status_code == 404 and local.json() == {"detail": "Not found"}
    assert hub.share_calls == []
