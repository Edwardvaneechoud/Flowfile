"""Run history honors the private-by-default catalog boundary.

On a shared server a restricted user sees only the runs they started, runs of a
flow they own or were granted, and runs of a flow registered in a namespace they
can see. That holds for the run list (in the query, so totals and pages agree),
run detail and log (an invisible run is a 404, never a 403), active runs, and the
counts on the stats and overview screens. Admins see every run.
"""

import uuid
from datetime import datetime, timezone

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from shared.run_logs import run_log_path


@pytest.fixture
def team(users, group_factory):
    """alice (owner) shares with bob (member); carol is outside the group."""
    return group_factory("run-history-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def tag():
    """A flow-name prefix unique to the test, so ``search`` scopes lists to its own runs."""
    return f"runhist_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def alice_flows(users, resource_factory, tag):
    """alice's private flow in her own namespace, and her flow in a public namespace."""
    alice = users["alice"].id
    private_ns = resource_factory(db_models.CatalogNamespace, name=f"{tag}_private", level=0, owner_id=alice)
    public_ns = resource_factory(
        db_models.CatalogNamespace, name=f"{tag}_public", level=0, owner_id=alice, is_public=True
    )
    private_flow = resource_factory(
        db_models.FlowRegistration,
        name=f"{tag}_private_flow",
        flow_path=f"/tmp/{tag}_private.flowfile",
        namespace_id=private_ns,
        owner_id=alice,
    )
    public_flow = resource_factory(
        db_models.FlowRegistration,
        name=f"{tag}_public_flow",
        flow_path=f"/tmp/{tag}_public.flowfile",
        namespace_id=public_ns,
        owner_id=alice,
    )
    return {"private_ns": private_ns, "private": private_flow, "public": public_flow}


@pytest.fixture
def make_run(resource_factory, tag):
    def _make(user_id: int, registration_id: int | None, *, ended: bool = True, run_type: str = "manual") -> int:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        flow_uuid = None
        if registration_id is not None:
            with get_db_context() as db:
                flow_uuid = db.get(db_models.FlowRegistration, registration_id).flow_uuid
        return resource_factory(
            db_models.FlowRun,
            registration_id=registration_id,
            flow_uuid=flow_uuid,
            flow_name=f"{tag}_run",
            user_id=user_id,
            started_at=now,
            ended_at=now if ended else None,
            success=True if ended else None,
            run_type=run_type,
        )

    return _make


@pytest.fixture
def run_log():
    """Write a run's subprocess log where the log route reads it; removed on teardown."""
    written = []

    def _write(run_id: int) -> None:
        path = run_log_path(run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("run output")
        written.append(path)

    yield _write
    for path in written:
        path.unlink(missing_ok=True)


def _run_ids(client, tag, **params) -> list[int]:
    response = client.get("/catalog/runs", params={"search": tag, **params})
    assert response.status_code == 200, response.text
    return [r["id"] for r in response.json()["items"]]


def test_run_history_owner_sees_run_colleague_without_grant_does_not(users, client_for, alice_flows, make_run, tag):
    run_id = make_run(users["alice"].id, alice_flows["private"])

    alice = client_for("alice")
    assert run_id in _run_ids(alice, tag)
    assert alice.get(f"/catalog/runs/{run_id}").status_code == 200

    carol = client_for("carol")
    assert run_id not in _run_ids(carol, tag)
    response = carol.get(f"/catalog/runs/{run_id}")
    assert response.status_code == 404, response.text
    assert response.json()["detail"] == "Run not found"


def test_run_history_invisible_run_is_indistinguishable_from_missing(users, client_for, alice_flows, make_run):
    run_id = make_run(users["alice"].id, alice_flows["private"])
    carol = client_for("carol")

    hidden = carol.get(f"/catalog/runs/{run_id}")
    missing = carol.get("/catalog/runs/999999999")
    assert (hidden.status_code, hidden.json()) == (missing.status_code, missing.json())


def test_run_history_log_follows_run_visibility(users, client_for, alice_flows, make_run, run_log, team, grant_factory):
    run_id = make_run(users["alice"].id, alice_flows["private"])
    run_log(run_id)

    assert client_for("alice").get(f"/catalog/runs/{run_id}/log").json() == {"log": "run output"}

    response = client_for("carol").get(f"/catalog/runs/{run_id}/log")
    assert response.status_code == 404
    assert response.json()["detail"] == "Run not found"

    grant_factory("flow", alice_flows["private"], team, granted_by=users["alice"].id)
    assert client_for("bob").get(f"/catalog/runs/{run_id}/log").status_code == 200


def test_run_history_use_grant_on_flow_makes_its_runs_visible(
    users, client_for, alice_flows, make_run, tag, team, grant_factory
):
    run_id = make_run(users["alice"].id, alice_flows["private"])
    bob = client_for("bob")
    assert run_id not in _run_ids(bob, tag)
    assert bob.get(f"/catalog/runs/{run_id}").status_code == 404

    grant_factory("flow", alice_flows["private"], team, permission="use", granted_by=users["alice"].id)

    assert run_id in _run_ids(bob, tag)
    assert bob.get(f"/catalog/runs/{run_id}").status_code == 200
    # carol is outside the group the flow was shared with.
    assert run_id not in _run_ids(client_for("carol"), tag)


def test_run_history_visible_namespace_makes_its_flows_runs_visible(users, client_for, alice_flows, make_run, tag):
    """A flow in a public namespace is not usable by bob, but its runs are visible to him."""
    run_id = make_run(users["alice"].id, alice_flows["public"])
    bob = client_for("bob")

    assert run_id in _run_ids(bob, tag)
    assert bob.get(f"/catalog/runs/{run_id}").status_code == 200


def test_run_history_namespace_grant_makes_its_flows_runs_visible(
    users, client_for, alice_flows, make_run, tag, team, grant_factory
):
    run_id = make_run(users["alice"].id, alice_flows["private"])
    grant_factory("catalog_namespace", alice_flows["private_ns"], team, granted_by=users["alice"].id)

    bob = client_for("bob")
    assert run_id in _run_ids(bob, tag)
    assert bob.get(f"/catalog/runs/{run_id}").status_code == 200


def test_run_history_own_runs_of_an_inaccessible_flow_stay_visible(users, client_for, alice_flows, make_run, tag):
    run_id = make_run(users["bob"].id, alice_flows["private"])
    bob = client_for("bob")

    assert run_id in _run_ids(bob, tag)
    assert bob.get(f"/catalog/runs/{run_id}").status_code == 200


def test_run_history_admin_sees_all(users, client_for, alice_flows, make_run, tag):
    private_run = make_run(users["alice"].id, alice_flows["private"])
    adhoc_run = make_run(users["bob"].id, None)
    admin = client_for("admin")

    ids = _run_ids(admin, tag)
    assert private_run in ids
    assert adhoc_run in ids
    assert admin.get(f"/catalog/runs/{private_run}").status_code == 200
    assert admin.get(f"/catalog/runs/{adhoc_run}").status_code == 200


def test_runs_filter_paginates_over_the_visible_set(users, client_for, alice_flows, make_run, tag):
    """Totals and page sizes are computed over the visible runs, not over a filtered page."""
    bob_run = make_run(users["bob"].id, None)
    for _ in range(3):
        make_run(users["alice"].id, alice_flows["private"])

    body = client_for("bob").get("/catalog/runs", params={"search": tag, "limit": 1}).json()
    assert [r["id"] for r in body["items"]] == [bob_run]
    assert (body["total"], body["total_success"]) == (1, 1)

    body = client_for("alice").get("/catalog/runs", params={"search": tag, "limit": 2}).json()
    assert len(body["items"]) == 2
    assert body["total"] == 3


def test_runs_filter_applies_to_a_by_flow_listing(users, client_for, alice_flows, make_run, tag):
    run_id = make_run(users["alice"].id, alice_flows["private"])

    assert run_id not in _run_ids(client_for("carol"), tag, registration_id=alice_flows["private"])
    assert run_id in _run_ids(client_for("alice"), tag, registration_id=alice_flows["private"])


def test_run_history_active_runs_are_filtered(users, client_for, alice_flows, make_run):
    run_id = make_run(users["alice"].id, alice_flows["private"], ended=False, run_type="in_designer_run")

    def active_ids(key):
        response = client_for(key).get("/catalog/active-runs")
        assert response.status_code == 200, response.text
        return [r["id"] for r in response.json()]

    assert run_id in active_ids("alice")
    assert run_id in active_ids("admin")
    assert run_id not in active_ids("carol")


def test_stats_count_over_the_visible_runs(users, client_for, alice_flows, make_run):
    private_run = make_run(users["alice"].id, alice_flows["private"])
    active_run = make_run(users["alice"].id, alice_flows["private"], ended=False, run_type="in_designer_run")
    carol = client_for("carol")

    stats = carol.get("/catalog/stats").json()
    assert stats["total_runs"] == carol.get("/catalog/runs").json()["total"]
    assert private_run not in [r["id"] for r in stats["recent_runs"]]
    assert active_run not in [r["id"] for r in stats["active_runs"]]

    admin_stats = client_for("admin").get("/catalog/stats").json()
    assert admin_stats["total_runs"] >= stats["total_runs"] + 2
    assert private_run in [r["id"] for r in admin_stats["recent_runs"]]


def test_overview_counts_over_the_visible_runs(users, client_for, alice_flows, make_run):
    private_run = make_run(users["alice"].id, alice_flows["private"])
    active_run = make_run(users["alice"].id, alice_flows["private"], ended=False, run_type="in_designer_run")
    carol = client_for("carol")

    overview = carol.get("/catalog/overview").json()
    assert private_run not in [r["id"] for r in overview["runs"]["items"]]
    assert active_run not in [r["id"] for r in overview["active_runs"]]
    assert active_run not in [r["id"] for r in overview["stats"]["active_runs"]]
    assert overview["runs"]["total"] == overview["stats"]["total_runs"]

    alice_overview = client_for("alice").get("/catalog/overview").json()
    assert private_run in [r["id"] for r in alice_overview["runs"]["items"]]
    assert active_run in [r["id"] for r in alice_overview["active_runs"]]
