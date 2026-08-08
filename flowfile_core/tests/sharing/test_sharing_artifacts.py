"""Artifact sharing: read filtering, owner-or-manage gates, and grant continuity across versions."""

import hashlib
from pathlib import Path

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context


@pytest.fixture
def team(users, group_factory):
    """alice (owner) shares with bob (member)."""
    return group_factory("artifact-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


@pytest.fixture
def alice_flow(users, resource_factory):
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="AliceModels", parent_id=None, level=0, owner_id=users["alice"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="alice_models", parent_id=catalog_id, level=1, owner_id=users["alice"].id
    )
    return resource_factory(
        db_models.FlowRegistration,
        name="alice_model_flow",
        flow_path="/tmp/alice_model_flow.flowfile",
        namespace_id=schema_id,
        owner_id=users["alice"].id,
    )


@pytest.fixture
def bob_flow(users, resource_factory):
    """A registration bob owns, in his own namespace."""
    catalog_id = resource_factory(
        db_models.CatalogNamespace, name="BobModels", parent_id=None, level=0, owner_id=users["bob"].id
    )
    schema_id = resource_factory(
        db_models.CatalogNamespace, name="bob_models", parent_id=catalog_id, level=1, owner_id=users["bob"].id
    )
    return resource_factory(
        db_models.FlowRegistration,
        name="bob_model_flow",
        flow_path="/tmp/bob_model_flow.flowfile",
        namespace_id=schema_id,
        owner_id=users["bob"].id,
    )


@pytest.fixture
def publish_as(client_for, resource_factory):
    """Publish an artifact as any user; every published name is removed on teardown.

    Depends on ``resource_factory`` for teardown order: artifacts must go before the
    registrations they point at.
    """
    clients: dict[str, object] = {}
    names: set[str] = set()

    def _publish(user: str, name: str, registration_id: int, payload: bytes = b"model-bytes") -> int:
        names.add(name)
        client = clients.setdefault(user, client_for(user))
        prep = client.post(
            "/artifacts/prepare-upload",
            json={
                "name": name,
                "source_registration_id": registration_id,
                "serialization_format": "pickle",
                "python_type": "builtins.dict",
            },
        )
        assert prep.status_code == 201, prep.text
        data = prep.json()
        path = Path(data["path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        fin = client.post(
            "/artifacts/finalize",
            json={
                "artifact_id": data["artifact_id"],
                "storage_key": data["storage_key"],
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
            },
        )
        assert fin.status_code == 200, fin.text
        return data["artifact_id"]

    yield _publish
    with get_db_context() as db:
        for name in names:
            for row in db.query(db_models.GlobalArtifact).filter_by(name=name).all():
                db.query(db_models.ResourceGrant).filter_by(
                    resource_type="global_artifact", resource_id=row.id
                ).delete()
                db.delete(row)
        db.commit()


@pytest.fixture
def published(alice_flow, publish_as):
    """Publish versions of an artifact as alice."""

    def _publish(name: str, payload: bytes = b"model-bytes") -> int:
        return publish_as("alice", name, alice_flow, payload=payload)

    return _publish


def _grants_for(artifact_id: int) -> list[db_models.ResourceGrant]:
    with get_db_context() as db:
        return db.query(db_models.ResourceGrant).filter_by(
            resource_type="global_artifact", resource_id=artifact_id
        ).all()


def test_artifact_reads_are_private_by_default(users, client_for, published):
    artifact_id = published("private_model")
    bob = client_for("bob")

    assert "private_model" not in [a["name"] for a in bob.get("/artifacts/").json()]
    assert bob.get(f"/artifacts/{artifact_id}").status_code == 404
    assert bob.get("/artifacts/by-name/private_model").status_code == 404
    assert client_for("alice").get(f"/artifacts/{artifact_id}").status_code == 200


def test_grant_survives_a_new_version(users, client_for, published, team, grant_factory):
    """A direct artifact share must not be silently revoked by the next publish."""
    v1 = published("continuity_model")
    grant_factory("global_artifact", v1, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    assert bob.get(f"/artifacts/{v1}").status_code == 200

    v2 = published("continuity_model", payload=b"model-bytes-v2")

    assert [g.permission for g in _grants_for(v2)] == ["use"]
    assert bob.get(f"/artifacts/{v2}").status_code == 200
    assert bob.get("/artifacts/by-name/continuity_model").json()["id"] == v2


def test_version_delete_revokes_only_that_versions_grants(users, client_for, published, team, grant_factory):
    v1 = published("revoke_model")
    grant_factory("global_artifact", v1, team, permission="manage", granted_by=users["alice"].id)
    v2 = published("revoke_model", payload=b"v2")
    v3 = published("revoke_model", payload=b"v3")

    alice = client_for("alice")
    assert alice.delete(f"/artifacts/{v1}").status_code == 200

    assert _grants_for(v1) == []
    assert len(_grants_for(v2)) == 1
    assert len(_grants_for(v3)) == 1


def test_use_grantee_cannot_mutate_versions(users, client_for, published, team, grant_factory):
    v1 = published("use_only_model")
    v2 = published("use_only_model", payload=b"v2")
    for artifact_id in (v1, v2):
        grant_factory("global_artifact", artifact_id, team, permission="use", granted_by=users["alice"].id)

    bob = client_for("bob")
    assert bob.delete(f"/artifacts/{v1}").status_code == 403
    assert bob.post(f"/artifacts/{v1}/promote").status_code == 403
    assert bob.post("/artifacts/by-name/use_only_model/prune", params={"keep": 1}).status_code == 403
    assert bob.delete("/artifacts/by-name/use_only_model").status_code == 403


def test_manage_grantee_can_promote_and_prune_without_forking_ownership(
    users, client_for, published, team, grant_factory
):
    v1 = published("manage_model")
    v2 = published("manage_model", payload=b"v2")
    v3 = published("manage_model", payload=b"v3")
    for artifact_id in (v1, v2, v3):
        grant_factory("global_artifact", artifact_id, team, permission="manage", granted_by=users["alice"].id)

    bob = client_for("bob")
    promoted = bob.post(f"/artifacts/{v1}/promote")
    assert promoted.status_code == 200, promoted.text
    assert promoted.json()["owner_id"] == users["alice"].id
    assert promoted.json()["version"] == 4

    pruned = bob.post("/artifacts/by-name/manage_model/prune", params={"keep": 2, "dry_run": False})
    assert pruned.status_code == 200, pruned.text
    assert pruned.json()["deleted"] == 2


def test_use_grantee_cannot_delete_another_owners_same_named_artifact(
    users, client_for, publish_as, alice_flow, bob_flow, team, grant_factory
):
    """Owning a same-named artifact must not authorize deleting the one merely shared."""
    alice_row = publish_as("alice", "churn", alice_flow)
    grant_factory("global_artifact", alice_row, team, permission="use", granted_by=users["alice"].id)
    bob_row = publish_as("bob", "churn", bob_flow)

    bob = client_for("bob")
    assert bob.delete("/artifacts/by-name/churn").status_code == 403

    with get_db_context() as db:
        assert db.get(db_models.GlobalArtifact, alice_row).status == "active"
        assert db.get(db_models.GlobalArtifact, bob_row).status == "active"


def test_managing_only_the_latest_version_does_not_authorize_pruning_older_rows(
    users, client_for, published, team, grant_factory
):
    """Prune must require manage on every row it deletes, not just the newest one."""
    v1 = published("prunable_model")
    v2 = published("prunable_model", payload=b"v2")
    grant_factory("global_artifact", v1, team, permission="use", granted_by=users["alice"].id)
    grant_factory("global_artifact", v2, team, permission="manage", granted_by=users["alice"].id)

    bob = client_for("bob")
    resp = bob.post(
        "/artifacts/by-name/prunable_model/prune", params={"keep": 1, "dry_run": False}
    )
    assert resp.status_code == 403

    with get_db_context() as db:
        assert db.get(db_models.GlobalArtifact, v1).status == "active"
        assert db.get(db_models.GlobalArtifact, v2).status == "active"


def test_stranger_cannot_see_or_prune(users, client_for, published, alice_flow):
    v1 = published("hidden_model")
    v2 = published("hidden_model", payload=b"v2")
    carol = client_for("carol")
    with get_db_context() as db:
        namespace_id = db.get(db_models.FlowRegistration, alice_flow).namespace_id

    # Invisible rather than forbidden: an unshared artifact must not be probeable —
    # not by bare name, not with an explicit namespace, not by row id.
    assert carol.post("/artifacts/by-name/hidden_model/prune", params={"keep": 1}).status_code == 404
    assert (
        carol.post(
            "/artifacts/by-name/hidden_model/prune", params={"keep": 1, "namespace_id": namespace_id}
        ).status_code
        == 404
    )
    assert (
        carol.post(
            "/artifacts/by-name/AliceModels.alice_models::hidden_model/prune", params={"keep": 1}
        ).status_code
        == 404
    )
    assert carol.post(f"/artifacts/{v1}/promote").status_code == 404
    assert carol.delete(f"/artifacts/{v2}").status_code == 404
    assert carol.delete("/artifacts/by-name/hidden_model").status_code == 404
