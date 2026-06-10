"""Connection sharing (db / cloud / GA / Kafka): resolution, manage rules, credential re-entry."""

import json

import pytest

from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_cloud_connection_schema,
    get_database_connection_schema,
)
from flowfile_core.flowfile.database_connection_manager.ga_connections import (
    get_encrypted_credential,
    get_ga_connection,
)
from flowfile_core.kafka.connection_manager import get_kafka_connection
from flowfile_core.secret_manager.secret_manager import decrypt_secret


@pytest.fixture
def team(users, group_factory):
    """alice (owner) shares with bob (member); carol is not in the group."""
    return group_factory("conn-team", users["admin"].id, {users["alice"].id: "owner", users["bob"].id: "member"})


def _share(client, resource_type, resource_id, group_id, permission="use"):
    response = client.post(
        "/shares",
        json={
            "resource_type": resource_type,
            "resource_id": resource_id,
            "group_id": group_id,
            "permission": permission,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def _db_connection_body(name, host="db.internal", password="pgpass"):
    return {
        "connection_name": name,
        "database_type": "postgresql",
        "username": "svc",
        "password": password,
        "host": host,
        "port": 5432,
        "database": "analytics",
        "ssl_enabled": False,
    }


def _create_db_connection(client, name, **kwargs):
    response = client.post("/db_connection_lib", json=_db_connection_body(name, **kwargs))
    assert response.status_code == 200, response.text


def _db_connection_row(name, owner_id):
    with get_db_context() as db:
        row = (
            db.query(db_models.DatabaseConnection)
            .filter_by(connection_name=name, user_id=owner_id)
            .first()
        )
        assert row is not None
        return row.id, row.password_id


# ---------- database connections ----------


def test_grantee_resolves_shared_db_connection(users, client_for, team):
    alice = client_for("alice")
    _create_db_connection(alice, "shared_pg")
    conn_id, _ = _db_connection_row("shared_pg", users["alice"].id)
    _share(alice, "database_connection", conn_id, team)

    listed = client_for("bob").get("/db_connection_lib").json()
    shared = [c for c in listed if c["connection_name"] == "shared_pg"]
    assert len(shared) == 1
    assert shared[0]["access"] == {"is_owner": False, "access_level": "use", "shared_by": "share_alice"}
    assert "password" not in shared[0]

    # Execution seam: the schema loader resolves the shared connection and its
    # bundled secret stays OWNER-keyed (worker decrypts via the embedded id).
    with get_db_context() as db:
        schema = get_database_connection_schema(db, "shared_pg", users["bob"].id)
    assert schema is not None
    ciphertext = schema.password.get_secret_value()
    assert ciphertext.startswith(f"$ffsec$1${users['alice'].id}$")
    assert decrypt_secret(ciphertext).get_secret_value() == "pgpass"

    # Not shared with carol.
    with get_db_context() as db:
        assert get_database_connection_schema(db, "shared_pg", users["carol"].id) is None


def test_use_grantee_cannot_mutate_db_connection(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    _create_db_connection(alice, "use_only_pg")
    conn_id, _ = _db_connection_row("use_only_pg", users["alice"].id)
    _share(alice, "database_connection", conn_id, team)

    assert bob.put("/db_connection_lib", json=_db_connection_body("use_only_pg")).status_code == 404
    assert bob.delete("/db_connection_lib", params={"connection_name": "use_only_pg"}).status_code == 404
    assert alice.get("/db_connection_lib").status_code == 200


def test_manage_grantee_target_change_requires_credentials(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    _create_db_connection(alice, "managed_pg")
    conn_id, password_id = _db_connection_row("managed_pg", users["alice"].id)
    _share(alice, "database_connection", conn_id, team, permission="manage")

    # Repointing the host while keeping the owner's bundled password is rejected.
    repoint = bob.put("/db_connection_lib", json=_db_connection_body("managed_pg", host="evil.example.com", password=""))
    assert repoint.status_code == 422
    assert "re-entering" in repoint.json()["detail"]

    # Non-target edits without a password are fine (e.g. username).
    body = _db_connection_body("managed_pg", password="")
    body["username"] = "svc2"
    assert bob.put("/db_connection_lib", json=body).status_code == 200

    # Target change WITH new credentials is allowed — and the rotated password is
    # re-encrypted under the OWNER's key, not the editor's.
    ok = bob.put("/db_connection_lib", json=_db_connection_body("managed_pg", host="replica.internal", password="newpass"))
    assert ok.status_code == 200
    with get_db_context() as db:
        secret = db.get(db_models.Secret, password_id)
        assert secret.encrypted_value.startswith(f"$ffsec$1${users['alice'].id}$")
        assert decrypt_secret(secret.encrypted_value).get_secret_value() == "newpass"

    # Owner edits stay unrestricted.
    assert alice.put("/db_connection_lib", json=_db_connection_body("managed_pg", host="db2.internal", password="")).status_code == 200


def test_manage_grantee_can_delete_and_grants_are_cleaned(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    _create_db_connection(alice, "deletable_pg")
    conn_id, _ = _db_connection_row("deletable_pg", users["alice"].id)
    _share(alice, "database_connection", conn_id, team, permission="manage")

    assert bob.delete("/db_connection_lib", params={"connection_name": "deletable_pg"}).status_code == 200
    with get_db_context() as db:
        assert db.get(db_models.DatabaseConnection, conn_id) is None
        grants = (
            db.query(db_models.ResourceGrant)
            .filter_by(resource_type="database_connection", resource_id=conn_id)
            .count()
        )
    assert grants == 0


def test_own_connection_shadows_shared(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    _create_db_connection(alice, "shadow_pg", password="alice-pass")
    conn_id, _ = _db_connection_row("shadow_pg", users["alice"].id)
    _share(alice, "database_connection", conn_id, team)
    _create_db_connection(bob, "shadow_pg", password="bob-pass")

    with get_db_context() as db:
        schema = get_database_connection_schema(db, "shadow_pg", users["bob"].id)
    assert schema.password.get_secret_value().startswith(f"$ffsec$1${users['bob'].id}$")


# ---------- cloud connections ----------


def _cloud_body(name, endpoint_url="https://minio.internal", secret="s3secret"):
    return {
        "connection_name": name,
        "storage_type": "s3",
        "auth_method": "access_key",
        "aws_region": "eu-west-1",
        "aws_access_key_id": "AKIAEXAMPLE",
        "aws_secret_access_key": secret,
        "endpoint_url": endpoint_url,
        "verify_ssl": True,
    }


def test_shared_cloud_connection_end_to_end(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    body = _cloud_body("shared_s3")
    assert alice.post("/cloud_connections/cloud_connection", json=body).status_code == 200
    with get_db_context() as db:
        conn_id = (
            db.query(db_models.CloudStorageConnection)
            .filter_by(connection_name="shared_s3", user_id=users["alice"].id)
            .first()
            .id
        )
    _share(alice, "cloud_connection", conn_id, team, permission="manage")

    listed = bob.get("/cloud_connections/cloud_connections").json()
    shared = [c for c in listed if c["connection_name"] == "shared_s3"]
    assert len(shared) == 1
    assert shared[0]["access"]["access_level"] == "manage"
    assert "aws_secret_access_key" not in shared[0]

    # Execution: secrets decrypt in core for the grantee.
    with get_db_context() as db:
        schema = get_cloud_connection_schema(db, "shared_s3", users["bob"].id)
    assert schema.aws_secret_access_key.get_secret_value() == "s3secret"

    # Repointing the endpoint while keeping the owner's secret is rejected.
    repoint = _cloud_body("shared_s3", endpoint_url="https://evil.example.com", secret="")
    repoint["aws_secret_access_key"] = None
    response = bob.put("/cloud_connections/cloud_connection", json=repoint)
    assert response.status_code == 422

    # With re-entered credentials it works, re-encrypted under the OWNER's key.
    response = bob.put(
        "/cloud_connections/cloud_connection",
        json=_cloud_body("shared_s3", endpoint_url="https://minio2.internal", secret="news3secret"),
    )
    assert response.status_code == 200
    with get_db_context() as db:
        row = db.get(db_models.CloudStorageConnection, conn_id)
        secret = db.get(db_models.Secret, row.aws_secret_access_key_id)
        assert secret.encrypted_value.startswith(f"$ffsec$1${users['alice'].id}$")


# ---------- Google Analytics connections ----------


def _service_account_key():
    return json.dumps(
        {
            "type": "service_account",
            "client_email": "svc@project.iam.gserviceaccount.com",
            "private_key": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    )


def test_shared_ga_connection(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    response = alice.post(
        "/ga_connections/ga_connection/service_account",
        json={"connection_name": "shared_ga", "service_account_key": _service_account_key()},
    )
    assert response.status_code == 200, response.text
    with get_db_context() as db:
        conn_id = (
            db.query(db_models.GoogleAnalyticsConnection)
            .filter_by(connection_name="shared_ga", user_id=users["alice"].id)
            .first()
            .id
        )
    _share(alice, "ga_connection", conn_id, team)

    listed = bob.get("/ga_connections/ga_connections").json()
    shared = [c for c in listed if c["connection_name"] == "shared_ga"]
    assert len(shared) == 1
    assert shared[0]["access"]["access_level"] == "use"

    # Execution seam: grantee resolves the connection and the owner-keyed credential.
    with get_db_context() as db:
        assert get_ga_connection(db, "shared_ga", users["bob"].id) is not None
        blob = get_encrypted_credential(db, "shared_ga", users["bob"].id)
    assert blob.startswith(f"$ffsec$1${users['alice'].id}$")

    # use-grantee cannot edit metadata or delete; uniform 404.
    metadata = {"connection_name": "shared_ga", "description": "hacked"}
    assert bob.put("/ga_connections/ga_connection", json=metadata).status_code == 404
    assert bob.delete("/ga_connections/ga_connection", params={"connection_name": "shared_ga"}).status_code == 404

    # manage-grantee can edit metadata and delete, with grant cleanup.
    _grant_patch = alice.get("/shares", params={"resource_type": "ga_connection", "resource_id": conn_id}).json()
    alice.patch(f"/shares/{_grant_patch[0]['id']}", json={"permission": "manage"})
    assert bob.put("/ga_connections/ga_connection", json=metadata).status_code == 200
    assert bob.delete("/ga_connections/ga_connection", params={"connection_name": "shared_ga"}).status_code == 200
    with get_db_context() as db:
        grants = db.query(db_models.ResourceGrant).filter_by(resource_type="ga_connection", resource_id=conn_id).count()
    assert grants == 0


# ---------- Kafka connections ----------


def test_shared_kafka_connection(users, client_for, team):
    alice = client_for("alice")
    bob = client_for("bob")
    response = alice.post(
        "/kafka/connections",
        json={
            "connection_name": "shared_kafka",
            "bootstrap_servers": "broker.internal:9092",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "svc",
            "sasl_password": "kafkapass",
        },
    )
    assert response.status_code == 200, response.text
    conn_id = response.json()["id"]
    _share(alice, "kafka_connection", conn_id, team)

    listed = bob.get("/kafka/connections").json()
    shared = [c for c in listed if c["connection_name"] == "shared_kafka"]
    assert len(shared) == 1
    assert shared[0]["access"]["access_level"] == "use"

    with get_db_context() as db:
        assert get_kafka_connection(db, conn_id, users["bob"].id) is not None
        assert get_kafka_connection(db, conn_id, users["carol"].id) is None

    # use-grantee cannot mutate (uniform 404).
    assert bob.put(f"/kafka/connections/{conn_id}", json={"sasl_username": "x"}).status_code == 404
    assert bob.delete(f"/kafka/connections/{conn_id}").status_code == 404

    # manage: repointing bootstrap_servers without re-entering the password -> 422.
    grant = alice.get("/shares", params={"resource_type": "kafka_connection", "resource_id": conn_id}).json()[0]
    alice.patch(f"/shares/{grant['id']}", json={"permission": "manage"})
    repoint = bob.put(f"/kafka/connections/{conn_id}", json={"bootstrap_servers": "evil:9092"})
    assert repoint.status_code == 422
    ok = bob.put(
        f"/kafka/connections/{conn_id}",
        json={"bootstrap_servers": "broker2.internal:9092", "sasl_password": "newkafkapass"},
    )
    assert ok.status_code == 200, ok.text
    with get_db_context() as db:
        row = db.get(db_models.KafkaConnection, conn_id)
        secret = db.get(db_models.Secret, row.sasl_password_id)
        assert secret.encrypted_value.startswith(f"$ffsec$1${users['alice'].id}$")

    assert bob.delete(f"/kafka/connections/{conn_id}").status_code == 200
    with get_db_context() as db:
        grants = (
            db.query(db_models.ResourceGrant).filter_by(resource_type="kafka_connection", resource_id=conn_id).count()
        )
    assert grants == 0
