"""Integration tests for artifact persistence through the full API stack.

These tests swap the global ``artifact_store`` in ``main.py`` with a
persistence-enabled store so that the FastAPI endpoints exercise the real
persist → recover → cleanup flow.  This validates the user stories:

    C1  Automatic artifact persistence (publish → disk)
    C2  Automatic recovery on restart (restart → lazy/eager load)
    C3  Recovery mode configuration (lazy / eager / none)
    C4  Manual recovery trigger (POST /recover)
    C5  Artifact cleanup (POST /cleanup)
    C6  Persistence status visibility (/persistence, /health, /recovery-status)
"""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kernel_runtime.artifact_store import ArtifactStore, RecoveryMode
from kernel_runtime.persistence import PersistenceManager

import kernel_runtime.main as _main_mod


# ---------------------------------------------------------------------------
# Fixtures — swap the global store with a persistence-enabled one
# ---------------------------------------------------------------------------


@pytest.fixture()
def _persistent_app(tmp_path):
    """Swap module-level globals so the FastAPI app uses persistence.

    Yields ``(client, persistence_manager, artifacts_dir)`` and restores
    the original globals on teardown.
    """
    pm = PersistenceManager(str(tmp_path), kernel_id="integration-kernel")
    store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.LAZY)

    prev_store = _main_mod.artifact_store
    prev_pm = _main_mod._persistence_manager
    prev_mode = _main_mod._recovery_mode
    prev_kid = _main_mod.KERNEL_ID

    _main_mod.artifact_store = store
    _main_mod._persistence_manager = pm
    _main_mod._recovery_mode = RecoveryMode.LAZY
    _main_mod.KERNEL_ID = "integration-kernel"

    with TestClient(_main_mod.app) as client:
        yield client, pm, tmp_path / "integration-kernel"

    _main_mod.artifact_store = prev_store
    _main_mod._persistence_manager = prev_pm
    _main_mod._recovery_mode = prev_mode
    _main_mod.KERNEL_ID = prev_kid


def _publish(client: TestClient, name: str, value: str, node_id: int = 1):
    """Helper: publish an artifact via /execute."""
    resp = client.post(
        "/execute",
        json={
            "node_id": node_id,
            "code": f'flowfile.publish_artifact("{name}", {value})',
            "input_paths": {},
            "output_dir": "",
        },
    )
    data = resp.json()
    assert data["success"], f"publish failed: {data['error']}"
    return data


def _read(client: TestClient, name: str, node_id: int = 99):
    """Helper: read an artifact via /execute and print it."""
    resp = client.post(
        "/execute",
        json={
            "node_id": node_id,
            "code": f'val = flowfile.read_artifact("{name}")\nprint(repr(val))',
            "input_paths": {},
            "output_dir": "",
        },
    )
    return resp.json()


# ===================================================================
# C1: Automatic Artifact Persistence
# ===================================================================


class TestC1AutoPersistence:
    """publish_artifact() transparently writes to disk."""

    def test_publish_creates_disk_files(self, _persistent_app):
        client, pm, artifacts_dir = _persistent_app

        _publish(client, "my_model", '{"accuracy": 0.95}')

        # data.artifact and meta.json should exist on disk
        assert (artifacts_dir / "my_model" / "data.artifact").exists()
        assert (artifacts_dir / "my_model" / "meta.json").exists()

    def test_publish_metadata_on_disk(self, _persistent_app):
        client, pm, artifacts_dir = _persistent_app

        _publish(client, "encoder", '{"type": "label"}', node_id=7)

        meta = json.loads((artifacts_dir / "encoder" / "meta.json").read_text())
        assert meta["name"] == "encoder"
        assert meta["node_id"] == 7
        assert meta["type_name"] == "dict"
        assert "checksum" in meta
        assert "persisted_at" in meta
        assert meta["size_on_disk"] > 0

    def test_persistence_endpoint_shows_persisted(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "item", "42")

        resp = client.get("/persistence")
        data = resp.json()
        assert data["persistence_enabled"] is True
        assert data["persisted_count"] == 1
        assert data["memory_only_count"] == 0
        assert "item" in data["artifacts"]
        assert data["artifacts"]["item"]["persisted"] is True
        assert data["artifacts"]["item"]["in_memory"] is True

    def test_artifacts_listing_shows_persisted_flag(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "model", '"v1"')

        resp = client.get("/artifacts")
        data = resp.json()
        assert data["model"]["persisted"] is True


# ===================================================================
# C2: Automatic Recovery on Kernel Restart
# ===================================================================


class TestC2RestartRecovery:
    """After a kernel restart, persisted artifacts are recoverable."""

    def test_lazy_recovery_after_simulated_restart(self, _persistent_app):
        """The key scenario: publish → restart → read works."""
        client, pm, _ = _persistent_app

        # 1. Publish artifact (saved to memory + disk)
        _publish(client, "trained_model", '{"weights": [1, 2, 3]}')

        # 2. Simulate kernel restart — clear memory, keep disk
        client.post("/clear")

        # Verify it's gone from memory
        status = client.get("/recovery-status").json()
        assert status["artifacts_in_memory"] == 0
        assert status["artifacts_persisted"] == 1
        assert "trained_model" in status["not_yet_loaded"]

        # 3. Read the artifact — lazy loads from disk
        result = _read(client, "trained_model")
        assert result["success"], f"read failed: {result['error']}"
        assert "weights" in result["stdout"]
        assert "[1, 2, 3]" in result["stdout"]

    def test_lazy_recovery_shows_in_artifacts_list(self, _persistent_app):
        """After restart, /artifacts shows disk-only artifacts as not-loaded."""
        client, pm, _ = _persistent_app

        _publish(client, "model", '"v1"')
        client.post("/clear")

        listing = client.get("/artifacts").json()
        assert "model" in listing
        assert listing["model"]["persisted"] is True
        assert listing["model"]["loaded"] is False

    def test_full_train_and_apply_after_restart(self, _persistent_app):
        """
        Scenario from spec: Train publishes model → restart → Apply reads model.
        """
        client, pm, _ = _persistent_app

        # Train step: publish a "model" artifact
        train_code = (
            'import json\n'
            'model = {"type": "linear", "coefficients": [0.5, 0.3]}\n'
            'flowfile.publish_artifact("model", model)\n'
            'print("training complete")\n'
        )
        resp = client.post(
            "/execute",
            json={"node_id": 1, "code": train_code, "input_paths": {}, "output_dir": ""},
        )
        assert resp.json()["success"]
        assert "model" in resp.json()["artifacts_published"]

        # Simulate kernel crash/restart
        client.post("/clear")

        # Apply step: read model and use it
        apply_code = (
            'model = flowfile.read_artifact("model")\n'
            'prediction = sum(c * x for c, x in zip(model["coefficients"], [10, 20]))\n'
            'print(f"prediction={prediction}")\n'
        )
        resp = client.post(
            "/execute",
            json={"node_id": 2, "code": apply_code, "input_paths": {}, "output_dir": ""},
        )
        data = resp.json()
        assert data["success"], f"Apply failed: {data['error']}"
        assert "prediction=11.0" in data["stdout"]


# ===================================================================
# C3: Recovery Mode Configuration
# ===================================================================


class TestC3RecoveryModes:
    """Different recovery modes produce different startup behavior."""

    def test_eager_mode_preloads_on_init(self, tmp_path):
        """EAGER mode: all artifacts loaded into memory immediately."""
        pm = PersistenceManager(str(tmp_path), kernel_id="eager-k")
        # Pre-persist an artifact (simulates previous session)
        pm.persist("model", {"v": 1}, {"name": "model", "node_id": 1})

        store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.EAGER)

        prev_store = _main_mod.artifact_store
        _main_mod.artifact_store = store
        try:
            with TestClient(_main_mod.app) as client:
                status = client.get("/recovery-status").json()
                assert status["recovery_mode"] == "eager"
                assert status["artifacts_in_memory"] == 1
                assert status["artifacts_recovered"] == {"model": True}
                assert status["not_yet_loaded"] == []

                # Can read without lazy-load trigger
                result = _read(client, "model")
                assert result["success"]
        finally:
            _main_mod.artifact_store = prev_store

    def test_none_mode_clears_disk_on_init(self, tmp_path):
        """NONE mode: all persisted data cleared on startup."""
        pm = PersistenceManager(str(tmp_path), kernel_id="none-k")
        pm.persist("old_model", {"v": 1}, {"name": "old_model", "node_id": 1})

        store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.NONE)

        prev_store = _main_mod.artifact_store
        _main_mod.artifact_store = store
        try:
            with TestClient(_main_mod.app) as client:
                # Nothing should be available
                listing = client.get("/artifacts").json()
                assert listing == {}

                # Can't read the old artifact
                result = _read(client, "old_model")
                assert not result["success"]
                assert "not found" in result["error"]
        finally:
            _main_mod.artifact_store = prev_store

    def test_lazy_mode_does_not_preload(self, tmp_path):
        """LAZY mode: nothing loaded on init, loaded on first access."""
        pm = PersistenceManager(str(tmp_path), kernel_id="lazy-k")
        pm.persist("model", [1, 2, 3], {"name": "model", "node_id": 1})

        store = ArtifactStore(persistence=pm, recovery_mode=RecoveryMode.LAZY)

        prev_store = _main_mod.artifact_store
        _main_mod.artifact_store = store
        try:
            with TestClient(_main_mod.app) as client:
                # Not in memory yet
                status = client.get("/recovery-status").json()
                assert status["artifacts_in_memory"] == 0
                assert status["artifacts_persisted"] == 1
                assert status["not_yet_loaded"] == ["model"]

                # Read triggers lazy load
                result = _read(client, "model")
                assert result["success"]
                assert "[1, 2, 3]" in result["stdout"]

                # Now it's in memory
                status = client.get("/recovery-status").json()
                assert status["artifacts_in_memory"] == 1
                assert status["not_yet_loaded"] == []
        finally:
            _main_mod.artifact_store = prev_store


# ===================================================================
# C4: Manual Recovery Trigger
# ===================================================================


class TestC4ManualRecovery:
    """POST /recover loads all persisted artifacts into memory."""

    def test_recover_loads_all(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "model_a", '"alpha"')
        _publish(client, "model_b", '"beta"')

        # Simulate restart
        client.post("/clear")

        # Manually recover
        resp = client.post("/recover")
        data = resp.json()
        assert data["status"] == "recovery_complete"
        assert data["artifacts"]["model_a"] == "recovered"
        assert data["artifacts"]["model_b"] == "recovered"

        # Both should be in memory now
        status = client.get("/recovery-status").json()
        assert status["artifacts_in_memory"] == 2
        assert status["not_yet_loaded"] == []

    def test_recover_already_loaded(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "item", "42")

        resp = client.post("/recover")
        assert resp.json()["artifacts"]["item"] == "already_loaded"

    def test_recovery_status_endpoint(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "model", '{"v": 1}')

        resp = client.get("/recovery-status")
        data = resp.json()
        assert data["recovery_mode"] == "lazy"
        assert data["persistence_enabled"] is True
        assert data["artifacts_in_memory"] == 1
        assert data["artifacts_persisted"] == 1
        assert data["artifacts_recovered"] == {}


# ===================================================================
# C5: Artifact Cleanup
# ===================================================================


class TestC5Cleanup:
    """POST /cleanup removes persisted artifacts from disk."""

    def test_cleanup_specific_artifacts(self, _persistent_app):
        client, pm, artifacts_dir = _persistent_app

        _publish(client, "keep_me", '"important"')
        _publish(client, "delete_me", '"temporary"')

        resp = client.post("/cleanup", json={"artifact_names": ["delete_me"]})
        data = resp.json()
        assert data["status"] == "cleanup_complete"
        assert data["deleted"] == ["delete_me"]

        # Verify: keep_me still accessible, delete_me gone
        assert _read(client, "keep_me")["success"]
        assert not _read(client, "delete_me")["success"]

        # Verify disk state
        assert (artifacts_dir / "keep_me" / "data.artifact").exists()
        assert not (artifacts_dir / "delete_me").exists()

    def test_cleanup_removes_from_memory_and_disk(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "item", "42")

        # Verify it's persisted
        info = client.get("/persistence").json()
        assert info["persisted_count"] == 1

        # Cleanup
        client.post("/cleanup", json={"artifact_names": ["item"]})

        # Verify fully removed
        info = client.get("/persistence").json()
        assert info["total_artifacts"] == 0

    def test_cleanup_by_age(self, _persistent_app):
        client, pm, artifacts_dir = _persistent_app

        _publish(client, "old_item", '{"v": "old"}')

        # Manually backdate the persisted_at timestamp
        meta_path = artifacts_dir / "old_item" / "meta.json"
        meta = json.loads(meta_path.read_text())
        from datetime import datetime, timedelta, timezone
        old_time = datetime.now(timezone.utc) - timedelta(hours=48)
        meta["persisted_at"] = old_time.isoformat()
        meta_path.write_text(json.dumps(meta))

        _publish(client, "new_item", '{"v": "new"}')

        # Cleanup artifacts older than 24 hours
        resp = client.post("/cleanup", json={"max_age_hours": 24})
        data = resp.json()
        assert "old_item" in data["deleted"]
        assert "new_item" not in data["deleted"]

    def test_clear_all_via_cleanup(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "a", "1")
        _publish(client, "b", "2")
        _publish(client, "c", "3")

        resp = client.post("/cleanup", json={"artifact_names": ["a", "b", "c"]})
        assert len(resp.json()["deleted"]) == 3

        info = client.get("/persistence").json()
        assert info["total_artifacts"] == 0


# ===================================================================
# C6: Persistence Status Visibility
# ===================================================================


class TestC6StatusVisibility:
    """Endpoints expose persistence state for observability."""

    def test_health_shows_persistence_stats(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "model", '{"w": [1]}')

        resp = client.get("/health")
        data = resp.json()
        assert "persistence" in data
        assert data["persistence"]["enabled"] is True
        assert data["persistence"]["kernel_id"] == "integration-kernel"
        assert data["persistence"]["recovery_mode"] == "lazy"
        assert data["persistence"]["persisted_count"] == 1
        assert data["persistence"]["disk_usage_bytes"] > 0

    def test_persistence_info_mixed_state(self, _persistent_app):
        """Show correct status for in-memory + disk-only artifacts."""
        client, pm, _ = _persistent_app

        _publish(client, "in_both", '"data"')
        _publish(client, "disk_only_later", '"more data"')

        # Clear memory — disk_only_later is now disk-only
        client.post("/clear")

        # Publish new one (memory + disk)
        _publish(client, "fresh", '"new"')

        info = client.get("/persistence").json()
        assert info["total_artifacts"] == 3
        assert info["artifacts"]["fresh"]["in_memory"] is True
        assert info["artifacts"]["fresh"]["persisted"] is True
        assert info["artifacts"]["disk_only_later"]["in_memory"] is False
        assert info["artifacts"]["disk_only_later"]["persisted"] is True

    def test_recovery_status_tracks_recovered(self, _persistent_app):
        client, pm, _ = _persistent_app

        _publish(client, "model", '{"v": 1}')

        # Restart simulation
        client.post("/clear")

        # Lazy-load by reading
        _read(client, "model")

        status = client.get("/recovery-status").json()
        assert status["artifacts_recovered"] == {"model": True}

    def test_artifact_count_includes_disk_only(self, _persistent_app):
        """Health endpoint counts disk-only artifacts in lazy mode."""
        client, pm, _ = _persistent_app

        _publish(client, "a", "1")
        _publish(client, "b", "2")
        client.post("/clear")

        health = client.get("/health").json()
        assert health["artifact_count"] == 2  # both still visible via list_all
