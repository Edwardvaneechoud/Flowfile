"""Tests for the per-kernel scratch FlowRegistration auto-provisioning.

Each kernel container gets an auto-created ``FlowRegistration`` row when
``KernelManager.create_kernel`` runs, deleted again on ``delete_kernel``. The
manager injects that id as ``source_registration_id`` whenever an
``ExecuteRequest`` arrives without one — so artifacts published from
interactive cells have a valid producer to point at without the user having
to register a flow manually.

These tests cover the unit boundary (manager methods + persistence layer)
against a real SQLite test DB; the integration with Core's
``/artifacts/prepare-upload`` is exercised by the existing kernel-integration
tests.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration, Kernel
from flowfile_core.kernel.manager import KernelManager
from flowfile_core.kernel.models import (
    ExecuteRequest,
    ExecuteResult,
    ImageFlavour,
    KernelInfo,
    KernelState,
    ResolvedPackage,
)
from flowfile_core.kernel.persistence import (
    get_all_kernel_scratch_ids,
    get_kernel_scratch_flow_id,
    save_kernel,
    set_kernel_scratch_flow_id,
)


def _bare_manager() -> KernelManager:
    """Construct a KernelManager with a mocked docker client and no real init."""
    with patch.object(KernelManager, "__init__", lambda self, *a, **kw: None):
        mgr = KernelManager.__new__(KernelManager)
        mgr._docker = MagicMock()
        mgr._kernels = {}
        mgr._kernel_owners = {}
        mgr._scratch_flow_ids = {}
        mgr._shared_volume = "/tmp/test"
        mgr._docker_network = None
        mgr._kernel_volume = None
        mgr._kernel_volume_type = None
        mgr._kernel_mount_target = None
        mgr._pull_state = {}
        mgr._pull_state_lock = threading.Lock()
    return mgr


def _kernel_info(kernel_id: str = "k-scratch") -> KernelInfo:
    return KernelInfo(
        id=kernel_id,
        name=f"test-{kernel_id}",
        state=KernelState.STOPPED,
        packages=[],
        resolved_packages=[],
        image_flavour=ImageFlavour.BASE,
    )


def _seed_kernel_row(kernel_id: str, user_id: int = 1) -> None:
    """Persist a kernel row so ``set_kernel_scratch_flow_id`` has a target."""
    with get_db_context() as db:
        save_kernel(db, _kernel_info(kernel_id), user_id)


@pytest.fixture(autouse=True)
def _cleanup_scratch_rows():
    """Wipe scratch-related rows between tests so each starts clean."""
    with get_db_context() as db:
        db.query(Kernel).delete()
        db.query(FlowRegistration).filter(
            FlowRegistration.name.like("_kernel_scratch_%")
        ).delete(synchronize_session=False)
        db.commit()
    yield
    with get_db_context() as db:
        db.query(Kernel).delete()
        db.query(FlowRegistration).filter(
            FlowRegistration.name.like("_kernel_scratch_%")
        ).delete(synchronize_session=False)
        db.commit()


# ---------------------------------------------------------------------------
# Provision / discard lifecycle
# ---------------------------------------------------------------------------


class TestProvisionScratchFlow:
    def test_provision_creates_row_and_caches_id(self):
        mgr = _bare_manager()
        _seed_kernel_row("k-1", user_id=1)
        mgr._kernel_owners["k-1"] = 1
        mgr._provision_scratch_flow("k-1", user_id=1)
        assert "k-1" in mgr._scratch_flow_ids
        scratch_id = mgr._scratch_flow_ids["k-1"]
        with get_db_context() as db:
            row = db.get(FlowRegistration, scratch_id)
            assert row is not None
            assert row.name == "_kernel_scratch_k-1"
            assert row.flow_path == "<kernel:k-1>"
            assert row.owner_id == 1
            # And the kernel row points back at it
            assert get_kernel_scratch_flow_id(db, "k-1") == scratch_id

    def test_provision_uses_default_namespace_when_seeded(self):
        """The scratch flow must live under the default namespace so artifacts
        published from cells (without explicit ``namespace_id``) inherit it and
        appear in the catalog UI's tree.
        """
        from flowfile_core.catalog import SQLAlchemyCatalogRepository
        from flowfile_core.catalog.service import CatalogService

        mgr = _bare_manager()
        _seed_kernel_row("k-ns", user_id=1)
        mgr._kernel_owners["k-ns"] = 1
        mgr._provision_scratch_flow("k-ns", user_id=1)

        scratch_id = mgr._scratch_flow_ids["k-ns"]
        with get_db_context() as db:
            row = db.get(FlowRegistration, scratch_id)
            default_ns = CatalogService(SQLAlchemyCatalogRepository(db)).get_default_namespace_id()
            # When the seeder has run (and conftest sets it up), the scratch
            # flow should point at the default namespace. When it hasn't, both
            # are ``None`` — the contract still holds.
            assert row.namespace_id == default_ns

    def test_provision_is_idempotent_when_already_cached(self):
        mgr = _bare_manager()
        _seed_kernel_row("k-2", user_id=1)
        mgr._kernel_owners["k-2"] = 1
        mgr._provision_scratch_flow("k-2", user_id=1)
        first = mgr._scratch_flow_ids["k-2"]
        # Second call must not create a duplicate row.
        mgr._provision_scratch_flow("k-2", user_id=1)
        assert mgr._scratch_flow_ids["k-2"] == first
        with get_db_context() as db:
            rows = (
                db.query(FlowRegistration)
                .filter(FlowRegistration.name == "_kernel_scratch_k-2")
                .all()
            )
            assert len(rows) == 1


class TestDiscardScratchFlow:
    def test_discard_removes_row_and_clears_cache(self):
        mgr = _bare_manager()
        _seed_kernel_row("k-3", user_id=1)
        mgr._kernel_owners["k-3"] = 1
        mgr._provision_scratch_flow("k-3", user_id=1)
        scratch_id = mgr._scratch_flow_ids["k-3"]

        mgr._discard_scratch_flow("k-3")
        assert "k-3" not in mgr._scratch_flow_ids
        with get_db_context() as db:
            assert db.get(FlowRegistration, scratch_id) is None

    def test_discard_is_silent_when_nothing_cached(self):
        mgr = _bare_manager()
        # No row to remove; must not raise.
        mgr._discard_scratch_flow("never-existed")


# ---------------------------------------------------------------------------
# get_scratch_flow_id — cache + lazy upgrade
# ---------------------------------------------------------------------------


class TestGetScratchFlowId:
    def test_returns_cached_value_without_db_hit(self):
        mgr = _bare_manager()
        mgr._scratch_flow_ids["k-cached"] = 42
        assert mgr.get_scratch_flow_id("k-cached") == 42

    def test_returns_none_when_kernel_unknown(self):
        mgr = _bare_manager()
        assert mgr.get_scratch_flow_id("nope") is None

    def test_lazily_provisions_for_legacy_kernel(self):
        """Pre-existing kernel rows have NULL in the new column — first call
        to ``get_scratch_flow_id`` should provision one on the fly."""
        mgr = _bare_manager()
        _seed_kernel_row("k-legacy", user_id=1)
        mgr._kernel_owners["k-legacy"] = 1
        # Cache is empty: simulates a kernel restored from DB without a scratch_id.
        assert "k-legacy" not in mgr._scratch_flow_ids

        scratch_id = mgr.get_scratch_flow_id("k-legacy")
        assert scratch_id is not None
        with get_db_context() as db:
            row = db.get(FlowRegistration, scratch_id)
            assert row is not None
            assert row.name == "_kernel_scratch_k-legacy"


# ---------------------------------------------------------------------------
# ExecuteRequest injection
# ---------------------------------------------------------------------------


class TestExecuteScratchInjection:
    """``KernelManager.execute_sync`` falls back to the kernel's scratch
    FlowRegistration id when the request omits ``source_registration_id``.

    Uses the sync path because mocking ``httpx.Client`` is straightforward;
    the async ``execute`` path runs the same injection logic, so covering
    one is sufficient.
    """

    def _set_up_kernel(self, mgr: KernelManager) -> int:
        kernel = _kernel_info("k-exec")
        kernel.state = KernelState.IDLE  # skip _ensure_running
        kernel.container_id = "fake-container"
        kernel.port = 9999
        mgr._kernels["k-exec"] = kernel
        mgr._kernel_owners["k-exec"] = 1
        mgr._scratch_flow_ids["k-exec"] = 7777
        return 7777

    def _patched_httpx_client(self, captured: dict):
        """Return a context manager that swaps ``httpx.Client`` for a mock."""
        fake_resp = MagicMock()
        fake_resp.raise_for_status = MagicMock()
        fake_resp.json = MagicMock(return_value={"success": True})

        client_instance = MagicMock()
        client_instance.__enter__ = lambda s: client_instance
        client_instance.__exit__ = lambda s, *args: None

        def _post(url, json=None):
            captured["body"] = json
            return fake_resp

        client_instance.post = _post
        return patch("flowfile_core.kernel.manager.httpx.Client", return_value=client_instance)

    def test_execute_sync_injects_scratch_into_request_with_none(self):
        mgr = _bare_manager()
        expected_id = self._set_up_kernel(mgr)
        request = ExecuteRequest(node_id=1, code="pass", flow_id=0)
        captured: dict = {}
        with self._patched_httpx_client(captured):
            mgr.execute_sync("k-exec", request)
        assert captured["body"]["source_registration_id"] == expected_id

    def test_execute_sync_does_not_overwrite_explicit_source(self):
        mgr = _bare_manager()
        self._set_up_kernel(mgr)
        request = ExecuteRequest(
            node_id=1, code="pass", flow_id=0, source_registration_id=42
        )
        captured: dict = {}
        with self._patched_httpx_client(captured):
            mgr.execute_sync("k-exec", request)
        assert captured["body"]["source_registration_id"] == 42


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


class TestPersistenceHelpers:
    def test_set_and_get_roundtrip(self):
        _seed_kernel_row("k-p", user_id=1)
        with get_db_context() as db:
            assert get_kernel_scratch_flow_id(db, "k-p") is None
            flow = FlowRegistration(
                name="_kernel_scratch_k-p",
                flow_path="<kernel:k-p>",
                owner_id=1,
            )
            db.add(flow)
            db.commit()
            db.refresh(flow)
            scratch_id = flow.id  # capture before the session closes
            set_kernel_scratch_flow_id(db, "k-p", scratch_id)
        with get_db_context() as db:
            assert get_kernel_scratch_flow_id(db, "k-p") == scratch_id

    def test_get_all_kernel_scratch_ids_bulk_load(self):
        _seed_kernel_row("k-a", user_id=1)
        _seed_kernel_row("k-b", user_id=1)
        with get_db_context() as db:
            flow = FlowRegistration(
                name="_kernel_scratch_k-a", flow_path="<kernel:k-a>", owner_id=1
            )
            db.add(flow)
            db.commit()
            db.refresh(flow)
            scratch_id = flow.id
            set_kernel_scratch_flow_id(db, "k-a", scratch_id)
        with get_db_context() as db:
            mapping = get_all_kernel_scratch_ids(db)
            assert mapping["k-a"] == scratch_id
            assert mapping["k-b"] is None


# ---------------------------------------------------------------------------
# Imports we reference so the linter doesn't trip
# ---------------------------------------------------------------------------

# Silence unused-import noise — ``ResolvedPackage`` is referenced indirectly
# via ``KernelInfo``. Keeping the explicit import documents the dependency.
_ = ResolvedPackage
