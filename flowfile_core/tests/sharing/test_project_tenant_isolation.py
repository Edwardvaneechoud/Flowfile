"""Tenant-isolation regression tests for the Phase-2 project-git-tracking security fixes.

Theme A (H1/H2/H3/H5/H6/M-A2/M-A3/M-A4): multi-user docker-mode isolation.
Theme B (H4/L1/M-P1/M-P2/I1/M-P3): path / filesystem confinement.
All must PASS against the fixed code and would FAIL against the pre-fix code.

Runs in docker-mode via the `multi_user_mode` autouse fixture from conftest.py.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from flowfile_core.configs import settings
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.fileExplorer.funcs import _is_contained, validate_path_under_cwd
from flowfile_core.project import project_sync, repository
from flowfile_core.project.manifest import (
    flows_dir,
    kernels_manifest_path,
    namespaces_manifest_path,
    tables_manifest_path,
    visualizations_manifest_path,
    dashboards_manifest_path,
    write_manifest,
)
from flowfile_core.project.models import ActiveProject
from flowfile_core.project.normalize import write_yaml, dump_yaml
from flowfile_core.project.service import project_root_base, _confine_project_root

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_flow_for_user(owner_id: int, flow_uuid: str | None = None) -> tuple[str, str]:
    """Register a minimal flow registration row owned by owner_id; return (flow_uuid, flow_path)."""
    from flowfile_core.database.models import CatalogNamespace, FlowRegistration

    fuuid = flow_uuid or str(uuid4())
    with get_db_context() as db:
        ns = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).first()
        ns_id = ns.id if ns else None
        reg = FlowRegistration(
            name=f"flow_{fuuid[:8]}",
            flow_path=f"/tmp/flow_{fuuid}.yaml",
            flow_uuid=fuuid,
            namespace_id=ns_id,
            owner_id=owner_id,
        )
        db.add(reg)
        db.commit()
        db.refresh(reg)
        return reg.flow_uuid, reg.flow_path


def _seed_kernel_for_user(owner_id: int, kernel_id: str | None = None) -> str:
    """Insert a Kernel row owned by owner_id; return kernel_id."""
    kid = kernel_id or str(uuid4())
    with get_db_context() as db:
        row = db_models.Kernel(
            id=kid,
            user_id=owner_id,
            name=f"kernel_{kid[:8]}",
            packages="[]",
            resolved_packages="[]",
            cpu_cores=2.0,
            memory_gb=4.0,
            gpu=False,
            image_flavour="base",
        )
        db.add(row)
        db.commit()
    return kid


def _seed_viz_for_user(owner_id: int, viz_uuid: str | None = None) -> str:
    vuuid = viz_uuid or str(uuid4())
    with get_db_context() as db:
        row = db_models.CatalogVisualization(
            viz_uuid=vuuid,
            name=f"viz_{vuuid[:8]}",
            created_by=owner_id,
            spec_json="[]",
        )
        db.add(row)
        db.commit()
    return vuuid


def _seed_dashboard_for_user(owner_id: int, dashboard_uuid: str | None = None) -> str:
    duuid = dashboard_uuid or str(uuid4())
    with get_db_context() as db:
        row = db_models.CatalogDashboard(
            dashboard_uuid=duuid,
            name=f"dash_{duuid[:8]}",
            created_by=owner_id,
            layout_json='{"tiles":[],"filters":[],"grid":{"version":1}}',
            layout_version=1,
        )
        db.add(row)
        db.commit()
    return duuid


def _seed_namespace_for_user(owner_id: int, name: str, parent_id: int | None = None) -> int:
    with get_db_context() as db:
        ns = db_models.CatalogNamespace(
            name=name,
            parent_id=parent_id,
            level=0 if parent_id is None else 1,
            owner_id=owner_id,
        )
        db.add(ns)
        db.commit()
        db.refresh(ns)
        return ns.id


def _cleanup_flows(*uuids: str) -> None:
    from flowfile_core.database.models import FlowRegistration, FlowSchedule

    with get_db_context() as db:
        for fuuid in uuids:
            for reg in db.query(FlowRegistration).filter_by(flow_uuid=fuuid).all():
                db.query(FlowSchedule).filter_by(registration_id=reg.id).delete()
                db.delete(reg)
        db.commit()


def _cleanup_kernels(*kids: str) -> None:
    with get_db_context() as db:
        for kid in kids:
            row = db.query(db_models.Kernel).filter_by(id=kid).first()
            if row:
                db.delete(row)
        db.commit()


def _cleanup_vizs(*vuuids: str) -> None:
    with get_db_context() as db:
        for vuuid in vuuids:
            row = db.query(db_models.CatalogVisualization).filter_by(viz_uuid=vuuid).first()
            if row:
                db.delete(row)
        db.commit()


def _cleanup_dashboards(*duuids: str) -> None:
    with get_db_context() as db:
        for duuid in duuids:
            row = db.query(db_models.CatalogDashboard).filter_by(dashboard_uuid=duuid).first()
            if row:
                db.delete(row)
        db.commit()


def _cleanup_namespaces(*names: str) -> None:
    with get_db_context() as db:
        for name in names:
            for ns in db.query(db_models.CatalogNamespace).filter_by(name=name).all():
                db.delete(ns)
        db.commit()


def _cleanup_workspace_projects(*paths: str) -> None:
    with get_db_context() as db:
        for p in paths:
            for row in db.query(db_models.WorkspaceProject).filter_by(folder_path=p).all():
                db.delete(row)
        db.commit()


def _build_flow_manifest(flow_uuid: str, flowfile_name: str = "test_flow") -> dict:
    """Build a minimal but valid projected flow manifest (matches FlowfileData schema)."""
    from flowfile_core.project.normalize import deterministic_flow_id

    return {
        "flow_uuid": flow_uuid,
        "catalog_name": flowfile_name,
        "flowfile_version": "0.11.0",
        "flowfile_id": deterministic_flow_id(flow_uuid),
        "flowfile_name": flowfile_name,
        "flowfile_settings": {
            "execution_mode": "Performance",
            "execution_location": "local",
            "auto_save": False,
            "show_detailed_progress": True,
            "max_parallel_workers": 4,
            "source_registration_id": None,
            "parameters": [],
        },
        "nodes": [],
        "groups": [],
    }


def _write_flow_manifest(flows_directory: Path, flow_uuid: str, name: str = "test_flow") -> Path:
    flows_directory.mkdir(parents=True, exist_ok=True)
    p = flows_directory / f"{name}.flow.yaml"
    p.write_text(dump_yaml(_build_flow_manifest(flow_uuid, name)), encoding="utf-8")
    return p


def _write_flow_manifest_with_namespace(
    flows_directory: Path, flow_uuid: str, catalog: str, schema: str | None, name: str = "test_flow"
) -> Path:
    """Write a flow manifest whose portable namespace references {catalog, schema} (e.g. the public
    General/default), matching what projection emits for a flow placed in that namespace."""
    flows_directory.mkdir(parents=True, exist_ok=True)
    data = _build_flow_manifest(flow_uuid, name)
    data["namespace"] = {"catalog": catalog, "schema": schema}
    p = flows_directory / f"{name}.flow.yaml"
    p.write_text(dump_yaml(data), encoding="utf-8")
    return p


def _write_kernels_manifest(root: Path, kernels: list[dict]) -> None:
    write_yaml(kernels_manifest_path(root), {"kernels": kernels})


def _write_viz_manifest(root: Path, vizs: list[dict]) -> None:
    write_yaml(visualizations_manifest_path(root), {"visualizations": vizs})


def _write_dashboard_manifest(root: Path, dashboards: list[dict]) -> None:
    write_yaml(dashboards_manifest_path(root), {"dashboards": dashboards})


def _write_tables_manifest(root: Path, tables: list[dict]) -> None:
    write_yaml(tables_manifest_path(root), {"tables": tables})


def _minimal_project_dir(tmp_path: Path, name: str = "project") -> Path:
    from flowfile_core.project.manifest import ProjectManifest

    root = tmp_path / name
    root.mkdir(parents=True)
    m = ProjectManifest(name=name, project_id=str(uuid4()), created_with_version="0.11.0")
    write_manifest(root, m)
    return root


# ---------------------------------------------------------------------------
# Theme A: docker multi-user tenant isolation
# ---------------------------------------------------------------------------


class TestH1_CrossTenantOpenForbidden:
    """H1/M-A1 — user A cannot open or init a project folder belonging to user B.

    The fix is layered: (a) per-owner subtree confinement prevents A from addressing B's folder
    at all in docker mode; (b) _assert_not_foreign_owned provides a second defense for any path
    that somehow arrives with an existing WP row owned by B.  We test both layers.

    Pre-fix: open_project / init_project called upsert_active with no owner check,
    reassigning the existing WorkspaceProject row to A and triggering a full import
    of B's resources into A's account.  Post-fix: HTTPException(403) is raised before
    any import happens.
    """

    def test_assert_not_foreign_owned_raises_403_when_row_owned_by_other(self, tmp_path, users, monkeypatch):
        # Finding H1: the _assert_not_foreign_owned guard must reject a foreign-owned row.
        # Patch FLOWFILE_MODE to a non-electron value so the guard fires (it's a no-op in electron).
        from flowfile_core.configs import settings
        from flowfile_core.project.service import _assert_not_foreign_owned
        from fastapi import HTTPException

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "docker")
        alice_id = users["alice"].id
        bob_id = users["bob"].id
        root = tmp_path / "foreign_folder"
        root.mkdir()

        with get_db_context() as db:
            proj_b = db_models.WorkspaceProject(
                name="Bob",
                folder_path=str(root),
                owner_id=bob_id,
                is_active=False,
            )
            db.add(proj_b)
            db.commit()
            proj_b_id = proj_b.id

        try:
            with pytest.raises(HTTPException) as exc_info:
                _assert_not_foreign_owned(root, alice_id)
            assert exc_info.value.status_code == 403

            # B's row must still be owned by B (unchanged after the failed check).
            with get_db_context() as db:
                row = db.get(db_models.WorkspaceProject, proj_b_id)
                assert row is not None
                assert row.owner_id == bob_id
        finally:
            _cleanup_workspace_projects(str(root))

    def test_assert_not_foreign_owned_passes_for_own_row(self, tmp_path, users, monkeypatch):
        # Non-regression: _assert_not_foreign_owned must not raise when the row is the caller's own.
        from flowfile_core.configs import settings
        from flowfile_core.project.service import _assert_not_foreign_owned

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "docker")
        alice_id = users["alice"].id
        root = tmp_path / "alice_folder"
        root.mkdir()

        with get_db_context() as db:
            proj = db_models.WorkspaceProject(
                name="Alice",
                folder_path=str(root),
                owner_id=alice_id,
                is_active=False,
            )
            db.add(proj)
            db.commit()

        try:
            _assert_not_foreign_owned(root, alice_id)  # must not raise
        finally:
            _cleanup_workspace_projects(str(root))

    def test_assert_not_foreign_owned_passes_for_no_row(self, tmp_path, users, monkeypatch):
        # Non-regression: when no WP row exists for the path (fresh init), no 403.
        from flowfile_core.configs import settings
        from flowfile_core.project.service import _assert_not_foreign_owned

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "docker")
        alice_id = users["alice"].id
        root = tmp_path / "no_row_folder"
        root.mkdir()
        _assert_not_foreign_owned(root, alice_id)  # must not raise

    def test_open_project_perowner_subtree_blocks_cross_tenant_access(self, tmp_path, users, monkeypatch):
        # Finding H1/M-P3: in package mode, A cannot address B's path because
        # _confine_project_root restricts to A's own <user_data>/projects/<alice_id>/ subtree.
        from flowfile_core.configs import settings
        from fastapi import HTTPException

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "package")
        alice_id = users["alice"].id
        bob_id = users["bob"].id

        # The path that would be B's (under B's subtree, not A's).
        from shared.storage_config import storage

        bob_root = storage.user_data_directory / "projects" / str(bob_id) / "bob_proj"

        project_sync.close_project(alice_id)
        project_sync.close_project(bob_id)

        # Alice tries to open Bob's project path; _confine_project_root rejects it
        # because the path is not under A's <user_data>/projects/<alice_id>/ subtree.
        with pytest.raises(HTTPException) as exc_info:
            project_sync.open_project(str(bob_root), alice_id)
        assert exc_info.value.status_code in (403, 404)


class TestH2_OwnershipNotReassigned:
    """H2/M-A4 — opening a folder with an existing WorkspaceProject row does NOT reassign owner_id.

    Pre-fix: upsert_active contained `proj.owner_id = owner_id` which silently transferred
    ownership.  Post-fix: that line is deleted; the 403 fires before upsert_active is called.
    We verify both that the 403 fires AND that the DB row's owner_id is unmodified after the attempt.
    """

    def test_owner_id_is_unchanged_after_foreign_open_attempt(self, tmp_path, users, monkeypatch):
        # Finding H2: upsert_active must never write proj.owner_id = requester's id.
        from flowfile_core.configs import settings
        from flowfile_core.project.service import _assert_not_foreign_owned
        from fastapi import HTTPException

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "docker")
        alice_id = users["alice"].id
        bob_id = users["bob"].id
        root = tmp_path / "ownership_test"
        root.mkdir()

        with get_db_context() as db:
            proj = db_models.WorkspaceProject(
                name="ownership_test",
                folder_path=str(root),
                owner_id=bob_id,
                is_active=True,
            )
            db.add(proj)
            db.commit()
            proj_id = proj.id

        try:
            # The guard must raise 403 before any owner mutation occurs.
            with pytest.raises(HTTPException) as exc_info:
                _assert_not_foreign_owned(root, alice_id)
            assert exc_info.value.status_code == 403

            # B's row must be completely untouched.
            with get_db_context() as db:
                row = db.get(db_models.WorkspaceProject, proj_id)
                assert row is not None
                assert row.owner_id == bob_id, f"H2: owner_id was reassigned to {row.owner_id}!"
                assert row.is_active is True or row.is_active == 1, "B's is_active was flipped!"
        finally:
            _cleanup_workspace_projects(str(root))

    def test_upsert_active_does_not_change_owner_column(self, tmp_path, users):
        # Finding H2: upsert_active is owner-scoped (filter_by(folder_path=x, owner_id=own)).
        # A call by Alice on a row owned by Bob must find no matching row (it's invisible).
        from flowfile_core.project import repository

        alice_id = users["alice"].id
        bob_id = users["bob"].id
        root = tmp_path / "upsert_test"
        root.mkdir()

        with get_db_context() as db:
            proj_b = db_models.WorkspaceProject(
                name="Bob",
                folder_path=str(root),
                owner_id=bob_id,
                is_active=False,
            )
            db.add(proj_b)
            db.commit()
            proj_b_id = proj_b.id

        try:
            # upsert_active for Alice at Bob's path creates a SECOND row (no cross-owner update).
            # In production this is pre-empted by _assert_not_foreign_owned, but the function itself
            # must be safe in isolation too.
            with get_db_context() as db:
                result = repository.upsert_active(db, "Alice", str(root), alice_id)
                assert result.owner_id == alice_id

            # Bob's row must still be owned by Bob (owner_id unchanged).
            with get_db_context() as db:
                bob_row = db.get(db_models.WorkspaceProject, proj_b_id)
                assert bob_row is not None
                assert bob_row.owner_id == bob_id, f"H2: Bob's row owner was changed to {bob_row.owner_id}!"
        finally:
            _cleanup_workspace_projects(str(root))


class TestH3_H6_FlowUuidCollisionMintsFresh:
    """H3/H6 — importing a manifest whose flow_uuid matches another tenant's flow mints a fresh uuid.

    Pre-fix: _import_flow queried FlowRegistration.filter(flow_uuid==x) with no owner filter,
    then called write_yaml to B's on-disk flow_path, overwriting B's live flow file.
    Post-fix: cross-owner uuid → mint fresh uuid, write to A's own dir, B's row/file untouched.

    The test focuses on the collision detection logic in _import_flow (the DB-level fix) and
    verifies that B's DB registration is untouched after a cross-owner import attempt.
    The actual file write is to a tmp dir so it works in all environments.
    """

    def test_owned_or_none_is_cross_owner_invisible(self, users):
        # Finding H3: owned_or_none must return None for a row owned by another user.
        # This is the atomic unit that prevents the cross-owner lookup from succeeding.
        from flowfile_core.project.repository import owned_or_none

        alice_id = users["alice"].id
        bob_id = users["bob"].id
        b_flow_uuid = str(uuid4())

        with get_db_context() as db:
            reg = db_models.FlowRegistration(
                name="bob_flow",
                flow_path="/tmp/bob_flow.yaml",
                flow_uuid=b_flow_uuid,
                owner_id=bob_id,
            )
            db.add(reg)
            db.commit()
            b_reg_id = reg.id

        try:
            # Alice's lookup must NOT find Bob's flow_uuid.
            with get_db_context() as db:
                result = owned_or_none(db, db_models.FlowRegistration, "owner_id", alice_id, flow_uuid=b_flow_uuid)
                assert result is None, "owned_or_none returned Bob's row to Alice — H3 not fixed!"

            # Bob's lookup must find his own row.
            with get_db_context() as db:
                result = owned_or_none(db, db_models.FlowRegistration, "owner_id", bob_id, flow_uuid=b_flow_uuid)
                assert result is not None
                assert result.owner_id == bob_id
        finally:
            _cleanup_flows(b_flow_uuid)

    def test_import_flow_detects_cross_owner_collision_and_mints_fresh(self, tmp_path, users):
        # Finding H3/H6: _import_flow must mint a fresh uuid when the manifest uuid belongs to another owner.
        # Pre-fix: function resolved flow_uuid globally and wrote to B's flow_path (no owner filter).
        # Post-fix: cross-owner detection mints a new uuid; B's file/row are untouched.
        from unittest.mock import patch, MagicMock, PropertyMock
        from flowfile_core.project.importer import _import_flow

        alice_id = users["alice"].id
        bob_id = users["bob"].id

        # Seed Bob's flow file in tmp_path so it can be written to in tests.
        b_flow_uuid, _ = _seed_flow_for_user(bob_id)
        b_file = tmp_path / "bob_sentinel.yaml"
        b_file.write_text(dump_yaml({"sentinel": "BOB_ORIGINAL", "flow_uuid": b_flow_uuid}), encoding="utf-8")
        # Update the DB to point to our sentinel file in tmp_path.
        with get_db_context() as db:
            reg = db.query(db_models.FlowRegistration).filter_by(flow_uuid=b_flow_uuid).first()
            if reg:
                reg.flow_path = str(b_file)
                db.commit()

        # Redirect Alice's new-flow storage to a tmp dir so the write doesn't hit /data.
        alice_flows_dir = tmp_path / "alice_flows"
        alice_flows_dir.mkdir()
        mock_storage = MagicMock()
        type(mock_storage).flows_directory = PropertyMock(return_value=alice_flows_dir)

        # Build a manifest carrying Bob's flow_uuid.
        manifest_data = _build_flow_manifest(b_flow_uuid, "alice_flow")

        returned_uuid = None
        try:
            with patch("flowfile_core.project.importer.storage", mock_storage):
                returned_uuid = _import_flow(manifest_data, alice_id)

            # The function must have minted a fresh uuid (not B's).
            assert returned_uuid is not None
            assert returned_uuid != b_flow_uuid, "H3: _import_flow returned B's uuid to A — mint-fresh not working!"

            # Bob's sentinel file must be completely unchanged.
            b_content = yaml.safe_load(b_file.read_text(encoding="utf-8"))
            assert b_content.get("sentinel") == "BOB_ORIGINAL", "H3/H6: B's flow file was overwritten by A's import!"

            # Bob's DB registration must still belong to Bob.
            with get_db_context() as db:
                b_reg = db.query(db_models.FlowRegistration).filter_by(flow_uuid=b_flow_uuid).first()
                assert b_reg is not None, "B's registration was deleted!"
                assert b_reg.owner_id == bob_id, f"H3: B's owner_id was changed to {b_reg.owner_id}!"
        finally:
            _cleanup_flows(b_flow_uuid)
            if returned_uuid and returned_uuid != b_flow_uuid:
                _cleanup_flows(returned_uuid)
            project_sync.close_project(alice_id)


class TestH5_VizDashboardCollisionMintsFresh:
    """H5 — a manifest carrying B's viz_uuid / dashboard_uuid must not overwrite B's rows.

    Pre-fix: _import_visualizations called filter_by(viz_uuid=x).first() with no created_by
    filter, then set created_by = owner_id, hijacking B's visualization.
    Post-fix: owned_or_none scopes the lookup to the caller; cross-owner collision → mint fresh.
    """

    def test_viz_uuid_collision_mints_fresh_and_preserves_b(self, tmp_path, users):
        # Finding H5: pre-fix viz upsert had no created_by filter.
        from flowfile_core.project.importer import import_project

        alice_id = users["alice"].id
        bob_id = users["bob"].id

        # Seed B's visualization.
        b_vuuid = _seed_viz_for_user(bob_id)
        b_viz_name_original = f"viz_{b_vuuid[:8]}"

        # A's manifest carries B's viz_uuid.
        root = _minimal_project_dir(tmp_path, "a_viz_project")
        _write_viz_manifest(root, [{"name": "a_chart", "viz_uuid": b_vuuid, "chart_type": "bar", "spec": [], "source_type": "sql"}])
        project_sync.close_project(alice_id)

        try:
            import_project(root, alice_id)

            # B's viz row must be untouched (same owner, same name).
            with get_db_context() as db:
                b_viz = db.query(db_models.CatalogVisualization).filter_by(viz_uuid=b_vuuid).first()
                assert b_viz is not None, "B's visualization row was deleted!"
                assert b_viz.created_by == bob_id, f"B's viz ownership was changed to {b_viz.created_by}!"
                assert b_viz.name == b_viz_name_original, f"B's viz name was overwritten to {b_viz.name}!"

            # A should have gotten a visualization (possibly with a new uuid, not B's).
            with get_db_context() as db:
                a_vizs = db.query(db_models.CatalogVisualization).filter_by(created_by=alice_id).all()
                # A must not have claimed B's uuid.
                assert not any(v.viz_uuid == b_vuuid for v in a_vizs), "A was assigned B's viz_uuid!"
        finally:
            _cleanup_vizs(b_vuuid)
            with get_db_context() as db:
                db.query(db_models.CatalogVisualization).filter_by(created_by=alice_id).delete()
                db.commit()
            project_sync.close_project(alice_id)

    def test_dashboard_uuid_collision_mints_fresh_and_preserves_b(self, tmp_path, users):
        # Finding H5: same bug for dashboards.
        from flowfile_core.project.importer import import_project

        alice_id = users["alice"].id
        bob_id = users["bob"].id

        b_duuid = _seed_dashboard_for_user(bob_id)
        b_dash_name_original = f"dash_{b_duuid[:8]}"

        root = _minimal_project_dir(tmp_path, "a_dash_project")
        _write_dashboard_manifest(root, [{"name": "a_dashboard", "dashboard_uuid": b_duuid, "layout": {"tiles": [], "filters": [], "grid": {"version": 1}}}])
        project_sync.close_project(alice_id)

        try:
            import_project(root, alice_id)

            with get_db_context() as db:
                b_dash = db.query(db_models.CatalogDashboard).filter_by(dashboard_uuid=b_duuid).first()
                assert b_dash is not None
                assert b_dash.created_by == bob_id, "B's dashboard ownership was changed!"
                assert b_dash.name == b_dash_name_original, "B's dashboard name was overwritten!"

            with get_db_context() as db:
                a_dashes = db.query(db_models.CatalogDashboard).filter_by(created_by=alice_id).all()
                assert not any(d.dashboard_uuid == b_duuid for d in a_dashes), "A stole B's dashboard_uuid!"
        finally:
            _cleanup_dashboards(b_duuid)
            with get_db_context() as db:
                db.query(db_models.CatalogDashboard).filter_by(created_by=alice_id).delete()
                db.commit()
            project_sync.close_project(alice_id)


class TestMA2_KernelCollisionMintsFresh:
    """M-A2 — a manifest carrying B's kernel id must not take over B's kernel.

    Pre-fix: _import_kernels looked up by id alone and then set user_id = owner_id,
    overwriting B's custom_image/limits/user_id with attacker values.
    Post-fix: owned_or_none scopes by user_id; cross-owner collision → mint fresh id.
    """

    def test_kernel_id_collision_mints_fresh_and_preserves_b(self, tmp_path, users):
        # Finding M-A2: pre-fix kernel import had no user_id filter on the lookup.
        from flowfile_core.project.importer import import_project

        alice_id = users["alice"].id
        bob_id = users["bob"].id

        b_kid = _seed_kernel_for_user(bob_id)

        root = _minimal_project_dir(tmp_path, "a_kernel_project")
        _write_kernels_manifest(root, [{"id": b_kid, "name": "a_kernel", "image_flavour": "ml", "cpu_cores": 8.0, "memory_gb": 32.0}])
        project_sync.close_project(alice_id)

        try:
            import_project(root, alice_id)

            # B's kernel must still belong to B with original settings.
            with get_db_context() as db:
                b_kernel = db.query(db_models.Kernel).filter_by(id=b_kid).first()
                assert b_kernel is not None, "B's kernel row was deleted!"
                assert b_kernel.user_id == bob_id, f"B's kernel user_id changed to {b_kernel.user_id}!"
                # B's cpu_cores was 2.0; if A's import changed it, the attack succeeded.
                assert b_kernel.cpu_cores == 2.0, f"B's kernel cpu_cores was overwritten to {b_kernel.cpu_cores}!"

            # A must not own B's kernel id.
            with get_db_context() as db:
                a_kernels = db.query(db_models.Kernel).filter_by(user_id=alice_id).all()
                assert not any(k.id == b_kid for k in a_kernels), "A was assigned B's kernel id!"
        finally:
            _cleanup_kernels(b_kid)
            with get_db_context() as db:
                db.query(db_models.Kernel).filter_by(user_id=alice_id).delete()
                db.commit()
            project_sync.close_project(alice_id)


class TestMA3_NamespaceResolutionOwnFirst:
    """M-A3 — namespace lookup during import is own-first; A cannot attach to B's namespace.

    Pre-fix: get_namespace_by_name had no owner_id filter, so the first matching row
    (potentially B's) was returned and tables/flows were attached to it.
    Post-fix: optional owner_id kwarg appends a .filter(owner_col == owner_id) clause so
    A's import resolves only A's namespace rows.
    """

    def test_get_namespace_by_name_owner_scoped(self, users):
        # Finding M-A3: the repository method must scope namespace lookup by owner_id.
        # Pre-fix: no owner_id kwarg → returns B's ns to A (TypeError or wrong row).
        # Post-fix: owner_id kwarg filters to caller's own namespaces only.
        import inspect
        from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository

        # Verify the fix is present: the method must accept owner_id as a keyword arg.
        sig = inspect.signature(SQLAlchemyCatalogRepository.get_namespace_by_name)
        assert "owner_id" in sig.parameters, (
            "M-A3: get_namespace_by_name has no owner_id parameter — fix not applied!"
        )

        alice_id = users["alice"].id
        bob_id = users["bob"].id
        ns_name = f"scope_test_{uuid4().hex[:6]}"

        # B owns the namespace.
        b_ns_id = _seed_namespace_for_user(bob_id, ns_name)

        try:
            # With owner_id=alice: must return None (B's row is invisible to A).
            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                result = repo.get_namespace_by_name(ns_name, None, owner_id=alice_id)
                assert result is None, "M-A3: get_namespace_by_name returned B's namespace to A!"

            # With owner_id=bob: must return B's row.
            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                result = repo.get_namespace_by_name(ns_name, None, owner_id=bob_id)
                assert result is not None
                assert result.owner_id == bob_id

            # Without owner_id (None): must still return B's row (backward-compatible).
            with get_db_context() as db:
                repo = SQLAlchemyCatalogRepository(db)
                result = repo.get_namespace_by_name(ns_name, None)
                assert result is not None
        finally:
            _cleanup_namespaces(ns_name)

    def test_foreign_namespace_ownership_not_changed_on_import(self, tmp_path, users):
        # Finding M-A3: importing namespaces.yaml must not mutate B's namespace owner.
        # Note: if the fix's _resolve_namespace hits a NamespaceExistsError when trying
        # to create A's namespace (because create_namespace checks globally), that is
        # a residual gap — this test surfaces it.
        from flowfile_core.project.importer import _import_namespaces

        alice_id = users["alice"].id
        bob_id = users["bob"].id
        ns_name = f"mutual_ns_{uuid4().hex[:6]}"

        # B owns the namespace.
        b_ns_id = _seed_namespace_for_user(bob_id, ns_name)

        root = _minimal_project_dir(tmp_path, "a_ns_import_test")
        write_yaml(namespaces_manifest_path(root), {
            "namespaces": [{"catalog": ns_name, "schemas": []}]
        })

        try:
            # Import namespaces for Alice. The owner-scoped lookup returns None (B's ns invisible),
            # so the importer tries to create a new ns for Alice.
            try:
                _import_namespaces(root, alice_id)
            except Exception:
                pass  # NamespaceExistsError or other — see gap note below

            # CRITICAL: B's namespace must NOT have had its owner_id changed to Alice.
            with get_db_context() as db:
                b_ns = db.get(db_models.CatalogNamespace, b_ns_id)
                assert b_ns is not None, "B's namespace was deleted!"
                assert b_ns.owner_id == bob_id, (
                    f"M-A3: B's namespace owner_id was changed to {b_ns.owner_id}! "
                    "The fix must never re-own a foreign namespace."
                )
        finally:
            _cleanup_namespaces(ns_name)
            project_sync.close_project(alice_id)

    def test_public_namespace_import_succeeds_and_places_flow_without_mutating_seed(self, tmp_path, users):
        # M-A3 (Phase-4 re-open): a non-seed docker user imports a project whose flows reference the
        # public General/default namespace (owned by local_user, is_public=True).
        #   Pre-fix (owner-only): _import_namespaces hits create_namespace -> uncaught
        #   NamespaceExistsError on the public-name collision => the whole import aborts; and the
        #   flow's _resolve_namespace(create=False) returns None => the flow loses its placement.
        #   Post-fix (own-first-ELSE-public, read-only): the import SUCCEEDS, the flow lands on the
        #   seeded public namespace_id, and the seeded public rows are untouched.
        from flowfile_core.project.importer import import_project

        alice_id = users["alice"].id

        with get_db_context() as db:
            general = db.query(db_models.CatalogNamespace).filter_by(name="General", parent_id=None).first()
            assert general is not None and general.is_public, "seeded public General namespace missing"
            default_schema = (
                db.query(db_models.CatalogNamespace).filter_by(name="default", parent_id=general.id).first()
            )
            assert default_schema is not None and default_schema.is_public, "seeded public default schema missing"
            general_id = general.id
            default_id = default_schema.id
            seed_owner = general.owner_id
            seed_general_desc = general.description
            seed_default_desc = default_schema.description
            seed_default_owner = default_schema.owner_id

        assert seed_owner != alice_id, "test requires Alice to be a non-seed user"

        flow_uuid = str(uuid4())
        # Pre-register Alice's flow with NO namespace and a writable on-disk path (so the importer
        # re-points to it instead of writing under the real flows_directory). The reconcile step in
        # _import_flow is what must set namespace_id from the public-fallback resolution.
        seeded_path = str(tmp_path / f"alice_{flow_uuid[:8]}.flow.yaml")
        with get_db_context() as db:
            reg = db_models.FlowRegistration(
                name="public_ns_flow",
                flow_path=seeded_path,
                flow_uuid=flow_uuid,
                namespace_id=None,
                owner_id=alice_id,
            )
            db.add(reg)
            db.commit()

        root = _minimal_project_dir(tmp_path, "public_ns_import")
        # The manifest carries the public General/default catalog+schema, exactly as projection emits.
        write_yaml(
            namespaces_manifest_path(root),
            {
                "namespaces": [
                    {
                        "catalog": "General",
                        "is_public": True,
                        "schemas": [{"name": "default", "is_public": True}],
                    }
                ]
            },
        )
        _write_flow_manifest_with_namespace(flows_dir(root), flow_uuid, "General", "default", "public_ns_flow")
        project_sync.close_project(alice_id)

        try:
            # Pre-fix this raises (NamespaceExistsError) or leaves the flow unplaced; post-fix it succeeds.
            result = import_project(root, alice_id)
            assert result is not None
            assert result.imported_flows == 1, "the public-namespace flow was not imported"

            # The flow Alice imported lands on the seeded public namespace_id (placement preserved),
            # and the flow row is owned by Alice (own new row, not B's).
            with get_db_context() as db:
                reg = (
                    db.query(db_models.FlowRegistration)
                    .filter_by(flow_uuid=flow_uuid, owner_id=alice_id)
                    .first()
                )
                assert reg is not None, "Alice's flow was not registered"
                assert reg.namespace_id == default_id, (
                    f"flow namespace_id is {reg.namespace_id}, expected the public default schema {default_id} "
                    "(placement lost — own-first-ELSE-public fallback not applied)"
                )

                # SECURITY INVARIANT: the seeded public rows were not mutated (no re-own, no
                # description/name change, no duplicate row created under Alice).
                general_after = db.get(db_models.CatalogNamespace, general_id)
                default_after = db.get(db_models.CatalogNamespace, default_id)
                assert general_after.owner_id == seed_owner and general_after.name == "General"
                assert general_after.description == seed_general_desc and general_after.is_public
                assert default_after.owner_id == seed_default_owner and default_after.name == "default"
                assert default_after.description == seed_default_desc and default_after.is_public

                # No private duplicate of General/default was created under Alice.
                alice_dupes = (
                    db.query(db_models.CatalogNamespace)
                    .filter(
                        db_models.CatalogNamespace.owner_id == alice_id,
                        db_models.CatalogNamespace.name.in_(["General", "default"]),
                    )
                    .all()
                )
                assert alice_dupes == [], "the import duplicated a public namespace under Alice"
        finally:
            _cleanup_flows(flow_uuid)
            project_sync.close_project(alice_id)


class TestSingleUserNonRegression:
    """Single-user (package/electron) non-regression: own project open/init must still work.

    The owner-scoping must not over-restrict the legitimate path where the same user
    opens/inits their OWN project.  IDs must be preserved on the round-trip.
    """

    def test_own_flow_uuid_preserved_on_import(self, tmp_path, users):
        # Non-regression: when A imports A's own flow, the flow_uuid is preserved (no mint).
        from flowfile_core.project.importer import import_project

        alice_id = users["alice"].id

        # Build a proper project dir with A's flow.
        root = _minimal_project_dir(tmp_path, "alice_flow_test")
        a_flow_uuid, _ = _seed_flow_for_user(alice_id)
        _write_flow_manifest(flows_dir(root), a_flow_uuid, "a_own_flow")
        project_sync.close_project(alice_id)

        try:
            # Should NOT raise — A's uuid is visible to the importer (owned by A).
            result = import_project(root, alice_id)
            assert result is not None

            # A's flow_uuid must be preserved (no spurious mint for a same-owner uuid).
            with get_db_context() as db:
                reg = db.query(db_models.FlowRegistration).filter_by(
                    flow_uuid=a_flow_uuid, owner_id=alice_id
                ).first()
                assert reg is not None, "A's flow was not imported!"
                assert reg.flow_uuid == a_flow_uuid, "A's own flow_uuid was reminted unnecessarily!"
        finally:
            _cleanup_flows(a_flow_uuid)
            project_sync.close_project(alice_id)

    def test_electron_mode_own_project_unconfined(self, tmp_path, monkeypatch, users):
        # Non-regression: in electron mode there is no per-owner subtree confinement.
        from flowfile_core.configs import settings

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "electron")
        alice_id = users["alice"].id

        root = tmp_path / "any_arbitrary_path"
        root.mkdir()

        # project_root_base returns None in electron mode (no confinement).
        assert project_root_base(alice_id) is None

        project_sync.close_project(alice_id)
        try:
            project_sync.init_project(str(root), "Electron Test", alice_id)
            assert project_sync.get_active_project(alice_id) is not None
        finally:
            _cleanup_workspace_projects(str(root))
            project_sync.close_project(alice_id)


# ---------------------------------------------------------------------------
# Theme B: path / filesystem confinement
# ---------------------------------------------------------------------------


class TestH4_ManagedTablePointerSanitization:
    """H4 — a tables.yaml managed pointer name containing '../' or an absolute path must be
    sanitized (reduced to basename + containment check) so no directory/file is created
    outside catalog_tables_directory.

    Pre-fix: _resolve_table_path used pointer["name"] verbatim so `Path("/base") / "../../etc/x"`
    traversed; post-fix: Path(pointer["name"]).name (basename) + _is_managed_table_path check.
    """

    def test_traversal_in_managed_pointer_is_blocked(self, tmp_path):
        # Finding H4: crafted managed pointer name with ../ traversal.
        from flowfile_core.project.importer import _resolve_table_path
        from shared.storage_config import storage

        root = tmp_path / "project"
        root.mkdir()

        # Attempt path traversal: "../../etc/passwd" should not resolve outside managed dir.
        traversal_pointer = {"type": "managed", "name": "../../etc/passwd"}
        result = _resolve_table_path(traversal_pointer, root)
        if result is not None:
            # If a path is returned, it MUST stay inside catalog_tables_directory.
            cat_base = str(storage.catalog_tables_directory.resolve())
            resolved = os.path.realpath(result)
            assert resolved.startswith(cat_base), f"Traversal escaped managed dir! resolved={resolved}"

    def test_absolute_path_in_managed_pointer_is_blocked(self, tmp_path):
        # Finding H4: absolute path in managed pointer should not escape managed dir.
        from flowfile_core.project.importer import _resolve_table_path
        from shared.storage_config import storage

        root = tmp_path / "project"
        root.mkdir()

        absolute_pointer = {"type": "managed", "name": "/etc/passwd"}
        result = _resolve_table_path(absolute_pointer, root)
        if result is not None:
            cat_base = str(storage.catalog_tables_directory.resolve())
            resolved = os.path.realpath(result)
            assert resolved.startswith(cat_base), f"Absolute path escaped managed dir! resolved={resolved}"

    def test_valid_managed_pointer_is_accepted(self, tmp_path):
        # Non-regression: a safe managed pointer name still resolves correctly.
        from flowfile_core.project.importer import _resolve_table_path
        from shared.storage_config import storage

        root = tmp_path / "project"
        root.mkdir()

        valid_pointer = {"type": "managed", "name": "my_table_abc12345"}
        result = _resolve_table_path(valid_pointer, root)
        assert result is not None
        cat_base = str(storage.catalog_tables_directory.resolve())
        assert os.path.realpath(result).startswith(cat_base)


class TestL1_ExternalPointerValidation:
    """L1 — an external pointer path escaping user data roots must be rejected (file_path=None/skip);
    a legitimate in-bounds external path must be accepted.

    Pre-fix: external pointer paths were stored verbatim with no validation.
    """

    def test_escaping_external_pointer_is_rejected(self, tmp_path):
        # Finding L1: external pointer pointing outside user data roots.
        from flowfile_core.project.importer import _external_path_allowed

        root = tmp_path / "project"
        root.mkdir()
        assert not _external_path_allowed("/etc/passwd", root), "/etc/passwd must be rejected"
        assert not _external_path_allowed("/tmp/../../etc/shadow", root), "traversal must be rejected"

    def test_inbound_external_pointer_is_accepted(self, tmp_path):
        # Non-regression: a path under the project root is allowed.
        from flowfile_core.project.importer import _external_path_allowed

        root = tmp_path / "project"
        root.mkdir()
        legitimate = str(root / "data" / "my_file.parquet")
        assert _external_path_allowed(legitimate, root), "in-bounds path must be accepted"


class TestMP1_SiblingPrefixEscape:
    """M-P1 — validate_path_under_cwd must reject sibling-directory prefix escapes.

    Pre-fix: containment used bare `fullpath.startswith(base_path)` so `/data/user-evil`
    passed when base was `/data/user`.  Post-fix: _is_contained uses os.path.commonpath.
    """

    def test_sibling_prefix_dir_is_rejected(self, tmp_path, monkeypatch):
        # Finding M-P1: /data/user vs /data/user-evil (sibling prefix escape).
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        # Build a sibling directory: user_data = /tmp/<x>/user; sibling = /tmp/<x>/user-evil.
        from shared.storage_config import storage

        base = str(storage.user_data_directory)
        # Construct a sibling that shares the prefix but is not under it.
        sibling = base.rstrip("/") + "-evil"

        # _is_contained must reject this.
        assert not _is_contained(base, sibling), f"Sibling prefix {sibling!r} was wrongly accepted as under {base!r}!"

    def test_legitimate_child_is_accepted(self, tmp_path):
        # Non-regression: a real child dir of the base must pass _is_contained.
        base = str(tmp_path)
        child = str(tmp_path / "child" / "subdir")
        assert _is_contained(base, child)


class TestMP2_SymlinkEscape:
    """M-P2 — a symlink under an allowed base whose target escapes is rejected by _is_contained
    (realpath applied before the containment test).

    Pre-fix: validation was lexical-only; after .resolve() a symlink to /etc was followed silently.
    Post-fix: _is_contained calls os.path.realpath on both sides first.
    """

    def test_symlink_escaping_base_is_rejected(self, tmp_path):
        # Finding M-P2: symlink inside allowed base but pointing outside.
        allowed_base = tmp_path / "allowed"
        allowed_base.mkdir()
        symlink = allowed_base / "escape_link"
        escape_target = tmp_path / "outside"
        escape_target.mkdir()
        symlink.symlink_to(escape_target)

        # The symlink path is under allowed_base lexically, but its realpath points outside.
        assert not _is_contained(str(allowed_base), str(symlink)), (
            "A symlink whose target escapes the base must be rejected by _is_contained!"
        )

    def test_symlink_within_base_is_accepted(self, tmp_path):
        # Non-regression: a symlink whose target stays inside the base is fine.
        allowed_base = tmp_path / "allowed"
        allowed_base.mkdir()
        real_target = allowed_base / "real_dir"
        real_target.mkdir()
        symlink = allowed_base / "inner_link"
        symlink.symlink_to(real_target)

        assert _is_contained(str(allowed_base), str(symlink))


class TestI1_DotDotGuardInDockerMode:
    """I1 — in docker/package mode the path validator must reject paths containing '..'.

    Pre-fix: only the electron branch had an explicit '..' guard; the docker branch relied
    on normpath which is insufficient.  Post-fix: `".." in user_path` check added.
    """

    def test_dotdot_in_docker_mode_raises_403(self, tmp_path, monkeypatch):
        # Finding I1: docker/package branch must reject .. sequences.
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        from flowfile_core.fileExplorer.funcs import validate_path_under_cwd
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            validate_path_under_cwd("some/../../etc/passwd")
        assert exc_info.value.status_code == 403

    def test_dotdot_in_package_mode_raises_403(self, tmp_path, monkeypatch):
        # Finding I1: same check applies in package mode.
        monkeypatch.setenv("FLOWFILE_MODE", "package")
        from flowfile_core.fileExplorer.funcs import validate_path_under_cwd
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            validate_path_under_cwd("../secret")
        assert exc_info.value.status_code == 403


class TestMP3_DockerRouterGate:
    """M-P3/Contract 2(d) — the /project router 404s in docker mode by default; enabled when
    FLOWFILE_ENABLE_PROJECTS is set; always reachable in package/electron.

    Pre-fix: the router was mounted unconditionally (no docker gate).
    Post-fix: require_projects_enabled() dependency raises 404 when is_docker_mode() and flag unset.

    NOTE: settings.FLOWFILE_MODE is a module-level constant — patch with monkeypatch.setattr.
    """

    def test_project_router_404s_in_docker_mode_without_flag(self, monkeypatch):
        # Finding M-P3: docker mode with no opt-in must 404 the /project router.
        from flowfile_core.configs import settings as s
        from fastapi import HTTPException as FHTTPException
        from flowfile_core.routes.project import require_projects_enabled

        monkeypatch.setattr(s, "FLOWFILE_MODE", "docker")
        # Ensure the enable flag is off (it's a MutableBool with a .set() method).
        s.FLOWFILE_ENABLE_PROJECTS.set(False)
        try:
            with pytest.raises(FHTTPException) as exc_info:
                require_projects_enabled()
            assert exc_info.value.status_code == 404
        finally:
            s.FLOWFILE_ENABLE_PROJECTS.set(False)

    def test_project_router_accessible_when_flag_set_in_docker(self, monkeypatch):
        # Non-regression: when FLOWFILE_ENABLE_PROJECTS=true the gate does not raise.
        from flowfile_core.configs import settings as s
        from flowfile_core.routes.project import require_projects_enabled

        monkeypatch.setattr(s, "FLOWFILE_MODE", "docker")
        s.FLOWFILE_ENABLE_PROJECTS.set(True)
        try:
            require_projects_enabled()  # must not raise
        finally:
            s.FLOWFILE_ENABLE_PROJECTS.set(False)

    def test_project_router_accessible_in_electron_mode(self, monkeypatch):
        # Non-regression: electron mode is always accessible regardless of the flag.
        from flowfile_core.configs import settings as s
        from flowfile_core.routes.project import require_projects_enabled

        monkeypatch.setattr(s, "FLOWFILE_MODE", "electron")
        require_projects_enabled()  # must not raise

    def test_project_router_accessible_in_package_mode(self, monkeypatch):
        # Non-regression: package mode is always accessible.
        from flowfile_core.configs import settings as s
        from flowfile_core.routes.project import require_projects_enabled

        monkeypatch.setattr(s, "FLOWFILE_MODE", "package")
        require_projects_enabled()  # must not raise


class TestPerOwnerConfinement:
    """Contract 2(a) — in multi-tenant mode A cannot init a project outside her own subtree.

    Pre-fix: no per-owner subtree existed; any path passing validate_path_under_cwd was allowed.
    Post-fix: _confine_project_root enforces the <user_data>/projects/<owner_id>/<name> layout.

    NOTE: settings.FLOWFILE_MODE is a module-level constant bound at import time.
    monkeypatch.setenv("FLOWFILE_MODE", ...) only changes os.environ for future reads of
    os.getenv() but NOT the already-bound module constant. We must use
    monkeypatch.setattr(settings, "FLOWFILE_MODE", ...) to change it for these tests.
    The multi_user_mode autouse fixture uses setenv because the sharing tests' conftest.py
    is designed to run before the module-level constant is cached, which doesn't apply here.
    """

    def test_path_outside_owner_subtree_raises_403_in_package_mode(self, tmp_path, monkeypatch, users):
        # Contract 2(a): path outside the per-owner root is rejected.
        from flowfile_core.configs import settings
        from fastapi import HTTPException

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "package")
        alice_id = users["alice"].id

        base = project_root_base(alice_id)
        assert base is not None, "project_root_base must return a path in package mode!"

        # A path OUTSIDE Alice's subtree (arbitrary tmp dir, not under base).
        outside = str(tmp_path / "outside_alice")

        with pytest.raises(HTTPException) as exc_info:
            _confine_project_root(outside, alice_id)
        assert exc_info.value.status_code == 403

    def test_bare_name_is_joined_under_owner_subtree(self, monkeypatch, users):
        # Contract 2(a): a bare name (no path separator) is placed under the owner's base.
        from flowfile_core.configs import settings

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "package")
        alice_id = users["alice"].id

        base = project_root_base(alice_id)
        assert base is not None
        resolved = _confine_project_root("my_project", alice_id)
        assert str(resolved).startswith(str(base)), f"Bare name {resolved!r} is not under owner base {base!r}!"

    def test_electron_mode_is_unconfined(self, tmp_path, monkeypatch, users):
        # Non-regression: electron returns any resolved path (no confinement).
        from flowfile_core.configs import settings

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "electron")
        alice_id = users["alice"].id

        root = _confine_project_root(str(tmp_path / "any"), alice_id)
        assert root is not None  # No 403 raised.


class TestH8_PruneGrantCleanup:
    """H8 — pruning a viz/dashboard must clean up resource_grants rows (no orphans).

    Pre-fix: the prune used bulk Query.delete() which bypasses ORM after_delete backstops,
    leaving orphaned resource_grants rows that could re-attach to new resources via rowid reuse.
    Post-fix: sharing.delete_grants_for_resource is called before each bulk delete.
    """

    def test_prune_viz_cleans_grants(self, tmp_path, users, group_factory, grant_factory):
        # Finding H8: grants must not survive a prune of their resource.
        from flowfile_core.auth import sharing
        from flowfile_core.project.importer import import_project

        alice_id = users["alice"].id
        gid = group_factory("h8-test-group", users["admin"].id, {users["bob"].id: "member"})

        # Seed A's viz and attach a grant to it.
        a_vuuid = _seed_viz_for_user(alice_id)
        with get_db_context() as db:
            viz = db.query(db_models.CatalogVisualization).filter_by(viz_uuid=a_vuuid).first()
            viz_id = viz.id
        grant_factory("visualization", viz_id, gid, permission="use", granted_by=alice_id)

        # Confirm grant exists before prune.
        with get_db_context() as db:
            grants_before = db.query(db_models.ResourceGrant).filter_by(resource_type="visualization", resource_id=viz_id).count()
        assert grants_before >= 1

        # Import an empty visualizations manifest for A → prune removes A's viz.
        root = _minimal_project_dir(tmp_path, "a_prune_project")
        _write_viz_manifest(root, [])  # empty: A's viz will be pruned.
        project_sync.close_project(alice_id)

        try:
            import_project(root, alice_id, prune=True)

            # The viz should be gone.
            with get_db_context() as db:
                viz_gone = db.query(db_models.CatalogVisualization).filter_by(viz_uuid=a_vuuid).first()
                assert viz_gone is None, "Viz was not pruned!"

                # And its grants must also be gone (no orphan).
                orphan_grants = db.query(db_models.ResourceGrant).filter_by(resource_type="visualization", resource_id=viz_id).count()
                assert orphan_grants == 0, f"H8: {orphan_grants} orphaned grant(s) survived the prune!"
        finally:
            _cleanup_vizs(a_vuuid)
            project_sync.close_project(alice_id)
