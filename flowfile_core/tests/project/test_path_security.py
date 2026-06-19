"""Path / filesystem confinement regression tests for the Phase-2 security fixes.

Theme B findings: H4 (managed table pointer traversal), L1 (external pointer validation),
M-P1 (sibling prefix escape in validate_path_under_cwd), M-P2 (symlink escape via realpath),
I1 (missing '..' guard in docker branch), M-P3 (router docker gate + per-owner subtree).

These tests run in single-user (electron) mode by default (no `multi_user_mode` autouse) so
they are hermetic and do not require the multi-user sharing fixture. Mode-specific tests
monkeypatch FLOWFILE_MODE internally.

Each test has a one-line comment naming the finding and the pre-fix vulnerable behavior.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from flowfile_core.fileExplorer.funcs import _is_contained, validate_path_under_cwd
from flowfile_core.project.normalize import write_yaml, dump_yaml
from flowfile_core.project.manifest import (
    ProjectManifest,
    tables_manifest_path,
    write_manifest,
)


# ---------------------------------------------------------------------------
# _is_contained helpers
# ---------------------------------------------------------------------------


class TestIsContainedHelper:
    """Unit tests for the _is_contained helper that replaced bare startswith."""

    def test_child_dir_is_contained(self, tmp_path):
        # Non-regression: a real child must be contained.
        base = str(tmp_path)
        child = str(tmp_path / "a" / "b")
        assert _is_contained(base, child)

    def test_exact_base_is_contained(self, tmp_path):
        # Non-regression: the base itself is contained in itself.
        base = str(tmp_path)
        assert _is_contained(base, base)

    def test_sibling_prefix_is_rejected(self, tmp_path):
        # Finding M-P1: pre-fix startswith let /data/user-evil pass when base was /data/user.
        parent = tmp_path / "data"
        parent.mkdir()
        base = parent / "user"
        sibling = parent / "user-evil"
        parent.mkdir(parents=True, exist_ok=True)
        assert not _is_contained(str(base), str(sibling)), (
            "Sibling-prefix /data/user-evil must not be considered inside /data/user!"
        )

    def test_sibling_with_extra_digit_is_rejected(self, tmp_path):
        # Finding M-P1: /data/user2 must not pass when base is /data/user.
        parent = tmp_path / "data"
        parent.mkdir()
        base_str = str(parent / "user")
        sibling_str = str(parent / "user2")
        assert not _is_contained(base_str, sibling_str)

    def test_symlink_escaping_base_is_rejected(self, tmp_path):
        # Finding M-P2: symlink under allowed_base but pointing outside must be rejected.
        allowed = tmp_path / "allowed"
        allowed.mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        link = allowed / "evil_link"
        link.symlink_to(outside)
        assert not _is_contained(str(allowed), str(link)), (
            "A symlink whose realpath escapes the base must be rejected!"
        )

    def test_symlink_within_base_is_accepted(self, tmp_path):
        # Non-regression: a symlink whose target stays inside is fine.
        allowed = tmp_path / "allowed"
        allowed.mkdir()
        target = allowed / "real"
        target.mkdir()
        link = allowed / "good_link"
        link.symlink_to(target)
        assert _is_contained(str(allowed), str(link))

    def test_absolute_path_outside_base_is_rejected(self, tmp_path):
        # Non-regression: /etc is not inside /tmp/<x>.
        assert not _is_contained(str(tmp_path), "/etc")

    def test_path_with_separator_only_match(self, tmp_path):
        # Strict boundary: /data/user must not match /data/user2.
        base = str(tmp_path / "user")
        candidate = str(tmp_path / "user2")
        assert not _is_contained(base, candidate)


# ---------------------------------------------------------------------------
# validate_path_under_cwd (M-P1, M-P2, I1)
# ---------------------------------------------------------------------------


class TestValidatePathDockerMode:
    """validate_path_under_cwd in docker/package mode: sibling prefix, symlink, and '..' checks."""

    def test_dotdot_in_docker_mode_raises_403(self, monkeypatch):
        # Finding I1: docker branch had no explicit '..' guard pre-fix; post-fix rejects it.
        from flowfile_core.configs import settings
        from fastapi import HTTPException

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "docker")
        with pytest.raises(HTTPException) as exc_info:
            validate_path_under_cwd("a/../../etc/passwd")
        assert exc_info.value.status_code == 403

    def test_dotdot_in_package_mode_raises_403(self, monkeypatch):
        # Finding I1: same guard applies in package mode.
        from flowfile_core.configs import settings
        from fastapi import HTTPException

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "package")
        with pytest.raises(HTTPException) as exc_info:
            validate_path_under_cwd("../secret")
        assert exc_info.value.status_code == 403

    def test_sibling_prefix_in_docker_mode_raises_403(self, tmp_path, monkeypatch):
        # Finding M-P1: sibling-prefix path must be rejected in docker mode.
        # Patch the module-level constant (monkeypatch.setenv only changes os.environ,
        # but settings.FLOWFILE_MODE is bound at import time).
        from flowfile_core.configs import settings
        from shared.storage_config import storage
        from fastapi import HTTPException

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "docker")
        # Construct a sibling of the user_data_directory.
        base = str(storage.user_data_directory).rstrip("/")
        sibling = base + "-evil/some/path"
        # This path is not under any allowed base; _is_contained with commonpath rejects it.
        assert not _is_contained(base, sibling), "Sanity: sibling must not be contained in base"
        # validate_path_under_cwd must 403 because sibling is not under cwd/base_dir/user_data.
        with pytest.raises(HTTPException) as exc_info:
            validate_path_under_cwd(sibling)
        assert exc_info.value.status_code == 403

    def test_allowed_path_in_docker_mode_succeeds(self, tmp_path, monkeypatch):
        # Non-regression: a path under an allowed base is accepted.
        from flowfile_core.configs import settings
        from shared.storage_config import storage

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "docker")
        allowed_path = str(storage.user_data_directory / "some" / "subdir")
        # Should not raise; returns the validated path.
        result = validate_path_under_cwd(allowed_path)
        assert result is not None


class TestValidatePathElectronMode:
    """validate_path_under_cwd in electron mode."""

    def test_dotdot_in_electron_mode_raises_403(self, monkeypatch):
        # Non-regression: electron mode blocks '..' traversal too (added in the same fix).
        from flowfile_core.configs import settings
        from fastapi import HTTPException

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "electron")
        with pytest.raises(HTTPException) as exc_info:
            validate_path_under_cwd("a/../../../etc/passwd")
        assert exc_info.value.status_code == 403

    def test_absolute_path_in_electron_mode_succeeds(self, tmp_path, monkeypatch):
        # Non-regression: electron allows any absolute path (not traversal).
        from flowfile_core.configs import settings

        monkeypatch.setattr(settings, "FLOWFILE_MODE", "electron")
        result = validate_path_under_cwd(str(tmp_path))
        assert result is not None


# ---------------------------------------------------------------------------
# H4: managed table pointer sanitization
# ---------------------------------------------------------------------------


class TestH4ManagedPointerSanitization:
    """H4 — tables.yaml managed pointer name with traversal/absolute path must not escape."""

    def test_traversal_name_reduced_to_basename(self, tmp_path):
        # Finding H4: pre-fix used pointer["name"] verbatim; post-fix uses Path(name).name (basename).
        from flowfile_core.project.importer import _resolve_table_path
        from shared.storage_config import storage

        root = tmp_path / "project"
        root.mkdir()

        for crafted in ("../../etc/passwd", "../sibling/secret", "subdir/table"):
            pointer = {"type": "managed", "name": crafted}
            result = _resolve_table_path(pointer, root)
            if result is not None:
                resolved = os.path.realpath(result)
                cat_dir = os.path.realpath(str(storage.catalog_tables_directory))
                assert resolved.startswith(cat_dir), (
                    f"Crafted name {crafted!r} escaped managed dir! resolved={resolved!r}"
                )

    def test_absolute_path_as_managed_name_blocked(self, tmp_path):
        # Finding H4: Path("/base") / "/abs/path" discards the base — basename reduces this to "path".
        from flowfile_core.project.importer import _resolve_table_path
        from shared.storage_config import storage

        root = tmp_path / "project"
        root.mkdir()

        pointer = {"type": "managed", "name": "/etc/sensitive_data"}
        result = _resolve_table_path(pointer, root)
        if result is not None:
            resolved = os.path.realpath(result)
            cat_dir = os.path.realpath(str(storage.catalog_tables_directory))
            assert resolved.startswith(cat_dir), (
                f"Absolute name escaped managed dir! resolved={resolved!r}"
            )

    def test_simple_managed_name_resolves_under_catalog_dir(self, tmp_path):
        # Non-regression: a clean managed table name resolves correctly.
        from flowfile_core.project.importer import _resolve_table_path
        from shared.storage_config import storage

        root = tmp_path / "project"
        root.mkdir()

        pointer = {"type": "managed", "name": "sales_data_abc12345"}
        result = _resolve_table_path(pointer, root)
        assert result is not None
        cat_dir = os.path.realpath(str(storage.catalog_tables_directory))
        assert os.path.realpath(result).startswith(cat_dir)

    def test_import_with_traversal_table_creates_nothing_outside_managed_dir(self, tmp_path):
        # Finding H4: full import path — crafted tables.yaml must not create dirs outside managed.
        from flowfile_core.project.importer import import_project
        from shared.storage_config import storage

        root = tmp_path / "project"
        root.mkdir()
        manifest = ProjectManifest(name="h4_test", project_id=str(uuid4()), created_with_version="0.11.0")
        write_manifest(root, manifest)

        escape_target = str(tmp_path / "escaped_write")
        # Craft a tables.yaml with a managed pointer whose name would traverse to escape_target.
        # The actual traversal vector: pointer["name"] = "../../<something>" from catalog_tables_dir.
        cat_dir = storage.catalog_tables_directory
        # Compute relative path from cat_dir to escape_target.
        try:
            rel = os.path.relpath(escape_target, str(cat_dir))
        except ValueError:
            rel = "../../escape_test"

        write_yaml(tables_manifest_path(root), {
            "tables": [{
                "name": "evil_table",
                "table_type": "physical",
                "pointer": {"type": "managed", "name": rel},
                "schema": [{"name": "id", "dtype": "Int64"}],
                "storage_format": "delta",
            }]
        })

        import_project(root, 1)
        # The escape target must NOT have been created by the import.
        assert not os.path.exists(escape_target), (
            f"H4: crafted managed pointer created {escape_target!r}!"
        )


# ---------------------------------------------------------------------------
# L1: external pointer validation
# ---------------------------------------------------------------------------


class TestL1ExternalPointerValidation:
    """L1 — external pointer paths escaping allowed roots must be skipped (not stored)."""

    def test_escaping_external_path_is_rejected(self, tmp_path):
        # Finding L1: pre-fix stored external paths verbatim; post-fix validates.
        from flowfile_core.project.importer import _external_path_allowed

        root = tmp_path / "project"
        root.mkdir()

        for bad in ("/etc/passwd", "/root/.bashrc", "/../secret", "/tmp/../../etc"):
            assert not _external_path_allowed(bad, root), f"External path {bad!r} should be rejected"

    def test_project_root_child_is_accepted(self, tmp_path):
        # Non-regression: a path inside the project root is allowed.
        from flowfile_core.project.importer import _external_path_allowed

        root = tmp_path / "project"
        root.mkdir()
        child = str(root / "data" / "file.parquet")
        assert _external_path_allowed(child, root), "Child of project root must be accepted"

    def test_traversal_in_external_path_is_rejected(self, tmp_path):
        # Finding L1: traversal in external path.
        from flowfile_core.project.importer import _external_path_allowed

        root = tmp_path / "project"
        root.mkdir()
        traversal = str(root / ".." / ".." / "etc" / "passwd")
        assert not _external_path_allowed(traversal, root), "Traversal external path must be rejected"


# ---------------------------------------------------------------------------
# Projection → import → projection round-trip with path-security checks
# ---------------------------------------------------------------------------


class TestRoundTripWithPathSecurity:
    """Round-trip regression: projection→import→projection stays byte-identical after path-security fixes."""

    def test_clean_project_round_trips_despite_path_security(self, tmp_path, monkeypatch):
        # Invariant: the path-security changes must not break a legitimate single-user round-trip.
        from flowfile_core.configs import settings
        from flowfile_core.project import project_sync, projection
        from flowfile_core.database.connection import get_db_context
        from flowfile_core.project.importer import import_project

        # Use electron mode (single-user, no confinement).
        monkeypatch.setattr(settings, "FLOWFILE_MODE", "electron")
        owner_id = 1

        root = tmp_path / "clean_project"
        project_sync.close_project(owner_id)

        try:
            project_sync.init_project(str(root), "Round Trip", owner_id)
            snap1: dict[str, bytes] = {}
            for p in sorted(root.rglob("*")):
                if p.is_file() and not p.name.startswith("."):
                    snap1[str(p.relative_to(root))] = p.read_bytes()

            with get_db_context() as db:
                projection.project_all(db, root, owner_id)

            snap2: dict[str, bytes] = {}
            for p in sorted(root.rglob("*")):
                if p.is_file() and not p.name.startswith("."):
                    snap2[str(p.relative_to(root))] = p.read_bytes()

            assert snap1 == snap2, "Re-projecting an unchanged project must be byte-identical!"

            import_project(root, owner_id)
            with get_db_context() as db:
                projection.project_all(db, root, owner_id)

            snap3: dict[str, bytes] = {}
            for p in sorted(root.rglob("*")):
                if p.is_file() and not p.name.startswith("."):
                    snap3[str(p.relative_to(root))] = p.read_bytes()

            assert snap1 == snap3, "project→import→project must round-trip byte-identically!"
        finally:
            project_sync.close_project(owner_id)
