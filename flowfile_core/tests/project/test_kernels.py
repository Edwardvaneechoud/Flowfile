"""Kernel-definition projection: determinism, import round-trip, prune, hook isolation.

Kernels round-trip their *definition* only (flavour, requested packages, resource limits) keyed
by the stable kernel id. Bake-time detail (resolved_packages, the resolved image tag) is never
projected, and the container is never started on import — these tests stay Docker-free by writing
Kernel rows directly and driving the project_sync hooks, exactly as _persist_kernel would.
"""

import json
from pathlib import Path
from uuid import uuid4

import yaml

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import Kernel
from flowfile_core.project import project_sync, projection

OWNER = 1


def _make_kernel(
    name: str,
    *,
    packages: list[str] | None = None,
    flavour: str = "base",
    cpu: float = 2.0,
    mem: float = 4.0,
    gpu: bool = False,
    custom_image: str | None = None,
    resolved: list[dict] | None = None,
) -> str:
    """Insert a Kernel config row directly (the DB effect _persist_kernel produces). Returns its id."""
    kernel_id = uuid4().hex
    with get_db_context() as db:
        db.add(
            Kernel(
                id=kernel_id,
                name=name,
                user_id=OWNER,
                packages=json.dumps(packages or []),
                resolved_packages=json.dumps(resolved or []),
                cpu_cores=cpu,
                memory_gb=mem,
                gpu=gpu,
                image_flavour=flavour,
                custom_image=custom_image,
            )
        )
        db.commit()
    return kernel_id


def _kernels_yaml(root: Path) -> dict:
    data = yaml.safe_load((root / "kernels.yaml").read_text(encoding="utf-8"))
    return {k["id"]: k for k in data["kernels"]}


def _kernel_row(kernel_id: str) -> Kernel | None:
    with get_db_context() as db:
        return db.query(Kernel).filter_by(id=kernel_id).first()


def _delete_all_kernels() -> None:
    with get_db_context() as db:
        db.query(Kernel).filter_by(user_id=OWNER).delete()
        db.commit()


def test_kernel_projection_deterministic_and_bake_free(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _delete_all_kernels()
    kid = _make_kernel(
        "ml-kernel",
        packages=["xgboost", "shap"],
        flavour="ml",
        cpu=4.0,
        mem=8.0,
        resolved=[{"name": "xgboost", "version": "2.1.1"}],
        custom_image=None,
    )
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Kernel Det", OWNER)
        entry = _kernels_yaml(root)[kid]
        assert entry["name"] == "ml-kernel" and entry["image_flavour"] == "ml"
        assert entry["packages"] == ["shap", "xgboost"]  # sorted for determinism
        assert entry["cpu_cores"] == 4.0 and entry["memory_gb"] == 8.0 and entry["gpu"] is False
        # Bake-time/host-specific detail must never be projected.
        assert "resolved_packages" not in entry and "image" not in entry
        assert "custom_image" not in entry  # omitted when unset

        kernels_bytes = (root / "kernels.yaml").read_bytes()
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert (root / "kernels.yaml").read_bytes() == kernels_bytes, "re-projection must be byte-identical"

        # project -> import -> project must round-trip byte-identically.
        import_project(root, OWNER)
        with get_db_context() as db:
            projection.project_all(db, root, OWNER)
        assert (root / "kernels.yaml").read_bytes() == kernels_bytes, "project->import->project must round-trip"
    finally:
        _delete_all_kernels()
        project_sync.close_project(OWNER)


def test_kernel_round_trips_and_upserts_by_id(tmp_path):
    from flowfile_core.project.importer import import_project

    project_sync.close_project(OWNER)
    _delete_all_kernels()
    kid = _make_kernel("analytics", packages=["polars-ds"], flavour="base", cpu=1.0, mem=2.0)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Kernel RT", OWNER)
        assert kid in _kernels_yaml(root)

        # Drop the row, then rebuild from files twice: upsert by id must not duplicate.
        project_sync.close_project(OWNER)
        _delete_all_kernels()
        import_project(root, OWNER)
        import_project(root, OWNER)
        with get_db_context() as db:
            rows = db.query(Kernel).filter_by(user_id=OWNER).all()
            assert len(rows) == 1
            row = rows[0]
            assert row.id == kid and row.name == "analytics"
            assert json.loads(row.packages) == ["polars-ds"]
            assert row.image_flavour == "base" and row.cpu_cores == 1.0 and row.memory_gb == 2.0
    finally:
        _delete_all_kernels()
        project_sync.close_project(OWNER)


def test_kernel_prune_removes_absent_kernel(tmp_path):
    from flowfile_core.project.normalize import write_yaml

    project_sync.close_project(OWNER)
    _delete_all_kernels()
    keep = _make_kernel("keep-kernel", flavour="base")
    drop = _make_kernel("drop-kernel", flavour="ml")
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Kernel Prune", OWNER)
        # Rewrite the manifest keeping only the survivor (preserving its exact projected entry).
        manifest = _kernels_yaml(root)
        write_yaml(root / "kernels.yaml", {"kernels": [manifest[keep]]})
        project_sync.reload_from_disk(OWNER)

        assert _kernel_row(drop) is None, "kernel absent from the manifest must be pruned"
        survivor = _kernel_row(keep)
        assert survivor is not None and survivor.name == "keep-kernel"
    finally:
        _delete_all_kernels()
        project_sync.close_project(OWNER)


def test_kernel_hook_and_failure_isolation(tmp_path, monkeypatch):
    project_sync.close_project(OWNER)
    _delete_all_kernels()
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Kernel Hook", OWNER)
        assert _kernels_yaml(root) == {}

        # Create hook: a new row + kernels_changed adds the manifest entry.
        kid = _make_kernel("hooked", packages=["numpy"])
        project_sync.kernels_changed(OWNER)
        assert kid in _kernels_yaml(root)

        # Delete hook: drop the row + kernels_changed removes it (manifest regenerated wholesale).
        _delete_all_kernels()
        project_sync.kernels_changed(OWNER)
        assert _kernels_yaml(root) == {}

        # A projection failure must never escape the hook.
        def _boom(*_args, **_kwargs):
            raise RuntimeError("projection boom")

        monkeypatch.setattr(projection, "regenerate_kernels_manifest", _boom)
        project_sync.kernels_changed(OWNER)  # must not raise
    finally:
        _delete_all_kernels()
        project_sync.close_project(OWNER)


def test_kernels_changed_is_noop_without_active_project():
    project_sync.close_project(OWNER)
    _delete_all_kernels()
    _make_kernel("orphan-kernel")
    try:
        project_sync.kernels_changed(OWNER)  # no active project -> silent no-op, no raise
    finally:
        _delete_all_kernels()
