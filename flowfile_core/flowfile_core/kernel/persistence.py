"""Database persistence for kernel configurations.

Kernels are persisted so they survive core process restarts. Only the
configuration is stored (id, name, packages, resource limits, user ownership).
Runtime state (container_id, port, state) is ephemeral and reconstructed at
startup by reclaiming running Docker containers.
"""

import json
import logging

from sqlalchemy.orm import Session

from flowfile_core.database import models as db_models
from flowfile_core.kernel.models import (
    ImageFlavour,
    KernelConfig,
    KernelInfo,
    ResolvedPackage,
)

logger = logging.getLogger(__name__)


def _resolved_packages_to_json(resolved: list[ResolvedPackage]) -> str:
    return json.dumps([{"name": p.name, "version": p.version} for p in resolved])


def save_kernel(db: Session, kernel: KernelInfo, user_id: int) -> None:
    """Insert or update a kernel record in the database."""
    existing = db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel.id).first()
    if existing:
        existing.name = kernel.name
        existing.packages = json.dumps(kernel.packages)
        existing.resolved_packages = _resolved_packages_to_json(kernel.resolved_packages)
        existing.cpu_cores = kernel.cpu_cores
        existing.memory_gb = kernel.memory_gb
        existing.gpu = kernel.gpu
        existing.image_flavour = kernel.image_flavour.value
        existing.custom_image = kernel.custom_image
        existing.user_id = user_id
    else:
        record = db_models.Kernel(
            id=kernel.id,
            name=kernel.name,
            user_id=user_id,
            packages=json.dumps(kernel.packages),
            resolved_packages=_resolved_packages_to_json(kernel.resolved_packages),
            cpu_cores=kernel.cpu_cores,
            memory_gb=kernel.memory_gb,
            gpu=kernel.gpu,
            image_flavour=kernel.image_flavour.value,
            custom_image=kernel.custom_image,
        )
        db.add(record)
    db.commit()


def delete_kernel(db: Session, kernel_id: str) -> None:
    """Remove a kernel record from the database."""
    db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).delete()
    db.commit()


def kernel_token_identity(db: Session, kernel_id: str, token_id: str | None = None) -> tuple[int, str, str] | None:
    """What a token of a kernel names: ``(owner id, kernel id, token id)``.

    Without ``token_id`` it is the token of the kernel's current run, ``None`` when it is in none. With one it is the
    token of a run the kernel has not begun; ``begin_kernel_run`` begins it. ``None`` when the kernel has no row.
    Nothing is written.
    """
    row = db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).first()
    if row is None:
        return None
    token_id = token_id or row.token_id
    return (row.user_id, row.id, token_id) if token_id else None


def begin_kernel_run(db: Session, kernel_id: str, token_id: str, *, keep_current: bool = False) -> None:
    """Make ``token_id`` the kernel's, which ends the token of the run before it.

    With ``keep_current`` a kernel that is in a run keeps it, decided in the one statement that writes, so a run
    another core process began meanwhile is never replaced.
    """
    rows = db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id)
    if keep_current:
        rows = rows.filter(db_models.Kernel.token_id.is_(None))
    rows.update({"token_id": token_id})
    db.commit()


def kernel_run_ids(db: Session) -> dict[str, str]:
    """Kernel id to the token id of its run, for every kernel that is in one."""
    rows = db.query(db_models.Kernel.id, db_models.Kernel.token_id).filter(db_models.Kernel.token_id.isnot(None))
    return {kernel_id: token_id for kernel_id, token_id in rows}


def clear_kernel_token(db: Session, kernel_id: str, token_id: str | None = None) -> None:
    """End the token of a kernel's run; a kernel with no row has none.

    Given ``token_id``, only while that run is still the kernel's: a run begun since is left alone.
    """
    rows = db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id)
    if token_id is not None:
        rows = rows.filter(db_models.Kernel.token_id == token_id)
    rows.update({"token_id": None})
    db.commit()


def set_kernel_scratch_flow_id(db: Session, kernel_id: str, scratch_flow_id: int | None) -> None:
    """Attach a scratch FlowRegistration id to a kernel row.

    Used by the kernel manager after auto-creating the scratch flow during
    ``create_kernel``. Passing ``None`` clears the column (used by the lazy
    upgrade path if the scratch row was deleted out-of-band).
    """
    row = db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).first()
    if row is None:
        logger.warning(
            "set_kernel_scratch_flow_id: kernel '%s' row missing; "
            "scratch_flow_id=%s not persisted",
            kernel_id,
            scratch_flow_id,
        )
        return
    row.scratch_flow_registration_id = scratch_flow_id
    db.commit()


def get_kernel_scratch_flow_id(db: Session, kernel_id: str) -> int | None:
    """Return the persisted scratch FlowRegistration id, or ``None`` if unset."""
    row = db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).first()
    if row is None:
        return None
    return getattr(row, "scratch_flow_registration_id", None)


def get_all_kernel_scratch_ids(db: Session) -> dict[str, int | None]:
    """Bulk-load every kernel's scratch FlowRegistration id.

    Called by ``KernelManager._restore_kernels_from_db`` so the in-memory
    ``_scratch_flow_ids`` mapping stays in sync with the DB on startup.
    """
    rows = db.query(db_models.Kernel).all()
    return {
        row.id: getattr(row, "scratch_flow_registration_id", None) for row in rows
    }


def get_kernels_for_user(db: Session, user_id: int) -> list[KernelConfig]:
    """Return all persisted kernel configs belonging to a user."""
    rows = db.query(db_models.Kernel).filter(db_models.Kernel.user_id == user_id).all()
    return [_row_to_config(row) for row in rows]


def get_all_kernels(db: Session) -> list[tuple[KernelConfig, list[ResolvedPackage], int]]:
    """Return all persisted kernels as ``(config, resolved_packages, user_id)`` tuples."""
    rows = db.query(db_models.Kernel).all()
    return [
        (_row_to_config(row), _row_to_resolved(row), row.user_id) for row in rows
    ]


def _row_to_resolved(row: db_models.Kernel) -> list[ResolvedPackage]:
    raw = getattr(row, "resolved_packages", None)
    if not raw:
        return []
    try:
        items = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if not isinstance(items, list):
        return []
    out: list[ResolvedPackage] = []
    for item in items:
        if isinstance(item, dict) and "name" in item and "version" in item:
            out.append(ResolvedPackage(name=str(item["name"]), version=str(item["version"])))
    return out


def _row_to_config(row: db_models.Kernel) -> KernelConfig:
    packages = json.loads(row.packages) if row.packages else []
    # Tolerate schema drift: getattr with a default lets us still restore a
    # kernel if a column from a later migration is missing (e.g. an alembic
    # stamp that rolled back without dropping the column).
    flavour_value = getattr(row, "image_flavour", None) or ImageFlavour.BASE.value
    try:
        flavour = ImageFlavour(flavour_value)
    except ValueError:
        flavour = ImageFlavour.BASE
    return KernelConfig(
        id=row.id,
        name=row.name,
        packages=packages,
        cpu_cores=row.cpu_cores,
        memory_gb=row.memory_gb,
        gpu=row.gpu,
        image_flavour=flavour,
        custom_image=getattr(row, "custom_image", None),
    )
