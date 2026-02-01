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
from flowfile_core.kernel.models import KernelConfig, KernelInfo

logger = logging.getLogger(__name__)


def save_kernel(db: Session, kernel: KernelInfo, user_id: int) -> None:
    """Insert or update a kernel record in the database."""
    existing = db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel.id).first()
    if existing:
        existing.name = kernel.name
        existing.packages = json.dumps(kernel.packages)
        existing.cpu_cores = kernel.cpu_cores
        existing.memory_gb = kernel.memory_gb
        existing.gpu = kernel.gpu
        existing.user_id = user_id
    else:
        record = db_models.Kernel(
            id=kernel.id,
            name=kernel.name,
            user_id=user_id,
            packages=json.dumps(kernel.packages),
            cpu_cores=kernel.cpu_cores,
            memory_gb=kernel.memory_gb,
            gpu=kernel.gpu,
        )
        db.add(record)
    db.commit()


def delete_kernel(db: Session, kernel_id: str) -> None:
    """Remove a kernel record from the database."""
    db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).delete()
    db.commit()


def get_kernels_for_user(db: Session, user_id: int) -> list[KernelConfig]:
    """Return all persisted kernel configs belonging to a user."""
    rows = db.query(db_models.Kernel).filter(db_models.Kernel.user_id == user_id).all()
    return [_row_to_config(row) for row in rows]


def get_all_kernels(db: Session) -> list[tuple[KernelConfig, int]]:
    """Return all persisted kernels as (config, user_id) tuples."""
    rows = db.query(db_models.Kernel).all()
    return [(_row_to_config(row), row.user_id) for row in rows]


def _row_to_config(row: db_models.Kernel) -> KernelConfig:
    packages = json.loads(row.packages) if row.packages else []
    return KernelConfig(
        id=row.id,
        name=row.name,
        packages=packages,
        cpu_cores=row.cpu_cores,
        memory_gb=row.memory_gb,
        gpu=row.gpu,
    )
