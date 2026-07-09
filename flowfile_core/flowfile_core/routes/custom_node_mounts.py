"""Mount management for custom-node directories + a combined catalog listing.

Mounted nodes are listed and usable but read-only for the designer's
save/delete routes (those operate on the default directory only); saving an
edited mounted node writes a copy into the default directory.
"""

from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.flowfile.user_defined.mounts import (
    MountValidationError,
    add_mount,
    load_mounts,
    remove_mount,
)
from flowfile_core.flowfile.user_defined.registry import LoadedNode, registry

router = APIRouter()


def require_admin(current_user=Depends(get_current_active_user)):
    """Registering a mount exec()s every .py in the directory inside core's process, and mounts
    are install-wide (mounts.json, not per-user) — so mutating them is admin-only. Reads stay open."""
    if not getattr(current_user, "is_admin", False):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required")
    return current_user


class MountInfo(BaseModel):
    path: str
    added_at: str
    exists: bool
    node_count: int
    error_count: int


class AddMountRequest(BaseModel):
    path: str


class CatalogCustomNode(BaseModel):
    """Row for the catalog's root-level Custom Nodes listing."""

    node_key: str
    node_name: str
    node_category: str = ""
    file_name: str
    environment: Literal["local", "kernel"] = "local"
    error: str | None = None
    source: str = "default"  # "default" | absolute mount path
    source_label: str = "Default"


def _mount_infos() -> list[MountInfo]:
    counts: dict[str, list[int]] = {}
    for entry in registry.all():
        if entry.mount_path is None:
            continue
        node_count, error_count = counts.setdefault(entry.mount_path, [0, 0])
        counts[entry.mount_path] = [node_count + 1, error_count + (1 if entry.error else 0)]
    infos = []
    for mount in load_mounts():
        node_count, error_count = counts.get(mount.path, [0, 0])
        path = Path(mount.path)
        infos.append(
            MountInfo(
                path=mount.path,
                added_at=mount.added_at,
                exists=path.exists() and path.is_dir(),
                node_count=node_count,
                error_count=error_count,
            )
        )
    return infos


def _catalog_node_from_entry(entry: LoadedNode) -> CatalogCustomNode:
    row = CatalogCustomNode(
        node_key=entry.node_key,
        node_name=entry.file_path.stem.replace("_", " ").title(),
        file_name=entry.file_name,
        error=entry.error,
    )
    if entry.mount_path is not None:
        row.source = entry.mount_path
        row.source_label = Path(entry.mount_path).name or entry.mount_path
    if entry.node_class is not None:
        node = entry.node_class()
        row.node_name = node.node_name
        row.node_category = node.node_category
        row.environment = node.environment
    return row


@router.get("", summary="List custom-node mount directories", response_model=list[MountInfo])
def list_custom_node_mounts(current_user=Depends(get_current_active_user)) -> list[MountInfo]:
    return _mount_infos()


@router.post("", summary="Register a custom-node mount directory", status_code=201)
def add_custom_node_mount(request: AddMountRequest, current_user=Depends(require_admin)) -> dict[str, Any]:
    try:
        mount = add_mount(request.path)
    except MountValidationError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    entries = registry.scan()
    logger.info(f"Registered custom-node mount {mount.path}")
    return {
        "path": mount.path,
        "added_at": mount.added_at,
        "total_nodes": len(entries),
        "mounts": [info.model_dump() for info in _mount_infos()],
    }


@router.delete("", summary="Unregister a custom-node mount directory (never deletes files)")
def delete_custom_node_mount(
    path: str = Query(..., description="Mount path to unregister"),
    current_user=Depends(require_admin),
) -> dict[str, Any]:
    removed = remove_mount(path)
    if removed is None:
        raise HTTPException(status_code=404, detail=f"Mount not found: {path}")
    entries = registry.scan()
    logger.info(f"Unregistered custom-node mount {removed.path}")
    return {
        "path": removed.path,
        "total_nodes": len(entries),
        "mounts": [info.model_dump() for info in _mount_infos()],
    }


@router.get("/nodes", summary="List all custom nodes with their source", response_model=list[CatalogCustomNode])
def list_custom_nodes_with_source(current_user=Depends(get_current_active_user)) -> list[CatalogCustomNode]:
    """Every registered custom node (including broken files) with its origin — default dir or mount."""
    rows = [_catalog_node_from_entry(entry) for entry in registry.all() if not entry.file_name.startswith("_")]
    rows.sort(key=lambda r: (r.node_name.lower(), r.source))
    return rows
