"""REST surface for browsing and installing community nodes.

All routes are JWT-gated; ``install`` / ``uninstall`` additionally require admin
(same reasoning as custom-node mounts — they land executable code install-wide).
No trailing slashes. Registry access goes through the ``CommunityClient``
singleton; the install ladder lives in ``installer`` and is authoritative over
the advisory consent dialog.
"""

import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.flowfile.community_nodes import installer
from flowfile_core.flowfile.community_nodes.client import (
    CommunityClient,
    CommunityUnavailableError,
    PinMismatchError,
    get_community_client,
)
from flowfile_core.flowfile.community_nodes.installer import (
    BlockedNodeError,
    CollisionError,
    ConsentCapabilityError,
    ConsentRequiredError,
    NodeNotFoundError,
    ReceiptRequiredError,
    ScanRejectedError,
)
from flowfile_core.flowfile.community_nodes.models import CommunityIndex, InstallRequest, get_app_version
from flowfile_core.flowfile.community_nodes.receipts import load_receipts
from flowfile_core.routes.custom_node_mounts import require_admin
from flowfile_core.routes.user_defined_components import _node_info_from_entry
from shared import storage

router = APIRouter()

_MEDIA_CONTENT_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
}
ALLOWED_SCREENSHOT_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
MAX_SCREENSHOT_SIZE = 5 * 1024 * 1024
MAX_SCREENSHOTS_PER_STEM = 5


def _safe_media_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.\-]", "_", name)


def _safe_stem(stem: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_\-]", "_", stem)
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid file stem")
    return safe


def _get_index_or_503(client: CommunityClient, refresh: bool = False) -> tuple[CommunityIndex, dict]:
    try:
        return client.get_index(refresh=refresh)
    except CommunityUnavailableError as e:
        raise HTTPException(
            status_code=503,
            detail={"error_code": "COMMUNITY_UNAVAILABLE", "message": str(e)},
        ) from e


@router.get("/index", summary="Browse the community node registry")
def get_index(refresh: bool = False, current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    client = get_community_client()
    index, meta = _get_index_or_503(client, refresh=refresh)
    popularity = client.get_popularity()
    receipts = load_receipts()
    modified_map = {node.receipt.node_id: node.modified_locally for node in installer.list_installed()}
    app_version = get_app_version()

    nodes: list[dict[str, Any]] = []
    for entry in index.nodes:
        pop = popularity.nodes.get(entry.id) if popularity else None
        node = entry.model_dump()
        node["popularity"] = (
            {"thumbs_up": pop.thumbs_up, "discussion_url": pop.discussion_url} if pop is not None else None
        )
        node["install_state"] = installer.install_state(
            entry, receipts.get(entry.id), modified_map.get(entry.id, False), app_version
        )
        nodes.append(node)

    return {
        "fetched_at": meta.get("fetched_at", ""),
        "source": meta.get("source", ""),
        "stale": meta.get("stale", False),
        "categories": index.categories,
        "repo_stars": popularity.repo_stars if popularity else 0,
        "nodes": nodes,
    }


@router.get("/node/{node_id}", summary="Community node detail")
def get_node_detail(node_id: str, current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", node_id)
    client = get_community_client()
    index, _ = _get_index_or_503(client)
    entry = index.entry(safe_id)
    if entry is None:
        raise HTTPException(status_code=404, detail={"error_code": "NODE_NOT_FOUND", "node_id": safe_id})

    readme_text: str | None = None
    if entry.artifacts.readme is not None:
        try:
            readme_text = client.download_pinned(entry.artifacts.readme, index.registry.commit).decode(
                "utf-8", errors="replace"
            )
        except (PinMismatchError, CommunityUnavailableError) as e:
            logger.warning("Could not fetch README for %s: %s", safe_id, e)

    popularity = client.get_popularity()
    pop = popularity.nodes.get(entry.id) if popularity else None
    receipts = load_receipts()
    modified_map = {node.receipt.node_id: node.modified_locally for node in installer.list_installed()}

    detail = entry.model_dump()
    detail["readme_text"] = readme_text
    detail["icon_file"] = Path(entry.artifacts.icon.path).name if entry.artifacts.icon else None
    detail["screenshots"] = [Path(shot.path).name for shot in entry.artifacts.screenshots]
    detail["popularity"] = (
        {"thumbs_up": pop.thumbs_up, "discussion_url": pop.discussion_url} if pop is not None else None
    )
    detail["install_state"] = installer.install_state(
        entry, receipts.get(entry.id), modified_map.get(entry.id, False), get_app_version()
    )
    return detail


@router.get("/media/{node_id}/{file_name}", summary="Serve pin-verified community media")
def get_media(node_id: str, file_name: str, current_user=Depends(get_current_active_user)) -> FileResponse:
    safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", node_id)
    safe_name = _safe_media_name(file_name)
    suffix = Path(safe_name).suffix.lower()
    if suffix not in _MEDIA_CONTENT_TYPES:
        raise HTTPException(status_code=404, detail="Unsupported media type")

    client = get_community_client()
    index, _ = _get_index_or_503(client)
    try:
        path = client.get_media(safe_id, safe_name, index)
    except PinMismatchError as e:
        raise HTTPException(status_code=502, detail={"error_code": "PIN_MISMATCH", "message": str(e)}) from e
    except CommunityUnavailableError as e:
        raise HTTPException(status_code=404, detail={"error_code": "MEDIA_NOT_FOUND", "message": str(e)}) from e

    return FileResponse(
        path=path,
        media_type=_MEDIA_CONTENT_TYPES[suffix],
        filename=safe_name,
        headers={"X-Content-Type-Options": "nosniff"},
    )


@router.post("/install", summary="Install a community node (admin)")
def install_node(request: InstallRequest, current_user=Depends(require_admin)) -> dict[str, Any]:
    client = get_community_client()
    index, _ = _get_index_or_503(client)
    try:
        outcome = installer.install(
            request,
            index,
            client=client,
            user_id=current_user.id,
            user_email=getattr(current_user, "email", "") or "",
        )
    except NodeNotFoundError as e:
        raise HTTPException(
            status_code=404, detail={"error_code": "NODE_NOT_FOUND", "node_id": request.node_id}
        ) from e
    except BlockedNodeError as e:
        raise HTTPException(status_code=410, detail={"error_code": "BLOCKED", "node_id": request.node_id}) from e
    except ConsentRequiredError as e:
        raise HTTPException(
            status_code=400, detail={"error_code": "CONSENT_REQUIRED", "message": str(e)}
        ) from e
    except CollisionError as e:
        raise HTTPException(
            status_code=409,
            detail={"error_code": "NODE_NAME_COLLISION", "holder_file": e.holder_file, "message": str(e)},
        ) from e
    except ScanRejectedError as e:
        raise HTTPException(
            status_code=400, detail={"error_code": "SCAN_REJECTED", "rule_ids": e.rule_ids, "message": str(e)}
        ) from e
    except ConsentCapabilityError as e:
        raise HTTPException(
            status_code=403,
            detail={"error_code": "CONSENT_CAPABILITIES", "missing": e.missing, "message": str(e)},
        ) from e
    except PinMismatchError as e:
        raise HTTPException(status_code=502, detail={"error_code": "PIN_MISMATCH", "message": str(e)}) from e
    except CommunityUnavailableError as e:
        raise HTTPException(
            status_code=503, detail={"error_code": "COMMUNITY_UNAVAILABLE", "message": str(e)}
        ) from e

    return {
        "success": True,
        "node": _node_info_from_entry(outcome.entry).model_dump(),
        "receipt": outcome.receipt.model_dump(),
        "load_error": outcome.load_error,
    }


@router.delete("/uninstall/{node_id}", summary="Uninstall a community node (admin)")
def uninstall_node(node_id: str, current_user=Depends(require_admin)) -> dict[str, Any]:
    safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", node_id)
    try:
        installer.uninstall(safe_id)
    except ReceiptRequiredError as e:
        raise HTTPException(
            status_code=404, detail={"error_code": "NOT_INSTALLED", "node_id": safe_id}
        ) from e
    return {"success": True, "node_id": safe_id}


@router.get("/installed", summary="List installed community nodes")
def list_installed(current_user=Depends(get_current_active_user)) -> list[dict[str, Any]]:
    return [node.model_dump() for node in installer.list_installed()]


@router.get("/updates", summary="Check installed community nodes for updates")
def check_updates(current_user=Depends(get_current_active_user)) -> list[dict[str, Any]]:
    client = get_community_client()
    index, _ = _get_index_or_503(client)
    return [update.model_dump() for update in installer.check_updates(index)]


# ==================== Publish-prep screenshots (Workstream C) ====================


@router.post("/screenshots/{file_stem}", summary="Upload a publish-prep screenshot")
async def upload_screenshot(
    file_stem: str, file: UploadFile = File(...), current_user=Depends(get_current_active_user)
) -> dict[str, Any]:
    safe_stem = _safe_stem(file_stem)
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in ALLOWED_SCREENSHOT_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Allowed types: {', '.join(sorted(ALLOWED_SCREENSHOT_EXTENSIONS))}",
        )

    content = await file.read()
    if len(content) > MAX_SCREENSHOT_SIZE:
        raise HTTPException(
            status_code=400, detail=f"File too large. Maximum size is {MAX_SCREENSHOT_SIZE // (1024 * 1024)}MB"
        )

    safe_name = _safe_media_name(file.filename)
    stem_dir = storage.user_defined_nodes_screenshots / safe_stem
    stem_dir.mkdir(parents=True, exist_ok=True)

    existing = [p for p in stem_dir.iterdir() if p.is_file()]
    if not (stem_dir / safe_name).exists() and len(existing) >= MAX_SCREENSHOTS_PER_STEM:
        raise HTTPException(status_code=400, detail=f"At most {MAX_SCREENSHOTS_PER_STEM} screenshots per node")

    file_path = stem_dir / safe_name
    try:
        file_path.write_bytes(content)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to save screenshot: {e}") from e
    logger.info("Saved publish screenshot %s", file_path)
    return {"success": True, "file_name": safe_name}


@router.get("/screenshots/{file_stem}", summary="List publish-prep screenshots for a node")
def list_screenshots(file_stem: str, current_user=Depends(get_current_active_user)) -> list[dict[str, str]]:
    safe_stem = _safe_stem(file_stem)
    stem_dir = storage.user_defined_nodes_screenshots / safe_stem
    if not stem_dir.exists():
        return []
    shots = [
        {"file_name": p.name}
        for p in sorted(stem_dir.iterdir())
        if p.is_file() and p.suffix.lower() in ALLOWED_SCREENSHOT_EXTENSIONS
    ]
    return shots


@router.get("/screenshots/{file_stem}/{file_name}", summary="Serve a publish-prep screenshot")
def get_screenshot(
    file_stem: str, file_name: str, current_user=Depends(get_current_active_user)
) -> FileResponse:
    safe_stem = _safe_stem(file_stem)
    safe_name = _safe_media_name(file_name)
    suffix = Path(safe_name).suffix.lower()
    if suffix not in _MEDIA_CONTENT_TYPES:
        raise HTTPException(status_code=404, detail="Unsupported media type")
    file_path = storage.user_defined_nodes_screenshots / safe_stem / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Screenshot '{safe_name}' not found")
    return FileResponse(
        path=file_path,
        media_type=_MEDIA_CONTENT_TYPES[suffix],
        filename=safe_name,
        headers={"X-Content-Type-Options": "nosniff"},
    )


@router.delete("/screenshots/{file_stem}/{file_name}", summary="Delete a publish-prep screenshot")
def delete_screenshot(
    file_stem: str, file_name: str, current_user=Depends(get_current_active_user)
) -> dict[str, Any]:
    safe_stem = _safe_stem(file_stem)
    safe_name = _safe_media_name(file_name)
    file_path = storage.user_defined_nodes_screenshots / safe_stem / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Screenshot '{safe_name}' not found")
    try:
        file_path.unlink()
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete screenshot: {e}") from e
    return {"success": True, "file_name": safe_name}
