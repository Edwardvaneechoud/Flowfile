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

from fastapi import APIRouter, Depends, File, HTTPException, Response, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.configs.app_settings import get_user_secret
from flowfile_core.database.connection import get_db
from flowfile_core.flowfile.community_nodes import github_client, installer, publish, validation
from flowfile_core.flowfile.community_nodes.client import (
    CommunityClient,
    CommunityUnavailableError,
    PinMismatchError,
    get_community_client,
)
from flowfile_core.flowfile.community_nodes.github_client import (
    ForkPendingError,
    GithubApiError,
    GithubAuthError,
    GithubNotConfiguredError,
)
from flowfile_core.flowfile.community_nodes.installer import (
    BlockedNodeError,
    CollisionError,
    ConsentCapabilityError,
    ConsentRequiredError,
    IncompatibleVersionError,
    NodeNotFoundError,
    ReceiptRequiredError,
    ScanRejectedError,
    YankedNodeError,
)
from flowfile_core.flowfile.community_nodes.models import (
    CommunityIndex,
    InstallRequest,
    get_app_version,
    semver_gt,
)
from flowfile_core.flowfile.community_nodes.receipts import get_receipt
from flowfile_core.flowfile.node_designer.parsing import extract_manifest
from flowfile_core.flowfile.user_defined.registry import registry
from flowfile_core.routes.community_github import GITHUB_PUBLISH_TOKEN_KEY
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
# PNG only: the community pipeline (validation, bundle export) is PNG-only, so
# accepting other formats here would silently drop them from exported bundles.
ALLOWED_SCREENSHOT_EXTENSIONS = {".png"}
MAX_SCREENSHOT_SIZE = 5 * 1024 * 1024
MAX_SCREENSHOTS_PER_STEM = 5


def _safe_node_id(node_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", node_id)


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
    # list_installed prunes receipts for vanished files, so it is the one source
    # of both the receipt and the modified_locally flag for this response.
    installed = {node.receipt.node_id: node for node in installer.list_installed()}
    app_version = get_app_version()

    nodes: list[dict[str, Any]] = []
    for entry in index.nodes:
        pop = popularity.nodes.get(entry.id) if popularity else None
        node = entry.model_dump()
        node["popularity"] = (
            {"thumbs_up": pop.thumbs_up, "discussion_url": pop.discussion_url} if pop is not None else None
        )
        local = installed.get(entry.id)
        node["install_state"] = installer.install_state(
            entry, local.receipt if local else None, local.modified_locally if local else False, app_version
        )
        nodes.append(node)

    return {
        "fetched_at": meta.get("fetched_at", ""),
        "source": meta.get("source", ""),
        "stale": meta.get("stale", False),
        "categories": index.categories,
        "repo_stars": popularity.repo_stars if popularity else 0,
        "nodes": nodes,
        # Installed nodes the registry has since blocked/yanked — delisted from
        # nodes[], so this is the frontend's only signal to warn about them.
        "alerts": [alert.model_dump() for alert in installer.installed_alerts(index)],
    }


@router.get("/node/{node_id}", summary="Community node detail")
def get_node_detail(node_id: str, current_user=Depends(get_current_active_user)) -> dict[str, Any]:
    safe_id = _safe_node_id(node_id)
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
    local = installer.get_installed(entry.id)

    detail = entry.model_dump()
    detail["readme_text"] = readme_text
    detail["icon_file"] = Path(entry.artifacts.icon.path).name if entry.artifacts.icon else None
    detail["screenshots"] = [Path(shot.path).name for shot in entry.artifacts.screenshots]
    detail["popularity"] = (
        {"thumbs_up": pop.thumbs_up, "discussion_url": pop.discussion_url} if pop is not None else None
    )
    detail["install_state"] = installer.install_state(
        entry, local.receipt if local else None, local.modified_locally if local else False, get_app_version()
    )
    return detail


@router.get("/media/{node_id}/{file_name}", summary="Serve pin-verified community media")
def get_media(node_id: str, file_name: str, current_user=Depends(get_current_active_user)) -> FileResponse:
    safe_id = _safe_node_id(node_id)
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
        raise HTTPException(status_code=404, detail={"error_code": "NODE_NOT_FOUND", "node_id": request.node_id}) from e
    except BlockedNodeError as e:
        raise HTTPException(status_code=410, detail={"error_code": "BLOCKED", "node_id": request.node_id}) from e
    except YankedNodeError as e:
        raise HTTPException(status_code=410, detail={"error_code": "YANKED", "node_id": request.node_id}) from e
    except IncompatibleVersionError as e:
        raise HTTPException(
            status_code=409,
            detail={
                "error_code": "INCOMPATIBLE_VERSION",
                "node_id": request.node_id,
                "min_flowfile_version": e.min_flowfile_version,
                "app_version": e.app_version,
                "message": str(e),
            },
        ) from e
    except ConsentRequiredError as e:
        raise HTTPException(status_code=400, detail={"error_code": "CONSENT_REQUIRED", "message": str(e)}) from e
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
        raise HTTPException(status_code=503, detail={"error_code": "COMMUNITY_UNAVAILABLE", "message": str(e)}) from e

    return {
        "success": True,
        "node": _node_info_from_entry(outcome.entry).model_dump(),
        "receipt": outcome.receipt.model_dump(),
        "load_error": outcome.load_error,
    }


@router.delete("/uninstall/{node_id}", summary="Uninstall a community node (admin)")
def uninstall_node(node_id: str, current_user=Depends(require_admin)) -> dict[str, Any]:
    safe_id = _safe_node_id(node_id)
    try:
        installer.uninstall(safe_id)
    except ReceiptRequiredError as e:
        raise HTTPException(status_code=404, detail={"error_code": "NOT_INSTALLED", "node_id": safe_id}) from e
    return {"success": True, "node_id": safe_id}


@router.get("/installed", summary="List installed community nodes")
def list_installed(current_user=Depends(get_current_active_user)) -> list[dict[str, Any]]:
    return [node.model_dump() for node in installer.list_installed()]


@router.get("/updates", summary="Check installed community nodes for updates")
def check_updates(current_user=Depends(get_current_active_user)) -> list[dict[str, Any]]:
    client = get_community_client()
    index, _ = _get_index_or_503(client)
    return [update.model_dump() for update in installer.check_updates(index)]


# Publish-prep screenshots: staged per node file-stem, bundled by /publish-bundle.


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

    # The prep dir also holds the README sidecar; only screenshots count toward the cap.
    existing = [
        p for p in stem_dir.iterdir() if p.is_file() and p.suffix.lower() in ALLOWED_SCREENSHOT_EXTENSIONS
    ]
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
def get_screenshot(file_stem: str, file_name: str, current_user=Depends(get_current_active_user)) -> FileResponse:
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
def delete_screenshot(file_stem: str, file_name: str, current_user=Depends(get_current_active_user)) -> dict[str, Any]:
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


class PublishReadmeBody(BaseModel):
    readme: str = ""


@router.get("/readme/{file_stem}", summary="Stored publish README for a designed node")
def get_publish_readme(file_stem: str, current_user=Depends(get_current_active_user)) -> dict[str, str]:
    """The README sidecar. Falls back once to the published registry README for
    receipt-tracked installs only (a hand-authored node whose stem happens to match a
    registry id must never receive another author's README), then persists it; an
    empty tombstone sidecar (deliberate clear) suppresses the fallback."""
    stem = _safe_stem(file_stem)
    text = publish.load_prep_readme(stem)
    if not text and not publish.readme_prep_path(stem).exists() and get_receipt(stem) is not None:
        client = get_community_client()
        try:
            index, _ = client.get_index()
            entry = index.entry(stem)
            if entry is not None and entry.artifacts.readme is not None:
                text = client.download_pinned(entry.artifacts.readme, index.registry.commit).decode(
                    "utf-8", errors="replace"
                )
                publish.save_prep_readme(stem, text)
        except (CommunityUnavailableError, PinMismatchError):
            text = ""
    return {"readme": text}


@router.put("/readme/{file_stem}", summary="Store the publish README for a designed node")
def put_publish_readme(
    file_stem: str, body: PublishReadmeBody, current_user=Depends(get_current_active_user)
) -> dict[str, bool]:
    if len(body.readme.encode("utf-8")) > validation.README_MAX:
        raise HTTPException(
            status_code=422,
            detail={"error_code": "README_TOO_LARGE", "message": f"README exceeds {validation.README_MAX} bytes"},
        )
    publish.save_prep_readme(_safe_stem(file_stem), body.readme)
    return {"ok": True}


class PublishBundleRequest(BaseModel):
    file_name: str
    license: str
    repository: str = ""
    description: str = ""
    category: str = "Utilities"
    # Markdown for the node's registry README; empty ships the generated TODO stub.
    readme: str = ""
    changelog: str = ""
    # check_only returns the completeness report as JSON (plus node_id/version/
    # published_version context) instead of a zip, so the designer modal can render
    # the checklist and version banner without downloading.
    check_only: bool = False


def _issue_payload(issue) -> dict[str, str]:
    severity = "warning" if issue.code in publish.WARNING_CODES else "error"
    return {"code": issue.code, "message": issue.message, "severity": severity}


@router.post("/publish-bundle", summary="Export a publishable community-node bundle")
def publish_bundle(request: PublishBundleRequest, current_user=Depends(get_current_active_user)):
    entry = registry.get_by_file(request.file_name)
    if entry is None:
        raise HTTPException(status_code=404, detail={"error_code": "NODE_NOT_FOUND", "file_name": request.file_name})

    issues = publish.completeness_check(
        entry,
        license=request.license,
        repository=request.repository,
        description=request.description,
        readme=request.readme,
        changelog=request.changelog,
    )
    errors = [issue for issue in issues if issue.code not in publish.WARNING_CODES]

    if request.check_only:
        node_id = publish.node_id_for(entry)
        published_version = None
        try:
            index, _ = get_community_client().get_index()
            published = index.entry(node_id)
            published_version = published.version if published is not None else None
        except CommunityUnavailableError:
            pass
        return {
            "ok": not errors,
            "issues": [_issue_payload(issue) for issue in issues],
            "node_id": node_id,
            "version": extract_manifest(entry.source_text or "").version or "0.0.0",
            "published_version": published_version,
        }

    if errors:
        raise HTTPException(
            status_code=422,
            detail={"error_code": "INCOMPLETE", "issues": [_issue_payload(issue) for issue in issues]},
        )

    publish.save_prep_readme(Path(entry.file_name).stem, request.readme)
    screenshots = publish.png_screenshots(entry)
    data = publish.build_bundle(
        entry,
        license=request.license,
        repository=request.repository,
        description=request.description,
        category=request.category,
        screenshots=screenshots,
        readme=request.readme,
        changelog=request.changelog,
    )
    file_name = f"{publish.node_id_for(entry)}-bundle.zip"
    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
    )


class PublishPrRequest(BaseModel):
    file_name: str
    license: str
    repository: str = ""
    description: str = ""
    category: str = "Utilities"
    readme: str = ""
    changelog: str = ""


class PublishPrResponse(BaseModel):
    status: str  # created | updated | fork_creating
    pr_url: str = ""
    pr_number: int | None = None
    branch: str = ""
    node_id: str = ""
    version: str = ""
    is_update: bool = False


def _resolve_pr_description(description: str, intro: str | None) -> str:
    # Mirror build_publish_files' manifest description resolution so the PR body matches the manifest.
    intro = (intro or "").strip()
    resolved = (description or intro).strip()[:300]
    if len(resolved) < publish.MIN_DESCRIPTION_LEN:
        resolved = intro[:300] if len(intro) >= publish.MIN_DESCRIPTION_LEN else "A Flowfile community node."
    return resolved


def _pr_api_http(e: GithubApiError) -> HTTPException:
    code = "GITHUB_RATE_LIMITED" if e.reason == "rate_limited" else "GITHUB_API_ERROR"
    return HTTPException(status_code=502, detail={"error_code": code, "message": str(e)})


@router.get("/publish-pr/open", summary="The connected account's open registry PRs for a designed node")
def open_publish_prs(
    file_name: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    entry = registry.get_by_file(file_name)
    if entry is None:
        raise HTTPException(status_code=404, detail={"error_code": "NODE_NOT_FOUND", "file_name": file_name})
    token = get_user_secret(db, GITHUB_PUBLISH_TOKEN_KEY, current_user.id)
    if not token:
        raise HTTPException(
            status_code=412,
            detail={"error_code": "GITHUB_NOT_CONNECTED", "message": "Connect a GitHub account first."},
        )
    node_id = publish.node_id_for(entry)
    try:
        open_prs = github_client.GithubPublisher(token).open_prs_for_node(node_id)
    except GithubAuthError as e:
        raise HTTPException(status_code=412, detail={"error_code": "GITHUB_TOKEN_INVALID", "message": str(e)}) from e
    except GithubApiError as e:
        raise _pr_api_http(e) from e
    return {"node_id": node_id, "open_prs": open_prs}


@router.post("/publish-pr", summary="Publish a community node via a GitHub pull request")
def publish_pr(
    request: PublishPrRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> PublishPrResponse:
    entry = registry.get_by_file(request.file_name)
    if entry is None:
        raise HTTPException(status_code=404, detail={"error_code": "NODE_NOT_FOUND", "file_name": request.file_name})

    issues = publish.completeness_check(
        entry,
        license=request.license,
        repository=request.repository,
        description=request.description,
        readme=request.readme,
        changelog=request.changelog,
    )
    errors = [issue for issue in issues if issue.code not in publish.WARNING_CODES]
    if errors:
        raise HTTPException(
            status_code=422,
            detail={"error_code": "INCOMPLETE", "issues": [_issue_payload(issue) for issue in issues]},
        )

    token = get_user_secret(db, GITHUB_PUBLISH_TOKEN_KEY, current_user.id)
    if not token:
        raise HTTPException(
            status_code=412,
            detail={"error_code": "GITHUB_NOT_CONNECTED", "message": "Connect a GitHub account before publishing."},
        )

    try:
        login = github_client.fetch_login(token)
    except GithubAuthError as e:
        # Leave the rows in place — the user may reconnect; don't wipe on a transient revoke read.
        raise HTTPException(status_code=412, detail={"error_code": "GITHUB_TOKEN_INVALID", "message": str(e)}) from e
    except GithubApiError as e:
        raise _pr_api_http(e) from e

    meta = extract_manifest(entry.source_text or "")
    node_id = publish.node_id_for(entry)
    version = meta.version or "0.0.0"
    resolved_description = _resolve_pr_description(request.description, meta.intro)

    # Best-effort live-index pre-check so a stale version fails before any GitHub write;
    # when the index is unreachable, CI stays authoritative and we proceed as a new node.
    is_update = False
    try:
        index, _ = get_community_client().get_index()
    except CommunityUnavailableError:
        index = None
    if index is not None:
        published = index.entry(node_id)
        if published is not None:
            is_update = True
            if not semver_gt(version, published.version):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error_code": "VERSION_NOT_INCREMENTED",
                        "published_version": published.version,
                        "message": f"Version {version} is not newer than the published {published.version}. "
                        "Bump the version in the Publishing section.",
                    },
                )

    publish.save_prep_readme(Path(entry.file_name).stem, request.readme)
    screenshots = publish.png_screenshots(entry)
    pf = publish.build_publish_files(
        entry,
        license=request.license,
        repository=request.repository,
        description=request.description,
        category=request.category,
        screenshots=screenshots,
        author_github=login,
        readme=request.readme,
        changelog=request.changelog,
    )

    publisher = github_client.GithubPublisher(token)
    branch = publish.pr_branch_name(pf.node_id, pf.version)
    title = publish.pr_title(pf.node_id, pf.node_name, pf.version, is_update)
    body_text = publish.build_pr_body(pf.node_id, pf.version, is_update, resolved_description)
    try:
        fork = publisher.ensure_fork()
        default_branch, base = publisher.base_sha(fork)
        publisher.sync_fork(fork, default_branch)
        _, base = publisher.base_sha(fork)
        publisher.ensure_branch(fork, branch, base)
        message = f"{'Update' if is_update else 'Add'} {pf.node_id} v{pf.version}"
        publisher.commit_files(fork, branch, base, pf.files, message)
        pr = publisher.open_or_get_pr(branch, title, body_text)
        if not pr.created:
            try:
                publisher.update_pr(pr.number, title, body_text)
            except GithubApiError:  # branch already force-updated; title/body refresh is cosmetic
                logger.info("PR #%s title/body refresh failed", pr.number)
    except ForkPendingError:
        return PublishPrResponse(
            status="fork_creating", node_id=pf.node_id, version=pf.version, branch=branch, is_update=is_update
        )
    except GithubAuthError as e:
        raise HTTPException(status_code=412, detail={"error_code": "GITHUB_TOKEN_INVALID", "message": str(e)}) from e
    except GithubNotConfiguredError as e:  # PAT path can't reach this; mapped defensively.
        raise HTTPException(status_code=503, detail={"error_code": "GITHUB_NOT_CONFIGURED", "message": str(e)}) from e
    except GithubApiError as e:
        raise _pr_api_http(e) from e

    return PublishPrResponse(
        status="created" if pr.created else "updated",
        pr_url=pr.url,
        pr_number=pr.number,
        branch=branch,
        node_id=pf.node_id,
        version=pf.version,
        is_update=is_update,
    )
