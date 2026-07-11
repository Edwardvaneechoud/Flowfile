"""Install / uninstall / list / update-check for community nodes.

The install ladder is authoritative — the in-app consent dialog is advisory.
Core re-downloads every artifact, re-verifies its sha256 pin, and re-runs the
AST security scan on the downloaded bytes before writing anything. A deny
finding or a capability the user never acknowledged aborts the install; the
consent dialog only ever *reduces* friction, never grants trust.

Community nodes are written flat into ``storage.user_defined_nodes_directory``
as ``<node_id>.py`` and are distinguished from a user's own nodes solely by
their receipt — every mutation path is receipt-gated so hand-authored nodes are
never touched.
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from flowfile_core.configs import logger
from flowfile_core.configs.settings import get_community_index_url
from flowfile_core.flowfile.community_nodes.client import CommunityClient
from flowfile_core.flowfile.community_nodes.models import (
    CommunityIndex,
    CommunityIndexEntry,
    ConsentRecord,
    InstalledCommunityNode,
    InstallReceipt,
    InstallRequest,
    InstallState,
    UpdateInfo,
    get_app_version,
    is_version_compatible,
    semver_gt,
)
from flowfile_core.flowfile.community_nodes.receipts import (
    list_receipts,
    load_receipts,
    put_receipt,
    remove_receipt,
    save_receipts,
)
from flowfile_core.flowfile.community_nodes.security_scan import scan_source
from flowfile_core.flowfile.user_defined.registry import LoadedNode, registry
from shared import storage

_ALLOWED_ICON_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif"}


class CommunityInstallError(Exception):
    """Base class for install-ladder failures."""


class NodeNotFoundError(CommunityInstallError):
    """The requested node_id is not present in the index (route → 404)."""


class BlockedNodeError(CommunityInstallError):
    """The node is on the registry blocklist (route → 410)."""


class ConsentRequiredError(CommunityInstallError):
    """acknowledged_risks was not set (route → 400 CONSENT_REQUIRED)."""


class CollisionError(CommunityInstallError):
    """A healthy non-community node already owns this node key (route → 409)."""

    def __init__(self, holder_file: str):
        self.holder_file = holder_file
        super().__init__(f"A node named the same is already installed ({holder_file})")


class ScanRejectedError(CommunityInstallError):
    """The server-side re-scan produced a deny finding (route → 400 SCAN_REJECTED)."""

    def __init__(self, rule_ids: list[str]):
        self.rule_ids = rule_ids
        super().__init__(f"Downloaded node source was rejected by the security scan: {', '.join(rule_ids)}")


class ConsentCapabilityError(CommunityInstallError):
    """The scan found capabilities the user never acknowledged (route → 403 CONSENT_CAPABILITIES)."""

    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__(f"Install requires acknowledging capabilities: {', '.join(missing)}")


class ReceiptRequiredError(CommunityInstallError):
    """Uninstall was requested for a node with no community receipt (route → 404)."""


@dataclass
class InstallOutcome:
    entry: LoadedNode
    receipt: InstallReceipt
    load_error: str | None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _community_icon_name(node_id: str, original_path: str) -> str | None:
    name = Path(original_path).name
    if Path(name).suffix.lower() not in _ALLOWED_ICON_SUFFIXES:
        return None
    return f"{node_id}__{name}"


def install(
    request: InstallRequest,
    index: CommunityIndex,
    *,
    client: CommunityClient,
    user_id: int,
    user_email: str,
) -> InstallOutcome:
    entry = index.entry(request.node_id)
    if entry is None:
        raise NodeNotFoundError(request.node_id)
    if request.node_id in index.blocked_ids():
        raise BlockedNodeError(request.node_id)
    if not request.acknowledged_risks:
        raise ConsentRequiredError("Risk acknowledgement is required")

    node_key = entry.id
    holder = registry.get(node_key)
    receipts = load_receipts()
    if holder is not None and holder.node_class is not None and node_key not in receipts:
        raise CollisionError(holder.file_name)

    commit = index.registry.commit
    node_bytes = client.download_pinned(entry.artifacts.node, commit)
    source_text = node_bytes.decode("utf-8", errors="replace")

    report = scan_source(source_text)
    if report.deny_count > 0:
        deny_ids = [f.rule_id for f in report.findings if f.severity == "deny"]
        raise ScanRejectedError(deny_ids or ["FF-SEC-000"])
    missing = sorted(set(report.capabilities) - set(request.acknowledged_capabilities))
    if missing:
        raise ConsentCapabilityError(missing)

    icon_bytes: bytes | None = None
    icon_file: str | None = None
    if entry.artifacts.icon is not None:
        candidate = _community_icon_name(entry.id, entry.artifacts.icon.path)
        if candidate is not None:
            icon_bytes = client.download_pinned(entry.artifacts.icon, commit)
            icon_file = candidate

    nodes_dir = storage.user_defined_nodes_directory
    nodes_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{entry.id}.py"
    file_path = nodes_dir / file_name
    file_path.write_bytes(node_bytes)

    if icon_bytes is not None and icon_file is not None:
        icons_dir = storage.user_defined_nodes_icons
        icons_dir.mkdir(parents=True, exist_ok=True)
        (icons_dir / icon_file).write_bytes(icon_bytes)

    loaded = registry.load_file(file_path)

    receipt = InstallReceipt(
        node_id=entry.id,
        node_key=loaded.node_key or node_key,
        file_name=file_name,
        version=entry.version,
        sha256=hashlib.sha256(node_bytes).hexdigest(),
        icon_file=icon_file,
        installed_at=_now_iso(),
        installed_by=user_email or str(user_id),
        consent=ConsentRecord(
            acknowledged=True,
            at=_now_iso(),
            by=user_email or str(user_id),
            capabilities=list(request.acknowledged_capabilities),
        ),
        source_commit=commit,
        index_url=get_community_index_url(),
        min_flowfile_version=entry.min_flowfile_version,
    )
    put_receipt(receipt)
    logger.info("Installed community node %s (%s)", entry.id, entry.version)
    return InstallOutcome(entry=loaded, receipt=receipt, load_error=loaded.error)


def uninstall(node_id: str) -> InstallReceipt:
    """Remove a community node. Receipt-gated: a node with no receipt is never touched."""
    receipt = load_receipts().get(node_id)
    if receipt is None:
        raise ReceiptRequiredError(node_id)

    registry.remove_file(receipt.file_name)
    file_path = storage.user_defined_nodes_directory / receipt.file_name
    if file_path.exists():
        try:
            file_path.unlink()
        except OSError as e:
            logger.warning("Could not delete community node file %s: %s", file_path, e)
    # Community icons are namespaced "<node_id>__…" at install, so only those are removed.
    if receipt.icon_file and receipt.icon_file.startswith(f"{node_id}__"):
        try:
            (storage.user_defined_nodes_icons / receipt.icon_file).unlink(missing_ok=True)
        except OSError as e:
            logger.warning("Could not delete community node icon %s: %s", receipt.icon_file, e)
    remove_receipt(node_id)
    logger.info("Uninstalled community node %s", node_id)
    return receipt


def _file_sha256(file_name: str) -> str | None:
    path = storage.user_defined_nodes_directory / file_name
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _joined(receipt: InstallReceipt, current_hash: str) -> InstalledCommunityNode:
    entry = registry.get(receipt.node_key)
    node_name = ""
    error = None
    if entry is not None:
        error = entry.error
        if entry.node_class is not None:
            node_name = entry.node_class().node_name
    return InstalledCommunityNode(
        receipt=receipt,
        node_name=node_name,
        modified_locally=current_hash != receipt.sha256,
        error=error,
    )


def list_installed() -> list[InstalledCommunityNode]:
    """Receipts joined with registry state; prunes receipts whose file vanished."""
    receipts = load_receipts()
    pruned: list[str] = []
    installed: list[InstalledCommunityNode] = []
    for node_id, receipt in receipts.items():
        current_hash = _file_sha256(receipt.file_name)
        if current_hash is None:
            pruned.append(node_id)
            continue
        installed.append(_joined(receipt, current_hash))
    if pruned:
        remaining = {nid: r for nid, r in receipts.items() if nid not in pruned}
        save_receipts(remaining)
        logger.info("Pruned %d community receipt(s) with missing files", len(pruned))
    return installed


def get_installed(node_id: str) -> InstalledCommunityNode | None:
    """Single-node variant of list_installed: one receipt read, one file hash."""
    receipt = load_receipts().get(node_id)
    if receipt is None:
        return None
    current_hash = _file_sha256(receipt.file_name)
    if current_hash is None:
        remove_receipt(node_id)
        return None
    return _joined(receipt, current_hash)


def _has_update(entry: CommunityIndexEntry, receipt: InstallReceipt) -> bool:
    """Newer semver, or the same version republished with different pinned bytes."""
    return semver_gt(entry.version, receipt.version) or (
        entry.version == receipt.version and entry.artifacts.node.sha256 != receipt.sha256
    )


def check_updates(index: CommunityIndex) -> list[UpdateInfo]:
    updates: list[UpdateInfo] = []
    for receipt in list_receipts():
        entry = index.entry(receipt.node_id)
        if entry is None:
            continue
        sha_changed = entry.version == receipt.version and entry.artifacts.node.sha256 != receipt.sha256
        if _has_update(entry, receipt):
            updates.append(
                UpdateInfo(
                    node_id=receipt.node_id,
                    installed_version=receipt.version,
                    latest_version=entry.version,
                    sha256_changed=sha_changed,
                    changelog=entry.changelog,
                )
            )
    return updates


def install_state(
    entry: CommunityIndexEntry,
    receipt: InstallReceipt | None,
    modified_locally: bool,
    app_version: str | None = None,
) -> InstallState:
    app_version = get_app_version() if app_version is None else app_version
    if receipt is None:
        if not is_version_compatible(entry.min_flowfile_version, app_version):
            return "incompatible"
        return "not_installed"
    if _has_update(entry, receipt):
        return "update_available"
    if modified_locally:
        return "modified_locally"
    return "installed"
