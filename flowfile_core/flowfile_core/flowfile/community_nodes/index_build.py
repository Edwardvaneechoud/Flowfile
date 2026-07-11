"""Deterministic index builder for the community registry.

Walks ``nodes/*`` and emits ``index.json`` (a ``CommunityIndex``): per-file
sha256 pins (the app's root of trust), AST-extracted behavioral fields, scanner
risk flags, and timestamp carry-over from the previous index. The blocklist and
category vocabulary come from ``registry/*.json``. Output is deterministic given
the same inputs, so CI can commit it and diffs stay reviewable.
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from flowfile_core.flowfile.community_nodes.models import (
    BlockedEntry,
    CommunityArtifacts,
    CommunityIndex,
    CommunityIndexEntry,
    CommunityManifest,
    PinnedFile,
    RegistryInfo,
    YankedEntry,
)
from flowfile_core.flowfile.community_nodes.security_scan import scan_source
from flowfile_core.flowfile.node_designer.parsing import extract_manifest, parse_source


def _pin(repo_root: Path, path: Path) -> PinnedFile:
    data = path.read_bytes()
    return PinnedFile(
        path=path.relative_to(repo_root).as_posix(),
        sha256=hashlib.sha256(data).hexdigest(),
        size=len(data),
    )


def _load_json(path: Path):
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _load_categories(path: Path) -> list[str]:
    data = _load_json(path)
    if isinstance(data, dict):
        data = data.get("categories", [])
    if isinstance(data, list):
        return [c for c in data if isinstance(c, str)]
    return []


def _to_blocked(entry) -> BlockedEntry:
    if isinstance(entry, BlockedEntry):
        return entry
    if isinstance(entry, dict):
        return BlockedEntry(**entry)
    return BlockedEntry(id=entry)


def _artifacts(repo_root: Path, node_dir: Path) -> CommunityArtifacts:
    icon = node_dir / "icon.png"
    readme = node_dir / "README.md"
    shots_dir = node_dir / "screenshots"
    screenshots = (
        [_pin(repo_root, s) for s in sorted(shots_dir.glob("*.png"), key=lambda p: p.name)]
        if shots_dir.is_dir()
        else []
    )
    return CommunityArtifacts(
        node=_pin(repo_root, node_dir / "node.py"),
        manifest=_pin(repo_root, node_dir / "manifest.json"),
        icon=_pin(repo_root, icon) if icon.is_file() else None,
        readme=_pin(repo_root, readme) if readme.is_file() else None,
        screenshots=screenshots,
    )


def _build_entry(repo_root: Path, node_dir: Path, gen: str, prev: CommunityIndexEntry | None) -> CommunityIndexEntry:
    manifest = CommunityManifest.model_validate_json((node_dir / "manifest.json").read_text(encoding="utf-8"))
    source = (node_dir / "node.py").read_text(encoding="utf-8")
    py = extract_manifest(source)

    published_at = prev.published_at if (prev and prev.published_at) else gen
    if prev and prev.version == manifest.version:
        updated_at = prev.updated_at or gen
    else:
        updated_at = gen

    return CommunityIndexEntry(
        id=manifest.id,
        node_name=manifest.node_name,
        version=manifest.version,
        description=manifest.description,
        author=manifest.author,
        maintainers=manifest.maintainers,
        license=manifest.license,
        category=manifest.category,
        palette_category=py.node_category,
        tags=manifest.tags,
        environment=py.environment.kind,
        dependencies=py.environment.dependencies,
        number_of_inputs=py.number_of_inputs,
        number_of_outputs=py.number_of_outputs,
        min_flowfile_version=manifest.min_flowfile_version,
        designer_editable=parse_source(source).mode == "designer",
        risk_flags=list(scan_source(source).capabilities),
        published_at=published_at,
        updated_at=updated_at,
        changelog=manifest.changelog,
        path=node_dir.relative_to(repo_root).as_posix(),
        artifacts=_artifacts(repo_root, node_dir),
    )


def build_index(
    repo_root: Path,
    previous: CommunityIndex | None,
    commit: str,
    generated_at: str | None = None,
) -> CommunityIndex:
    """Build a ``CommunityIndex`` from ``repo_root/nodes/*``.

    ``previous`` carries timestamps and registry metadata forward; ``commit`` is
    stamped into ``registry.commit`` (the pin URLs are composed from it client-side).
    Blocked ids from ``registry/blocklist.json`` are excluded from ``nodes[]``.
    """
    repo_root = Path(repo_root)
    gen = generated_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    blocklist = _load_json(repo_root / "registry" / "blocklist.json") or {}
    blocked_entries = [_to_blocked(entry) for entry in blocklist.get("blocked", []) or []]
    yanked_entries = [YankedEntry(**entry) for entry in blocklist.get("yanked", []) or [] if isinstance(entry, dict)]
    blocked_ids = {entry.id for entry in blocked_entries}

    categories = _load_categories(repo_root / "registry" / "categories.json")
    config = _load_json(repo_root / "registry" / "config.json") or {}
    prev_entries = {entry.id: entry for entry in previous.nodes} if previous is not None else {}

    entries: list[CommunityIndexEntry] = []
    nodes_dir = repo_root / "nodes"
    if nodes_dir.is_dir():
        for node_dir in sorted((p for p in nodes_dir.iterdir() if p.is_dir()), key=lambda p: p.name):
            if not ((node_dir / "node.py").is_file() and (node_dir / "manifest.json").is_file()):
                continue
            manifest = CommunityManifest.model_validate_json((node_dir / "manifest.json").read_text(encoding="utf-8"))
            if manifest.id in blocked_ids:
                continue
            entries.append(_build_entry(repo_root, node_dir, gen, prev_entries.get(manifest.id)))

    entries.sort(key=lambda entry: entry.id)

    prev_registry = previous.registry if previous is not None else RegistryInfo()
    registry = RegistryInfo(
        repo=config.get("repo") or prev_registry.repo,
        commit=commit,
        min_supported_app_version=config.get("min_supported_app_version") or prev_registry.min_supported_app_version,
    )

    return CommunityIndex(
        schema_version=1,
        generated_at=gen,
        registry=registry,
        categories=categories,
        nodes=entries,
        yanked=yanked_entries,
        blocked=blocked_entries,
    )
