"""Publish-time bundle validation for community nodes.

Runs the full check ladder over a single node folder (``node.py`` +
``manifest.json`` + optional PNG media/README). This is the single source of
truth used by both the community repo's CI (via ``cli.py``) and the in-app
publish-bundle completeness check — local check == CI check.

Every failure carries a stable UPPER_SNAKE ``code`` so the frontend and CI can
key off it. The reviewed PR remains the real security gate; this ladder blocks
the cheap footguns and surfaces capabilities for the consent dialog.
"""

import re
import struct
from pathlib import Path

from pydantic import BaseModel, ValidationError

from flowfile_core.flowfile.community_nodes.models import (
    CommunityIndex,
    CommunityManifest,
    semver_gt,
    slugify_node_name,
)
from flowfile_core.flowfile.community_nodes.security_scan import ScanReport, scan_source
from flowfile_core.flowfile.node_designer.parsing import (
    extract_example_inputs,
    extract_example_settings,
    extract_manifest,
    parse_source,
)

# Back-compat alias; the lift lives in parsing.py next to extract_example_inputs.
_extract_example_settings = extract_example_settings

REPO_MAINTAINER = "edwardvaneechoud"

NODE_PY_MAX = 200 * 1024
MANIFEST_MAX = 16 * 1024
ICON_MAX = 256 * 1024
ICON_DIM_MAX = 512
README_MAX = 100 * 1024
SCREENSHOT_MAX = 1024 * 1024
SCREENSHOT_DIM_MAX = 2000
SCREENSHOT_COUNT_MAX = 5
FOLDER_MAX = 6 * 1024 * 1024

_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_SCREENSHOT_NAME_RE = re.compile(r"^[a-z0-9_-]{1,40}\.png$")

# Fatal parse issues: the file cannot load or run at all. Other parse issues only
# mean the node is not designer-editable (``code_only``), which is allowed.
_FATAL_PARSE_CODES = {"SYNTAX_ERROR", "NO_NODE_CLASS", "MULTIPLE_NODE_CLASSES"}

_VERSION_OPS = ("==", ">=", "<=", "~=", "!=", ">", "<")


class Issue(BaseModel):
    code: str
    message: str


class ValidationReport(BaseModel):
    ok: bool = True
    errors: list[Issue] = []
    warnings: list[Issue] = []
    scan: ScanReport | None = None
    parse_mode: str | None = None
    manifest: CommunityManifest | None = None
    capabilities: list[str] = []


class _BundleFiles(BaseModel):
    node_py: Path | None = None
    manifest_file: Path | None = None
    icon: Path | None = None
    readme: Path | None = None
    screenshots: list[Path] = []

    model_config = {"arbitrary_types_allowed": True}


def png_dimensions(data: bytes) -> tuple[int, int] | None:
    """Return (width, height) from a PNG's IHDR chunk, or None if not a valid PNG.

    Stdlib-only (no Pillow): the first chunk after the 8-byte signature must be
    IHDR carrying big-endian width/height. Community media is PNG-only.
    """
    if len(data) < 24 or data[:8] != _PNG_MAGIC or data[12:16] != b"IHDR":
        return None
    width, height = struct.unpack(">II", data[16:24])
    return int(width), int(height)


def _looks_like_svg(data: bytes) -> bool:
    head = data[:512].lstrip()
    return head.startswith(b"<?xml") or head.startswith(b"<svg") or b"<svg" in head


def sniff_image_format(data: bytes) -> str:
    """Best-effort magic-byte format name, so a mislabeled file names its real type."""
    if data[:3] == b"\xff\xd8\xff":
        return "JPEG"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "GIF"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "WebP"
    if data[:2] == b"BM":
        return "BMP"
    if _looks_like_svg(data):
        return "SVG"
    return "unrecognized data"


def _valid_dependency_spec(spec: str) -> bool:
    """A dependency must be a plain PyPI name with an optional version specifier.

    Rejects URLs, VCS (``git+``), pip flags, and dependency-confusion vectors
    (leading ``.``/``/``/``_``).
    """
    spec = spec.strip()
    if not spec or spec[0] in "._/-" or "://" in spec or spec.lower().startswith("git+"):
        return False
    name, op = spec, None
    for candidate in _VERSION_OPS:
        idx = spec.find(candidate)
        if idx != -1:
            name, op = spec[:idx], spec[idx:]
            break
    name = name.split("[", 1)[0].strip()  # tolerate extras: name[extra]
    if not name or name[0] in "._-" or not all(c.isalnum() or c in "._-" for c in name):
        return False
    if op is not None:
        version = op.lstrip("<>=~!").strip()
        if not version or any(c.isspace() for c in version):
            return False
    return True


def _blocked_ids(blocklist: dict | None) -> set[str]:
    ids: set[str] = set()
    for entry in (blocklist or {}).get("blocked", []) or []:
        if isinstance(entry, str):
            ids.add(entry)
        elif isinstance(entry, dict) and isinstance(entry.get("id"), str):
            ids.add(entry["id"])
    return ids


def _check_folder_contract(folder: Path, errors: list[Issue]) -> _BundleFiles:
    files = _BundleFiles()
    total = 0
    for entry in sorted(folder.iterdir(), key=lambda p: p.name):
        if entry.is_dir():
            if entry.name == "screenshots":
                total += _check_screenshots(entry, files, errors)
            else:
                errors.append(Issue(code="UNEXPECTED_FILE", message=f"Unexpected directory: {entry.name}/"))
            continue
        if entry.suffix.lower() == ".svg":
            errors.append(Issue(code="SVG_FORBIDDEN", message=f"SVG files are not allowed: {entry.name}"))
            continue
        size = entry.stat().st_size
        total += size
        if entry.name == "node.py":
            files.node_py = entry
            if size > NODE_PY_MAX:
                errors.append(Issue(code="NODE_PY_TOO_LARGE", message=f"node.py is {size} bytes (max {NODE_PY_MAX})"))
        elif entry.name == "manifest.json":
            files.manifest_file = entry
            if size > MANIFEST_MAX:
                errors.append(
                    Issue(code="MANIFEST_TOO_LARGE", message=f"manifest.json is {size} bytes (max {MANIFEST_MAX})")
                )
        elif entry.name == "icon.png":
            files.icon = entry
            if size > ICON_MAX:
                errors.append(Issue(code="ICON_TOO_LARGE", message=f"icon.png is {size} bytes (max {ICON_MAX})"))
        elif entry.name == "README.md":
            files.readme = entry
            if size > README_MAX:
                errors.append(Issue(code="README_TOO_LARGE", message=f"README.md is {size} bytes (max {README_MAX})"))
        else:
            errors.append(Issue(code="UNEXPECTED_FILE", message=f"Unexpected file: {entry.name}"))

    if files.node_py is None:
        errors.append(Issue(code="NODE_PY_MISSING", message="node.py is required"))
    if files.manifest_file is None:
        errors.append(Issue(code="MANIFEST_MISSING", message="manifest.json is required"))
    if total > FOLDER_MAX:
        errors.append(Issue(code="FOLDER_TOO_LARGE", message=f"Folder totals {total} bytes (max {FOLDER_MAX})"))
    return files


def _check_screenshots(screenshots_dir: Path, files: _BundleFiles, errors: list[Issue]) -> int:
    total = 0
    for shot in sorted(screenshots_dir.iterdir(), key=lambda p: p.name):
        if shot.suffix.lower() == ".svg":
            errors.append(Issue(code="SVG_FORBIDDEN", message=f"SVG files are not allowed: screenshots/{shot.name}"))
            continue
        if not shot.is_file() or not _SCREENSHOT_NAME_RE.match(shot.name):
            errors.append(Issue(code="UNEXPECTED_FILE", message=f"Unexpected screenshots entry: {shot.name}"))
            continue
        size = shot.stat().st_size
        total += size
        files.screenshots.append(shot)
        if size > SCREENSHOT_MAX:
            errors.append(
                Issue(
                    code="SCREENSHOT_TOO_LARGE",
                    message=f"screenshots/{shot.name} is {size} bytes (max {SCREENSHOT_MAX})",
                )
            )
    if len(files.screenshots) > SCREENSHOT_COUNT_MAX:
        errors.append(
            Issue(
                code="TOO_MANY_SCREENSHOTS",
                message=f"{len(files.screenshots)} screenshots (max {SCREENSHOT_COUNT_MAX})",
            )
        )
    return total


def _read_text(path: Path, missing_code: str, errors: list[Issue]) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        errors.append(Issue(code=missing_code, message=f"Could not read {path.name}: {e}"))
        return None


def _parse_manifest(path: Path, errors: list[Issue]) -> CommunityManifest | None:
    text = _read_text(path, "MANIFEST_INVALID", errors)
    if text is None:
        return None
    try:
        return CommunityManifest.model_validate_json(text)
    except (ValidationError, ValueError) as e:
        errors.append(Issue(code="MANIFEST_INVALID", message=f"manifest.json failed validation: {e}"))
        return None


def _check_identity(
    folder: Path,
    manifest: CommunityManifest,
    py_node_name: str | None,
    blocklist: dict | None,
    categories: list[str] | None,
    errors: list[Issue],
    warnings: list[Issue],
) -> None:
    if folder.name != manifest.id:
        errors.append(Issue(code="ID_MISMATCH", message=f"Folder name '{folder.name}' != manifest.id '{manifest.id}'"))
    if py_node_name is not None:
        slug = slugify_node_name(py_node_name)
        if slug != manifest.id:
            errors.append(
                Issue(code="ID_MISMATCH", message=f"slug of node_name '{slug}' != manifest.id '{manifest.id}'")
            )
        if py_node_name != manifest.node_name:
            errors.append(
                Issue(
                    code="NODE_NAME_MISMATCH",
                    message=f"node.py node_name '{py_node_name}' != manifest.node_name '{manifest.node_name}'",
                )
            )
    if manifest.id in _blocked_ids(blocklist):
        errors.append(Issue(code="ID_BLOCKED", message=f"'{manifest.id}' is blocked and retired forever"))
    try:
        from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

        if manifest.id in get_all_standard_nodes()[1]:
            errors.append(Issue(code="ID_BUILTIN", message=f"'{manifest.id}' collides with a built-in node type"))
    except Exception as e:  # importing the node store is heavy; degrade to a warning
        warnings.append(Issue(code="BUILTIN_CHECK_SKIPPED", message=f"Could not check built-in node types: {e}"))
    if categories is not None and manifest.category not in categories:
        errors.append(
            Issue(code="CATEGORY_UNKNOWN", message=f"category '{manifest.category}' is not in the allowed set")
        )


def _check_version_rules(
    manifest: CommunityManifest, index: CommunityIndex | None, pr_author: str | None, errors: list[Issue]
) -> None:
    if index is None:
        return
    existing = index.entry(manifest.id)
    if existing is not None:
        if not semver_gt(manifest.version, existing.version):
            errors.append(
                Issue(
                    code="VERSION_NOT_INCREMENTED",
                    message=f"version '{manifest.version}' must be greater than published '{existing.version}'",
                )
            )
        if pr_author is not None and pr_author not in manifest.authorized_logins() and pr_author != REPO_MAINTAINER:
            errors.append(
                Issue(code="UNAUTHORIZED_UPDATE", message=f"'{pr_author}' is not an author/maintainer of this node")
            )
    elif pr_author is not None and pr_author != manifest.author.github and pr_author != REPO_MAINTAINER:
        errors.append(
            Issue(
                code="UNAUTHORIZED_NEW_NODE",
                message=f"new node author.github '{manifest.author.github}' must equal the PR author '{pr_author}'",
            )
        )


def _check_cross(py_manifest, manifest: CommunityManifest, errors: list[Issue], warnings: list[Issue]) -> None:
    if py_manifest.version and py_manifest.version != manifest.version:
        errors.append(
            Issue(
                code="CROSS_VERSION_MISMATCH",
                message=f"node.py version '{py_manifest.version}' != manifest.version '{manifest.version}'",
            )
        )
    if py_manifest.author and py_manifest.author not in (manifest.author.github, manifest.author.name):
        warnings.append(
            Issue(
                code="CROSS_AUTHOR_MISMATCH",
                message=f"node.py author '{py_manifest.author}' does not match manifest author",
            )
        )


def _check_parse(source: str, errors: list[Issue]) -> str:
    result = parse_source(source)
    for issue in result.issues:
        if issue.severity == "error" and issue.code in _FATAL_PARSE_CODES:
            errors.append(Issue(code="PARSE_FAILED", message=issue.message))
    return result.mode


def _check_examples(source: str, errors: list[Issue]) -> None:
    if extract_example_inputs(source) is None:
        errors.append(Issue(code="EXAMPLES_REQUIRED", message="example_inputs is required and must be a literal list"))
    if extract_example_settings(source) is None:
        errors.append(
            Issue(code="EXAMPLES_REQUIRED", message="example_settings is required and must be a literal dict")
        )


def _check_dependencies(source: str, errors: list[Issue]) -> None:
    manifest = extract_manifest(source)
    deps = manifest.environment.dependencies
    if deps and manifest.environment.kind != "kernel":
        errors.append(Issue(code="DEPENDENCY_NOT_ALLOWED", message='dependencies require environment="kernel"'))
    for dep in deps:
        if not _valid_dependency_spec(dep):
            errors.append(Issue(code="INVALID_DEPENDENCY_SPEC", message=f"invalid dependency spec: {dep!r}"))


def _check_security(source: str, errors: list[Issue]) -> ScanReport:
    scan = scan_source(source)
    deny = [f for f in scan.findings if f.severity == "deny"]
    if deny:
        rule_ids = "; ".join(sorted({f.rule_id for f in deny}))
        errors.append(Issue(code="SECURITY_DENIED", message=f"security scan denied ({rule_ids})"))
    return scan


def _check_media(files: _BundleFiles, errors: list[Issue]) -> None:
    if files.icon is not None:
        _check_png(files.icon, "icon.png", ICON_DIM_MAX, errors)
    for shot in files.screenshots:
        _check_png(shot, f"screenshots/{shot.name}", SCREENSHOT_DIM_MAX, errors)


def _check_png(path: Path, label: str, dim_max: int, errors: list[Issue]) -> None:
    data = path.read_bytes()
    if _looks_like_svg(data):
        errors.append(Issue(code="SVG_FORBIDDEN", message=f"{label} contains SVG/XML markup, not PNG"))
        return
    dims = png_dimensions(data)
    if dims is None:
        errors.append(
            Issue(code="INVALID_PNG", message=f"{label} is not a valid PNG (looks like {sniff_image_format(data)})")
        )
        return
    width, height = dims
    if width > dim_max or height > dim_max:
        errors.append(Issue(code="MEDIA_DIMENSIONS", message=f"{label} is {width}x{height} (max {dim_max}x{dim_max})"))


def validate_bundle(
    folder: Path,
    *,
    index: CommunityIndex | None = None,
    blocklist: dict | None = None,
    categories: list[str] | None = None,
    pr_author: str | None = None,
) -> ValidationReport:
    """Validate a single community-node bundle folder.

    ``index`` enables version/authorization rules for updates; ``blocklist`` and
    ``categories`` come from ``registry/*.json``; ``pr_author`` is the PR opener's
    GitHub login when validating a pull request.
    """
    folder = Path(folder)
    errors: list[Issue] = []
    warnings: list[Issue] = []

    if not folder.is_dir():
        return ValidationReport(ok=False, errors=[Issue(code="NODE_PY_MISSING", message=f"{folder} is not a folder")])

    files = _check_folder_contract(folder, errors)

    source: str | None = None
    if files.node_py is not None:
        source = _read_text(files.node_py, "NODE_PY_MISSING", errors)

    manifest: CommunityManifest | None = None
    if files.manifest_file is not None:
        manifest = _parse_manifest(files.manifest_file, errors)

    py_manifest = extract_manifest(source) if source is not None else None

    if manifest is not None:
        py_node_name = py_manifest.node_name if py_manifest is not None else None
        _check_identity(folder, manifest, py_node_name, blocklist, categories, errors, warnings)
        _check_version_rules(manifest, index, pr_author, errors)
        if py_manifest is not None:
            _check_cross(py_manifest, manifest, errors, warnings)

    parse_mode: str | None = None
    scan: ScanReport | None = None
    capabilities: list[str] = []
    if source is not None:
        parse_mode = _check_parse(source, errors)
        _check_examples(source, errors)
        _check_dependencies(source, errors)
        scan = _check_security(source, errors)
        capabilities = list(scan.capabilities)

    _check_media(files, errors)

    return ValidationReport(
        ok=not errors,
        errors=errors,
        warnings=warnings,
        scan=scan,
        parse_mode=parse_mode,
        manifest=manifest,
        capabilities=capabilities,
    )
