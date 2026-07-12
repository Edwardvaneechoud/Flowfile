"""Publish-bundle export for the in-app Node Designer.

``completeness_check`` runs the publish preconditions and reuses ``validation``
helpers so the local, pre-download check equals the community-repo CI check where
they overlap. ``build_publish_files`` assembles the ``nodes/<id>/`` file set both
publish paths share: the verbatim on-disk ``node.py``, a ``manifest.json`` rendered
from the real ``CommunityManifest`` model (so it can never drift from the
validator), the PNG icon (only when the node ships one), screenshots, and the
author's README (or a TODO stub when none was written in the Publish form).
``build_bundle`` wraps that set plus a ``HOW_TO_PUBLISH.md`` (zip-only, never
part of the file set so it can't trip the community CI's scope check) into a
downloadable zip; the in-app PR path commits ``build_publish_files`` directly.

Warnings (default icon, no screenshots) never block export; only errors do. The
route classifies via ``WARNING_CODES``.
"""

import io
import zipfile
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from flowfile_core.flowfile.community_nodes import validation
from flowfile_core.flowfile.community_nodes.models import (
    ALLOWED_LICENSES,
    GITHUB_LOGIN_RE,
    NODE_ID_RE,
    NODE_NAME_RE,
    SEMVER_RE,
    CommunityAuthor,
    CommunityManifest,
    get_app_version,
    parse_semver,
    slugify_node_name,
)
from flowfile_core.flowfile.community_nodes.receipts import community_icon_override
from flowfile_core.flowfile.community_nodes.security_scan import scan_source
from flowfile_core.flowfile.community_nodes.validation import Issue
from flowfile_core.flowfile.node_designer.parsing import (
    extract_example_inputs,
    extract_example_settings,
    extract_manifest,
)
from flowfile_core.flowfile.user_defined.registry import CustomNodeExecError, LoadedNode, registry
from shared import storage

DEFAULT_NODE_ICON = "user-defined-icon.png"
DEFAULT_CATEGORY = "Utilities"
GITHUB_PLACEHOLDER = "your-github-username"
FALLBACK_MIN_VERSION = "0.1.0"
MIN_DESCRIPTION_LEN = 10
CHANGELOG_MAX = 1000  # mirrors CommunityManifest.changelog max_length

# PNG twins of the standard node glyphs the designer's icon picker offers (kept in
# lockstep with STANDARD_NODE_ICONS in the frontend's designer/utils.ts). A picked
# glyph keeps its .svg name and renders in-app as SVG; the store requires PNG, so we
# map it here and ship the twin — no PNG is bundled in the frontend.
STANDARD_ICONS_DIR = Path(__file__).parent / "standard_icons"


@lru_cache(maxsize=1)
def _standard_icon_names() -> frozenset[str]:
    if not STANDARD_ICONS_DIR.is_dir():
        return frozenset()
    return frozenset(p.name for p in STANDARD_ICONS_DIR.iterdir() if p.suffix.lower() == ".png")


def _standard_icon_path(node_icon: str) -> Path | None:
    """Bundled PNG twin for a standard glyph, or None. Resolves by base name so a
    picked ``.svg`` maps to its ``.png``; membership in the bundled set (bare
    filenames) both selects and guards against path traversal."""
    name = Path(node_icon).stem + ".png"
    return STANDARD_ICONS_DIR / name if name in _standard_icon_names() else None

# Codes the route/frontend render amber (advisory) instead of red (blocking).
WARNING_CODES = frozenset({"ICON_DEFAULT", "ICON_MISSING", "NO_SCREENSHOTS", "README_STUB"})


def _screenshots_dir(entry: LoadedNode) -> Path:
    return storage.user_defined_nodes_screenshots / Path(entry.file_name).stem


def png_screenshots(entry: LoadedNode) -> list[Path]:
    directory = _screenshots_dir(entry)
    if not directory.exists():
        return []
    return sorted(p for p in directory.iterdir() if p.is_file() and p.suffix.lower() == ".png")


def readme_prep_path(file_stem: str) -> Path:
    """The README sidecar shares the per-node publish-prep dir with the screenshots."""
    return storage.user_defined_nodes_screenshots / file_stem / "README.md"


def load_prep_readme(file_stem: str) -> str:
    try:
        return readme_prep_path(file_stem).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def save_prep_readme(file_stem: str, text: str) -> None:
    path = readme_prep_path(file_stem)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not text.strip():
        # Empty tombstone, not unlink: a deliberate clear must not resurrect via the registry fallback.
        path.write_text("", encoding="utf-8")
        return
    path.write_text(text if text.endswith("\n") else text + "\n", encoding="utf-8")


def _resolve_png_icon(node_icon: str | None, node_id: str) -> bytes | None:
    """Bytes of the node's PNG icon, or None for the default / non-PNG / missing.

    A standard glyph (picked in the designer, kept as its ``.svg`` name) resolves to
    its bundled PNG twin; an uploaded custom PNG resolves from disk. ``node_id`` (the
    node's file stem) resolves installed community nodes, whose icon file is
    namespaced on disk while ``node_icon`` keeps the original name.
    """
    if not node_icon or node_icon == DEFAULT_NODE_ICON:
        return None
    path = _standard_icon_path(node_icon)
    if path is None:
        if not node_icon.lower().endswith(".png"):
            return None
        path = storage.user_defined_nodes_icons / (community_icon_override(node_id) or node_icon)
        if not path.is_file():
            return None
    data = path.read_bytes()
    if validation.png_dimensions(data) is None:
        return None
    return data


def completeness_check(
    entry: LoadedNode,
    *,
    license: str | None,
    repository: str,
    description: str = "",
    readme: str = "",
    changelog: str = "",
) -> list[Issue]:
    """Publish preconditions for a designed node. Errors block export, warnings don't."""
    issues: list[Issue] = []
    manifest = extract_manifest(entry.source_text)

    if entry.error:
        issues.append(Issue(code="NODE_UNHEALTHY", message=f"Fix the node before publishing: {entry.error}"))
    else:
        # Publishing is an explicit author action — exec-validating the module
        # here keeps the gate as strong as when scanning exec'd every file.
        try:
            registry.ensure_class(entry)
        except CustomNodeExecError as e:
            issues.append(Issue(code="NODE_UNHEALTHY", message=f"Fix the node before publishing: {e}"))

    node_name = (manifest.node_name or "").strip()
    if not node_name:
        issues.append(Issue(code="NODE_NAME_INVALID", message="Give the node a name before publishing"))
    elif not NODE_NAME_RE.match(node_name) or not NODE_ID_RE.match(slugify_node_name(node_name)):
        issues.append(
            Issue(
                code="NODE_NAME_INVALID",
                message=f"Node name '{node_name}' cannot be a community id (letters, digits, spaces; 3-50 chars)",
            )
        )

    if not (manifest.author or "").strip():
        issues.append(Issue(code="AUTHOR_MISSING", message="Set an author in the Publishing section"))

    if not SEMVER_RE.match(manifest.version or ""):
        issues.append(
            Issue(code="VERSION_INVALID", message="Set a semver version (e.g. 1.0.0) in the Publishing section")
        )

    if len((manifest.intro or "").strip()) < MIN_DESCRIPTION_LEN:
        issues.append(
            Issue(
                code="DESCRIPTION_MISSING",
                message=f"Add an intro of at least {MIN_DESCRIPTION_LEN} characters (used as the description)",
            )
        )

    # A typed-but-too-short override would otherwise fail CommunityManifest's
    # min_length inside build_bundle (an opaque 500 instead of a checklist row).
    if description.strip() and len(description.strip()) < MIN_DESCRIPTION_LEN:
        issues.append(
            Issue(
                code="DESCRIPTION_TOO_SHORT",
                message=f"The description override must be at least {MIN_DESCRIPTION_LEN} characters (or left empty)",
            )
        )

    scan = scan_source(entry.source_text or "")
    if not scan.passed:
        rule_ids = sorted({finding.rule_id for finding in scan.findings if finding.severity == "deny"})
        issues.append(
            Issue(
                code="SECURITY_DENIED",
                message="The security scan community CI runs rejects this code: " + ", ".join(rule_ids),
            )
        )

    if not license or license not in ALLOWED_LICENSES:
        issues.append(Issue(code="LICENSE_INVALID", message=f"Choose a license: {', '.join(ALLOWED_LICENSES)}"))

    # A too-long changelog would otherwise fail CommunityManifest's max_length
    # inside build_publish_files (an opaque 500 instead of a checklist row).
    if len(changelog.strip()) > CHANGELOG_MAX:
        issues.append(
            Issue(
                code="CHANGELOG_TOO_LONG",
                message=f"The changelog must be at most {CHANGELOG_MAX} characters",
            )
        )

    readme_size = len(readme.encode("utf-8"))
    if readme_size > validation.README_MAX:
        issues.append(
            Issue(
                code="README_TOO_LARGE",
                message=f"README is {readme_size} bytes (max {validation.README_MAX}) — shorten it",
            )
        )
    elif not readme.strip():
        issues.append(
            Issue(
                code="README_STUB",
                message="No README written — a TODO stub ships instead; consider writing one here",
            )
        )

    _check_icon(manifest.node_icon, entry.file_path.stem, issues)

    # Community CI hard-requires persisted examples (it dry-runs them), so blocking
    # here spares authors a guaranteed red PR check.
    if (
        extract_example_inputs(entry.source_text or "") is None
        or extract_example_settings(entry.source_text or "") is None
    ):
        issues.append(
            Issue(
                code="EXAMPLES_MISSING",
                message='Add sample data in the Test tab and enable "Save test setup with node" — '
                "community CI runs your example inputs",
            )
        )

    if not png_screenshots(entry):
        issues.append(Issue(code="NO_SCREENSHOTS", message="Consider adding a PNG screenshot for the browse preview"))

    return issues


def _check_icon(node_icon: str | None, node_id: str, issues: list[Issue]) -> None:
    icon = node_icon or DEFAULT_NODE_ICON
    if icon == DEFAULT_NODE_ICON:
        issues.append(Issue(code="ICON_DEFAULT", message="Using the default icon — consider uploading a PNG icon"))
        return
    standard = _standard_icon_path(icon)
    if standard is not None:
        return  # standard glyph → ships its bundled PNG twin, always valid
    if not icon.lower().endswith(".png"):
        issues.append(
            Issue(
                code="ICON_NOT_PNG",
                message=f"Community icons must be PNG — '{icon}' is not. Upload a PNG icon in the designer.",
            )
        )
        return
    path = storage.user_defined_nodes_icons / (community_icon_override(node_id) or icon)
    if not path.is_file():
        issues.append(Issue(code="ICON_MISSING", message=f"Icon file '{icon}' is missing — no icon will be shipped"))
        return
    data = path.read_bytes()
    dims = None if validation._looks_like_svg(data) else validation.png_dimensions(data)
    if dims is None:
        detected = validation.sniff_image_format(data)
        issues.append(
            Issue(
                code="ICON_NOT_PNG",
                message=f"'{icon}' is named .png but its contents are {detected} — "
                "re-save it as a real PNG and upload it again",
            )
        )
        return
    width, height = dims
    if width > validation.ICON_DIM_MAX or height > validation.ICON_DIM_MAX:
        issues.append(
            Issue(
                code="ICON_NOT_PNG",
                message=f"Icon is {width}x{height} (max {validation.ICON_DIM_MAX}x{validation.ICON_DIM_MAX})",
            )
        )
    elif len(data) > validation.ICON_MAX:
        issues.append(Issue(code="ICON_NOT_PNG", message=f"Icon is {len(data)} bytes (max {validation.ICON_MAX})"))


def node_id_for(entry: LoadedNode) -> str:
    """The community node id (folder name / manifest.id) this bundle exports under."""
    return slugify_node_name((extract_manifest(entry.source_text).node_name or entry.file_path.stem).strip())


def _author_fields(raw_author: str) -> tuple[str, str, bool]:
    """(github, name, is_placeholder). A non-login author becomes name only + placeholder github."""
    author = (raw_author or "").strip()
    if author and len(author) <= 39 and GITHUB_LOGIN_RE.match(author):
        return author, author, False
    return GITHUB_PLACEHOLDER, author, True


def _min_flowfile_version() -> str:
    app_version = get_app_version()
    return app_version if parse_semver(app_version) is not None else FALLBACK_MIN_VERSION


@dataclass
class PublishFiles:
    """The ``nodes/<id>/**`` file set both publish paths share (no HOW_TO_PUBLISH.md).

    ``used_placeholder`` is True only on the bundle-download path when the node's
    author isn't a GitHub login — the in-app PR path supplies ``author_github`` and
    never falls back to the placeholder.
    """

    node_id: str
    node_name: str
    version: str
    files: list[tuple[str, bytes]]
    used_placeholder: bool


def build_publish_files(
    entry: LoadedNode,
    *,
    license: str,
    repository: str,
    description: str,
    category: str,
    screenshots: list[Path],
    author_github: str | None = None,
    readme: str = "",
    changelog: str = "",
) -> PublishFiles:
    """Assemble the ``nodes/<id>/`` file set for a publishable node.

    ``author_github=None`` keeps the bundle-download behaviour (a non-login node
    author becomes name-only + a placeholder github). ``author_github="<login>"``
    (the PR path) pins ``author.github`` to the token's login — CI requires the PR
    author to match the manifest — with the node's raw author kept as the name.
    """
    source_bytes = entry.file_path.read_bytes()
    meta = extract_manifest(source_bytes.decode("utf-8", errors="replace"))
    node_name = (meta.node_name or entry.file_path.stem).strip()
    node_id = slugify_node_name(node_name)

    if author_github is None:
        github, author_name, used_placeholder = _author_fields(meta.author)
    else:
        github, author_name, used_placeholder = author_github, (meta.author or "").strip(), False

    intro = (meta.intro or "").strip()
    resolved_description = (description or intro).strip()[:300]
    if len(resolved_description) < MIN_DESCRIPTION_LEN:
        resolved_description = intro[:300] if len(intro) >= MIN_DESCRIPTION_LEN else "A Flowfile community node."

    icon_bytes = _resolve_png_icon(meta.node_icon, entry.file_path.stem)

    shot_files: list[tuple[str, bytes]] = []
    for path in screenshots:
        try:
            data = path.read_bytes()
        except OSError:
            continue
        if validation.png_dimensions(data) is None:
            continue
        shot_files.append((f"{len(shot_files) + 1}.png", data))
        if len(shot_files) >= validation.SCREENSHOT_COUNT_MAX:
            break

    version = meta.version or "0.0.0"
    manifest = CommunityManifest(
        id=node_id,
        node_name=node_name,
        version=version,
        description=resolved_description,
        author=CommunityAuthor(github=github, name=author_name),
        license=license,
        category=category or DEFAULT_CATEGORY,
        tags=list(meta.tags)[:8],
        min_flowfile_version=_min_flowfile_version(),
        icon="icon.png" if icon_bytes is not None else None,
        screenshots=[f"screenshots/{name}" for name, _ in shot_files],
        repository=repository or "",
        changelog=changelog.strip(),
    )
    manifest_json = manifest.model_dump_json(indent=2, exclude_none=True) + "\n"

    readme_text = readme.strip()
    readme_content = readme_text + "\n" if readme_text else _readme_stub(node_name, resolved_description)

    base = f"nodes/{node_id}"
    files: list[tuple[str, bytes]] = [
        (f"{base}/node.py", source_bytes),
        (f"{base}/manifest.json", manifest_json.encode("utf-8")),
        (f"{base}/README.md", readme_content.encode("utf-8")),
    ]
    if icon_bytes is not None:
        files.append((f"{base}/icon.png", icon_bytes))
    files.extend((f"{base}/screenshots/{name}", data) for name, data in shot_files)

    return PublishFiles(
        node_id=node_id, node_name=node_name, version=version, files=files, used_placeholder=used_placeholder
    )


def build_bundle(
    entry: LoadedNode,
    *,
    license: str,
    repository: str,
    description: str,
    category: str,
    screenshots: list[Path],
    readme: str = "",
    changelog: str = "",
) -> bytes:
    """Assemble a ready-to-PR ``nodes/<id>/`` folder as an in-memory zip (bundle-download path)."""
    pf = build_publish_files(
        entry,
        license=license,
        repository=repository,
        description=description,
        category=category,
        screenshots=screenshots,
        readme=readme,
        changelog=changelog,
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("HOW_TO_PUBLISH.md", _how_to_publish(pf.node_id, pf.used_placeholder))
        for name, data in pf.files:
            zf.writestr(name, data)
    return buffer.getvalue()


def _how_to_publish(node_id: str, github_placeholder: bool) -> str:
    todo = ""
    if github_placeholder:
        todo = (
            f"\n> **TODO:** `manifest.json` lists `author.github` as `{GITHUB_PLACEHOLDER}` because the node's author\n"
            "> field isn't a valid GitHub username. Replace it with your GitHub login before opening the PR —\n"
            "> the PR author must match `author.github`.\n"
        )
    return (
        f"# Publishing `{node_id}` to the Flowfile community registry\n\n"
        "This bundle contains everything the registry needs. To publish:\n\n"
        "1. **Fork** [edwardvaneechoud/flowfile-community-nodes](https://github.com/edwardvaneechoud/flowfile-community-nodes).\n"
        f"2. **Copy** the `nodes/{node_id}/` folder from this zip into the `nodes/` directory of your fork.\n"
        "3. **Commit and push**, then **open a pull request** against `main`.\n"
        "4. **CI checks** run automatically: folder contract, manifest schema, an AST security scan, a dry-run of\n"
        "   your `example_inputs`, and PNG media checks. Fix anything the checks flag.\n"
        "5. A **maintainer reviews** the PR. Merging it publishes your node — it appears in the in-app\n"
        "   Community catalog on the next index build.\n"
        f"{todo}"
        "\nEvery file here is generated from your saved node. You can edit `README.md` and `manifest.json`\n"
        "(description, tags, category) before submitting.\n"
    )


def _readme_stub(node_name: str, description: str) -> str:
    return (
        f"# {node_name}\n\n"
        f"{description}\n\n"
        "## What it does\n\n"
        "TODO: Describe what this node does and when to reach for it.\n\n"
        "## Inputs\n\n"
        "TODO: Describe the expected input table(s) and columns.\n\n"
        "## Settings\n\n"
        "TODO: Describe each setting and how it changes the output.\n"
    )


# Path of the community repo's PR template that build_pr_body mirrors; if that
# template changes, this body must follow (review catches the drift via the pointer).
SCAFFOLD_PR_TEMPLATE = ".github/PULL_REQUEST_TEMPLATE.md"


def pr_branch_name(node_id: str, version: str) -> str:
    """Deterministic per-(node, version) branch, force-updated on re-publish."""
    return f"node/{node_id}-v{version}"


def pr_title(node_id: str, node_name: str, version: str, is_update: bool) -> str:
    verb = "Update" if is_update else "Add"
    return f"{verb} {node_name} ({node_id}) v{version}"


def build_pr_body(node_id: str, version: str, is_update: bool, description: str) -> str:
    """PR body mirroring the community repo's PULL_REQUEST_TEMPLATE.md.

    Mechanical items (test setup, author == login, single-folder scope, PNG media,
    version bump for updates) are pre-checked because the in-app flow guarantees
    them. The two honesty items are pre-checked on the strength of the explicit
    confirmation checkbox the author ticks in the Publish modal.
    """
    kind = f"Update — bumped to v{version}" if is_update else "New node"
    update_item = "[x]" if is_update else "[ ]"
    return (
        f"<!-- Opened from the Flowfile Node Designer's Publish flow. Mirrors {SCAFFOLD_PR_TEMPLATE}\n"
        "     in edwardvaneechoud/flowfile-community-nodes — keep the two in sync. -->\n\n"
        "## Node\n\n"
        f"- **Node id:** `nodes/{node_id}/`\n"
        f"- **New node** or **update** (bump version): {kind}\n"
        f"- **What does it do?** {description}\n\n"
        "## Author checklist\n\n"
        '- [x] I built and **dry-ran this node in the Node Designer** with **"Save test setup with '
        'node" ON**, so `node.py` carries working `example_inputs` + `example_settings`.\n'
        "- [x] The manifest `author.github` is **my** GitHub login (this PR's author), or I am a\n"
        "      listed maintainer of this node.\n"
        f"- [x] This PR adds/edits **only** `nodes/{node_id}/` — no changes to `index.json`,\n"
        "      `popularity.json`, `registry/`, `scripts/`, or `.github/`.\n"
        f"- [x] I added at least one **PNG** screenshot under `nodes/{node_id}/screenshots/` (no SVG).\n"
        # The two honesty items are backed by the author's explicit confirmation checkbox in the modal.
        "- [x] The node makes **no hidden network calls** and does nothing beyond what its\n"
        "      description and the flagged capabilities say.\n"
        f"- {update_item} For an update: I **bumped the semver `version`** and updated the `changelog`.\n"
        "- [x] I understand community nodes are **not sandboxed** — installed nodes run with the same\n"
        "      access as my own code — and my node is honest about what it does.\n\n"
        "## For the reviewer\n\n"
        "<!-- Point the reviewer at anything worth a close look: unusual imports, why a capability is\n"
        "     needed (network / filesystem / subprocess / secrets), non-obvious logic. -->\n"
    )
