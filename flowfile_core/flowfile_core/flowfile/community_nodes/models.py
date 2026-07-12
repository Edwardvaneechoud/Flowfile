"""Wire contracts for the community node registry.

These models are the single source of truth for three consumers: the community
repo's CI (validation + index build), the in-app browse/install client, and the
publish-bundle exporter. The committed ``manifest.schema.json`` in the community
repo is generated from ``CommunityManifest`` (``cli schema``) so it cannot drift.
"""

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

MANIFEST_SCHEMA_VERSION = 1
INDEX_SCHEMA_VERSION = 1

# Folder name == manifest.id == slug of node_name (CustomNodeBase.item); immutable.
NODE_ID_RE = re.compile(r"^[a-z][a-z0-9_]{2,49}$")
NODE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_ ]{2,49}$")
SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
# Lookahead-free (pydantic v2 compiles Field patterns with Rust regex): alphanumerics
# with single interior hyphens; the 39-char GitHub cap is enforced via max_length.
GITHUB_LOGIN_RE = re.compile(r"^[A-Za-z0-9](?:-?[A-Za-z0-9])*$")
SCREENSHOT_PATH_RE = re.compile(r"^screenshots/[a-z0-9_-]{1,40}\.png$")

ALLOWED_LICENSES = ("MIT", "Apache-2.0", "BSD-3-Clause", "BSD-2-Clause", "MPL-2.0", "Unlicense", "CC0-1.0")


def slugify_node_name(node_name: str) -> str:
    """Canonical node-type slug (CustomNodeBase.item); flows persist it as node_type."""
    from shared.node_designer.custom_node import node_key_for

    return node_key_for(node_name)


def parse_semver(value: str) -> tuple[int, int, int] | None:
    match = SEMVER_RE.match(value or "")
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def semver_gt(a: str, b: str) -> bool:
    """True when a > b; non-semver strings fall back to inequality-as-newer."""
    parsed_a, parsed_b = parse_semver(a), parse_semver(b)
    if parsed_a is None or parsed_b is None:
        return a != b
    return parsed_a > parsed_b


def is_version_compatible(min_flowfile_version: str, app_version: str) -> bool:
    """False only when both versions parse and the minimum exceeds the app."""
    minimum, current = parse_semver(min_flowfile_version), parse_semver(app_version)
    if minimum is None or current is None:
        return True
    return current >= minimum


def get_app_version() -> str:
    try:
        from importlib.metadata import version

        return version("Flowfile")
    except Exception:
        return ""


class CommunityAuthor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    github: str = Field(pattern=GITHUB_LOGIN_RE.pattern, max_length=39)
    name: str = Field(default="", max_length=100)
    url: str = ""

    @field_validator("url")
    @classmethod
    def _https_only(cls, v: str) -> str:
        if v and not v.startswith("https://"):
            raise ValueError("author url must be https://")
        return v


class CommunityManifest(BaseModel):
    """``manifest.json`` inside a community node folder: distribution metadata only.

    Runtime/behavioral metadata (environment, io counts, dependencies, examples)
    is AST-extracted from node.py at index build and never trusted from here.
    """

    model_config = ConfigDict(extra="forbid")

    manifest_version: Literal[1] = 1
    id: str = Field(pattern=NODE_ID_RE.pattern)
    node_name: str = Field(pattern=NODE_NAME_RE.pattern)
    version: str = Field(pattern=SEMVER_RE.pattern)
    description: str = Field(min_length=10, max_length=300)
    author: CommunityAuthor
    maintainers: list[str] = []
    license: str
    category: str
    tags: list[str] = Field(default=[], max_length=8)
    min_flowfile_version: str = Field(pattern=SEMVER_RE.pattern)
    icon: Literal["icon.png"] | None = None
    screenshots: list[str] = Field(default=[], max_length=5)
    homepage: str = ""
    repository: str = ""
    changelog: str = Field(default="", max_length=1000)

    @field_validator("maintainers")
    @classmethod
    def _valid_logins(cls, v: list[str]) -> list[str]:
        for login in v:
            if len(login) > 39 or not GITHUB_LOGIN_RE.match(login):
                raise ValueError(f"invalid GitHub login: {login!r}")
        return v

    @field_validator("license")
    @classmethod
    def _allowed_license(cls, v: str) -> str:
        if v not in ALLOWED_LICENSES:
            raise ValueError(f"license must be one of {ALLOWED_LICENSES}")
        return v

    @field_validator("screenshots")
    @classmethod
    def _screenshot_paths(cls, v: list[str]) -> list[str]:
        for path in v:
            if not SCREENSHOT_PATH_RE.match(path):
                raise ValueError(f"screenshot must match {SCREENSHOT_PATH_RE.pattern}: {path!r}")
        return v

    @field_validator("homepage", "repository")
    @classmethod
    def _https_urls(cls, v: str) -> str:
        if v and not v.startswith("https://"):
            raise ValueError("must be an https:// URL")
        return v

    def authorized_logins(self) -> set[str]:
        return {self.author.github, *self.maintainers}


def manifest_json_schema() -> dict:
    return CommunityManifest.model_json_schema()


class PinnedFile(BaseModel):
    """Repo-relative artifact path pinned by content hash; URLs are composed
    client-side from the index's registry.commit so the CDN can change server-side."""

    path: str
    sha256: str
    size: int


class CommunityArtifacts(BaseModel):
    node: PinnedFile
    manifest: PinnedFile
    icon: PinnedFile | None = None
    readme: PinnedFile | None = None
    screenshots: list[PinnedFile] = []


class CommunityIndexEntry(BaseModel):
    id: str
    node_name: str
    version: str
    description: str = ""
    author: CommunityAuthor
    maintainers: list[str] = []
    license: str = ""
    category: str = ""
    palette_category: str = "Custom"
    tags: list[str] = []
    environment: Literal["local", "kernel"] = "local"
    dependencies: list[str] = []
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    min_flowfile_version: str = ""
    designer_editable: bool = True
    risk_flags: list[str] = []
    published_at: str = ""
    updated_at: str = ""
    changelog: str = ""
    path: str = ""
    artifacts: CommunityArtifacts


class YankedEntry(BaseModel):
    id: str
    versions: list[str] = []
    severity: Literal["security", "deprecated"] = "security"
    reason: str = ""
    date: str = ""


class BlockedEntry(BaseModel):
    id: str
    reason: str = ""
    date: str = ""
    advisory_url: str = ""


class RegistryInfo(BaseModel):
    repo: str = ""
    commit: str = ""
    min_supported_app_version: str = ""


class CommunityIndex(BaseModel):
    schema_version: Literal[1] = 1
    generated_at: str = ""
    registry: RegistryInfo = RegistryInfo()
    categories: list[str] = []
    nodes: list[CommunityIndexEntry] = []
    yanked: list[YankedEntry] = []
    blocked: list[BlockedEntry] = []

    def entry(self, node_id: str) -> CommunityIndexEntry | None:
        return next((node for node in self.nodes if node.id == node_id), None)

    def blocked_ids(self) -> set[str]:
        return {entry.id for entry in self.blocked}


class NodePopularity(BaseModel):
    thumbs_up: int = 0
    comments: int = 0
    discussion_number: int | None = None
    discussion_url: str = ""


class PopularityData(BaseModel):
    schema_version: int = 1
    generated_at: str = ""
    repo_stars: int = 0
    nodes: dict[str, NodePopularity] = {}


class InstallRequest(BaseModel):
    node_id: str
    version: str
    acknowledged_risks: bool = False
    # Must superset the capabilities core recomputes from the downloaded source;
    # the consent dialog is advisory, the server-side re-scan is authoritative.
    acknowledged_capabilities: list[str] = []


class ConsentRecord(BaseModel):
    acknowledged: bool = False
    at: str = ""
    by: str = ""
    capabilities: list[str] = []


class InstallReceipt(BaseModel):
    node_id: str
    node_key: str
    file_name: str
    version: str
    sha256: str
    icon_file: str | None = None
    # Registry screenshots/README seeded into the publish-prep dir at install; uninstall removes exactly these.
    screenshot_files: list[str] = []
    readme_file: str | None = None
    installed_at: str = ""
    installed_by: str = ""
    consent: ConsentRecord = ConsentRecord()
    source_commit: str = ""
    index_url: str = ""
    min_flowfile_version: str = ""


class InstalledCommunityNode(BaseModel):
    receipt: InstallReceipt
    node_name: str = ""
    modified_locally: bool = False
    error: str | None = None


class UpdateInfo(BaseModel):
    node_id: str
    installed_version: str
    latest_version: str
    sha256_changed: bool = False
    changelog: str = ""


InstallState = Literal["not_installed", "installed", "update_available", "modified_locally", "incompatible"]
