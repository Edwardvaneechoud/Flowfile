"""Index-build tests: deterministic pins, timestamp carry-over, blocklist merge,
registry metadata, and AST-derived behavioral fields.
"""

import hashlib
import json
from pathlib import Path

from flowfile_core.flowfile.community_nodes.index_build import build_index

from .conftest import VALID_ID, copy_valid_bundle

GEN = "2026-01-01T00:00:00Z"


def make_repo(
    tmp_path: Path,
    *,
    categories: list[str] | None = None,
    blocklist: dict | None = None,
    config: dict | None = None,
    node_id: str = VALID_ID,
) -> Path:
    repo = tmp_path / "repo"
    copy_valid_bundle(repo / "nodes", name=node_id)
    if categories is not None or blocklist is not None or config is not None:
        (repo / "registry").mkdir(parents=True, exist_ok=True)
    if categories is not None:
        (repo / "registry" / "categories.json").write_text(json.dumps(categories))
    if blocklist is not None:
        (repo / "registry" / "blocklist.json").write_text(json.dumps(blocklist))
    if config is not None:
        (repo / "registry" / "config.json").write_text(json.dumps(config))
    return repo


def bump_version(repo: Path, node_id: str, version: str) -> None:
    node_dir = repo / "nodes" / node_id
    manifest = json.loads((node_dir / "manifest.json").read_text())
    manifest["version"] = version
    (node_dir / "manifest.json").write_text(json.dumps(manifest))
    src = (node_dir / "node.py").read_text()
    (node_dir / "node.py").write_text(src.replace('version: str = "1.0.0"', f'version: str = "{version}"'))


def test_deterministic_double_build(tmp_path):
    repo = make_repo(tmp_path, categories=["Text"], config={"repo": "x/y", "min_supported_app_version": "0.1.0"})
    a = build_index(repo, previous=None, commit="c0", generated_at=GEN)
    b = build_index(repo, previous=None, commit="c0", generated_at=GEN)
    assert a.model_dump() == b.model_dump()


def test_pin_correctness(tmp_path):
    repo = make_repo(tmp_path)
    index = build_index(repo, previous=None, commit="c0", generated_at=GEN)
    entry = index.entry(VALID_ID)
    assert entry is not None
    node_dir = repo / "nodes" / VALID_ID

    def sha(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    assert entry.artifacts.node.sha256 == sha(node_dir / "node.py")
    assert entry.artifacts.node.size == (node_dir / "node.py").stat().st_size
    assert entry.artifacts.node.path == f"nodes/{VALID_ID}/node.py"
    assert entry.artifacts.manifest.sha256 == sha(node_dir / "manifest.json")
    assert entry.artifacts.icon is not None and entry.artifacts.icon.sha256 == sha(node_dir / "icon.png")
    assert entry.artifacts.readme is not None and entry.artifacts.readme.sha256 == sha(node_dir / "README.md")
    assert len(entry.artifacts.screenshots) == 1
    shot = entry.artifacts.screenshots[0]
    assert shot.path == f"nodes/{VALID_ID}/screenshots/form.png"
    assert shot.sha256 == sha(node_dir / "screenshots" / "form.png")


def test_behavioral_fields_from_ast(tmp_path):
    repo = make_repo(tmp_path)
    entry = build_index(repo, previous=None, commit="c0", generated_at=GEN).entry(VALID_ID)
    assert entry.environment == "local"
    assert entry.dependencies == []
    assert entry.number_of_inputs == 1
    assert entry.number_of_outputs == 1
    assert entry.designer_editable is True
    assert entry.risk_flags == []
    assert entry.palette_category == "Text"
    assert entry.tags == ["text", "cleaning"]


def test_registry_and_commit(tmp_path):
    repo = make_repo(
        tmp_path,
        categories=["Text", "Fun"],
        config={"repo": "edwardvaneechoud/flowfile-community-nodes", "min_supported_app_version": "0.2.0"},
    )
    index = build_index(repo, previous=None, commit="deadbeef", generated_at=GEN)
    assert index.registry.commit == "deadbeef"
    assert index.registry.repo == "edwardvaneechoud/flowfile-community-nodes"
    assert index.registry.min_supported_app_version == "0.2.0"
    assert index.categories == ["Text", "Fun"]


def test_timestamp_carryover_same_version(tmp_path):
    repo = make_repo(tmp_path)
    first = build_index(repo, previous=None, commit="c0", generated_at=GEN)
    later = build_index(repo, previous=first, commit="c1", generated_at="2026-05-05T00:00:00Z")
    entry = later.entry(VALID_ID)
    assert entry.published_at == GEN
    assert entry.updated_at == GEN  # version unchanged, so updated_at carries


def test_timestamp_bumps_on_version_change(tmp_path):
    repo = make_repo(tmp_path)
    first = build_index(repo, previous=None, commit="c0", generated_at=GEN)
    bump_version(repo, VALID_ID, "1.1.0")
    later = build_index(repo, previous=first, commit="c1", generated_at="2026-05-05T00:00:00Z")
    entry = later.entry(VALID_ID)
    assert entry.published_at == GEN  # first-seen date carries
    assert entry.updated_at == "2026-05-05T00:00:00Z"  # version changed -> new updated_at


def test_blocklist_merge(tmp_path):
    blocklist = {
        "blocked": [{"id": VALID_ID, "reason": "malware"}],
        "yanked": [{"id": "other", "versions": ["1.0.0"], "severity": "security"}],
    }
    repo = make_repo(tmp_path, blocklist=blocklist)
    index = build_index(repo, previous=None, commit="c0", generated_at=GEN)
    assert [n.id for n in index.nodes] == []  # blocked id excluded from nodes
    assert [b.id for b in index.blocked] == [VALID_ID]
    assert [y.id for y in index.yanked] == ["other"]


def test_empty_repo(tmp_path):
    repo = tmp_path / "empty"
    (repo / "nodes").mkdir(parents=True)
    index = build_index(repo, previous=None, commit="c0", generated_at=GEN)
    assert index.nodes == []
    assert index.registry.commit == "c0"


def test_nodes_sorted_by_id(tmp_path):
    repo = tmp_path / "repo"
    copy_valid_bundle(repo / "nodes", name="zzz_node")
    copy_valid_bundle(repo / "nodes", name="aaa_node")
    # rename each folder's manifest id to match its dir so both are valid entries
    for node_id in ("zzz_node", "aaa_node"):
        node_dir = repo / "nodes" / node_id
        manifest = json.loads((node_dir / "manifest.json").read_text())
        manifest["id"] = node_id
        (node_dir / "manifest.json").write_text(json.dumps(manifest))
    index = build_index(repo, previous=None, commit="c0", generated_at=GEN)
    assert [n.id for n in index.nodes] == ["aaa_node", "zzz_node"]
