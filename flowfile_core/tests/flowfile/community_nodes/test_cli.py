"""CLI-level tests: validate-all re-validates the merged tree WITHOUT the PR-time
version-bump rule (it would otherwise fail every node against its own published entry).
"""

import argparse
import json
from pathlib import Path

from flowfile_core.flowfile.community_nodes import cli
from flowfile_core.flowfile.community_nodes.index_build import build_index

from .conftest import VALID_ID, copy_valid_bundle


def _repo_with_index(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    copy_valid_bundle(repo / "nodes", name=VALID_ID)
    (repo / "registry").mkdir(parents=True, exist_ok=True)
    (repo / "registry" / "categories.json").write_text(json.dumps(["Text"]))
    (repo / "registry" / "blocklist.json").write_text(json.dumps({"schema_version": 1, "blocked": [], "yanked": []}))
    # Index already lists the node at its current version — the merged-state baseline.
    index = build_index(repo, previous=None, commit="c0", generated_at="2026-01-01T00:00:00Z")
    (repo / "index.json").write_text(index.model_dump_json())
    return repo


def test_validate_all_passes_on_merged_tree_at_same_version(tmp_path):
    repo = _repo_with_index(tmp_path)
    args = argparse.Namespace(repo_root=str(repo), pr_author=None, summary=None, quick=True, timeout=120, row_limit=1000)
    assert cli._cmd_validate_all(args) == 0


def test_single_validate_still_enforces_version_bump(tmp_path):
    # The PR-time path keeps requiring a strictly-greater version than what's published.
    repo = _repo_with_index(tmp_path)
    args = argparse.Namespace(
        folder=str(repo / "nodes" / VALID_ID),
        index=str(repo / "index.json"),
        blocklist=str(repo / "registry" / "blocklist.json"),
        categories=str(repo / "registry" / "categories.json"),
        pr_author="edwardvaneechoud",
        summary=None,
    )
    assert cli._cmd_validate(args) == 1
