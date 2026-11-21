"""``shared.flow_merge``: importable without core, and the id rewrites a merge needs beyond node ids."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

from shared.flow_merge.relabel import relabel, relabel_comments, relabel_groups

REPO_ROOT = Path(__file__).resolve().parents[3]
FORBIDDEN = {"flowfile_core", "flowfile", "flowfile_frame", "flowfile_worker", "alembic", "sqlalchemy", "fastapi"}


def test_flow_merge_import_is_pure(tmp_path):
    """A git merge driver imports the package once per merged file: it must not start core or touch a database."""
    env = {
        **os.environ,
        "FLOWFILE_STORAGE_DIR": str(tmp_path / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user_data"),
        "FLOWFILE_DB_PATH": str(tmp_path / "purity.db"),
    }
    code = f"""
        import json, sys
        import shared.flow_merge
        from shared.flow_merge import canonical, cli, conflict, diff, edges, integrity, merge  # noqa: F401
        from shared.flow_merge import placement, records, relabel, render, yaml_io  # noqa: F401
        print(json.dumps(sorted(m for m in sys.modules if m.split(".")[0] in {sorted(FORBIDDEN)!r})))
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(REPO_ROOT),
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []
    assert not (tmp_path / "purity.db").exists()
    assert not (tmp_path / "storage").exists()


def _document() -> dict:
    return {
        "nodes": [
            {"id": 1, "type": "manual_input", "group_id": 2, "input_ids": [], "outputs": [3]},
            {"id": 3, "type": "filter", "group_id": 5, "input_ids": [1], "outputs": []},
            {"id": 4, "type": "sort", "group_id": None, "input_ids": [], "outputs": []},
        ],
        "groups": [
            {"id": 2, "name": "outer", "parent_group_id": None},
            {"id": 5, "name": "inner", "parent_group_id": 2},
        ],
        "comments": [{"id": 1, "text": "a"}, {"id": 2, "text": "b"}],
    }


def test_relabel_groups_rewrites_group_and_parent_ids():
    data = _document()
    out = relabel_groups(data, {2: 20, 5: 50})
    assert [(g["id"], g["parent_group_id"]) for g in out["groups"]] == [(20, None), (50, 20)]
    assert [n["group_id"] for n in out["nodes"]] == [20, 50, None]
    assert data["groups"][0]["id"] == 2, "the input is left as it was"


def test_relabel_groups_leaves_node_ids_alone():
    out = relabel_groups(_document(), {1: 10, 3: 30})
    assert [n["id"] for n in out["nodes"]] == [1, 3, 4]
    assert [n["group_id"] for n in out["nodes"]] == [2, 5, None]


def test_relabel_comments_rewrites_comment_ids_only():
    out = relabel_comments(_document(), {1: 7})
    assert [c["id"] for c in out["comments"]] == [7, 2]
    assert [n["id"] for n in out["nodes"]] == [1, 3, 4]


def test_relabel_still_rewrites_node_ids_and_not_groups():
    out = relabel(_document(), {1: 11, 3: 13})
    assert [n["id"] for n in out["nodes"]] == [11, 13, 4]
    assert out["nodes"][1]["input_ids"] == [11]
    assert out["nodes"][0]["outputs"] == [13]
    assert [n["group_id"] for n in out["nodes"]] == [2, 5, None]
