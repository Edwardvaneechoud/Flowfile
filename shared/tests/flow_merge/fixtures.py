"""The real flow files the repository tracks that the flow-merge tests read: templates, docs, the frontend and WASM."""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
_PATTERNS = (
    "data/templates/flows/*.yaml",
    "docs/assets/flows/*.yaml",
    "flowfile_frontend/tests/fixtures/complex-flow.yaml",
    "flowfile_wasm/public/**/*.yaml",
)


def flow_files() -> list[Path]:
    paths = sorted({p for pattern in _PATTERNS for p in REPO_ROOT.glob(pattern)})
    return [p for p in paths if isinstance(load(p), dict) and "nodes" in load(p)]


def load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def fixture_id(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))
