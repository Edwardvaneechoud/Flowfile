"""Drift guard for the mirrored LSP wire models.

``kernel_runtime/lsp/models.py`` and ``flowfile_core/lsp/models.py`` are kept
field-for-field identical by hand (core can't import the kernel package, and the
kernel image doesn't ship ``shared``). This test parses both with ``ast`` and
asserts the class/field/annotation/default shapes match, so silent drift fails CI
instead of surfacing as a malformed bridge response. Docstrings, comments, and
whitespace legitimately differ and are ignored.
"""

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
KERNEL_MODELS = _REPO_ROOT / "kernel_runtime" / "kernel_runtime" / "lsp" / "models.py"
CORE_MODELS = _REPO_ROOT / "flowfile_core" / "flowfile_core" / "lsp" / "models.py"


def _model_specs(path: Path) -> dict[str, list[tuple[str, str, str | None]]]:
    """Map each top-level class to its ordered (name, annotation, default) fields."""
    tree = ast.parse(path.read_text())
    specs: dict[str, list[tuple[str, str, str | None]]] = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        fields: list[tuple[str, str, str | None]] = []
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                annotation = ast.unparse(stmt.annotation)
                default = ast.unparse(stmt.value) if stmt.value is not None else None
                fields.append((stmt.target.id, annotation, default))
        specs[node.name] = fields
    return specs


def test_lsp_models_mirror_files_exist():
    assert KERNEL_MODELS.exists(), KERNEL_MODELS
    assert CORE_MODELS.exists(), CORE_MODELS


def test_lsp_models_stay_in_sync():
    kernel = _model_specs(KERNEL_MODELS)
    core = _model_specs(CORE_MODELS)
    assert kernel == core, (
        "kernel_runtime/lsp/models.py and flowfile_core/lsp/models.py have drifted. "
        "Keep the two mirrors field-for-field identical.\n"
        f"kernel: {kernel}\ncore:   {core}"
    )
