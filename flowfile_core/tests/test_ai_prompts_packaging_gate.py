"""Guard that the AI prompts ship in the PyInstaller bundle, where a missing datas entry fails only in Flowfile.exe."""

import re
from pathlib import Path

import flowfile_core
from flowfile_core.ai import semantic_draft
from flowfile_core.ai.context import builder
from flowfile_core.ai.local_model import code_build, oneshot

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = Path(flowfile_core.__file__).resolve().parent
PROMPTS_DIR = PACKAGE_ROOT / "ai" / "prompts"


def test_every_loader_reads_the_bundled_dir():
    assert builder._PROMPTS_DIR.resolve() == PROMPTS_DIR
    assert code_build._PROMPT_PATH.parent == PROMPTS_DIR
    assert oneshot._PROMPT_PATH.parent == PROMPTS_DIR
    for name in ("semantic_draft", "semantic_ask", "semantic_suggest"):
        assert semantic_draft.system_prompt(name).strip()


def test_pyinstaller_spec_ships_the_prompts(tmp_path, monkeypatch):
    from build_backends.main import create_spec_file

    monkeypatch.chdir(tmp_path)
    spec_text = Path(create_spec_file(".", "run.py", "probe", [])).read_text(encoding="utf-8")

    datas_expr = spec_text.split("datas=", 1)[1].split("hiddenimports=", 1)[0]
    assert "ai_prompts_datas" in datas_expr, "ai_prompts_datas is not part of the Analysis(datas=...) expression"

    helper = re.search(r"^def get_ai_prompts_datas\(\):\n(?:    .*\n)+", spec_text, re.MULTILINE)
    assert helper, "the generated spec has no get_ai_prompts_datas()"
    namespace: dict = {}
    exec(helper.group(0), namespace)
    monkeypatch.chdir(REPO_ROOT)
    [(source, dest)] = namespace["get_ai_prompts_datas"]()

    assert (REPO_ROOT / source).resolve() == PROMPTS_DIR
    assert Path(dest) == PROMPTS_DIR.relative_to(PACKAGE_ROOT.parent), (
        f"the spec places the prompts at {dest}, not where the AI modules read them"
    )
