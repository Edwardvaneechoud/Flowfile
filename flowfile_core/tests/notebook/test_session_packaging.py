"""Guard that the frozen core binary can host a notebook session.

The session runs ``import flowfile as fl`` inside the PyInstaller core binary (``--notebook-session``).
The frame's native-node modules are reached only through lazy imports, so they ship only when the spec
names them: ``flowfile``, ``flowfile_core.notebook.session_main`` and every ``flowfile_frame`` submodule.

* **L1 coverage**: the generated core spec lists the names and collects ``flowfile_frame``.
* **L2 behaviour**: the modules that collection must find exist as submodules of the package.
"""

import pkgutil
from pathlib import Path

import flowfile_frame

REQUIRED_FRAME_MODULES = (
    "native",
    "gate",
    "run_flow",
    "custom_node",
    "python_script",
    "parameters",
    "notebook",
    "notebook_cells",
)


def _core_spec(tmp_path, monkeypatch) -> str:
    from build_backends.main import NOTEBOOK_COLLECTED_PACKAGES, NOTEBOOK_HIDDEN_IMPORTS, create_spec_file

    monkeypatch.chdir(tmp_path)
    spec = create_spec_file(".", "main.py", "probe", ["polars", *NOTEBOOK_HIDDEN_IMPORTS], NOTEBOOK_COLLECTED_PACKAGES)
    return Path(spec).read_text(encoding="utf-8")


def test_core_build_passes_the_notebook_imports():
    source = (Path(__file__).resolve().parents[3] / "build_backends" / "build_backends" / "main.py").read_text(
        encoding="utf-8"
    )
    core_call = source.split('output_name="flowfile_core"', 1)[1].split(")", 1)[0]
    assert "NOTEBOOK_HIDDEN_IMPORTS" in core_call and "NOTEBOOK_COLLECTED_PACKAGES" in core_call


def test_generated_spec_carries_the_session_modules(tmp_path, monkeypatch):
    spec = _core_spec(tmp_path, monkeypatch)
    hidden = spec.split("hiddenimports=", 1)[1].split("excludes=", 1)[0]
    assert "'flowfile'" in hidden
    assert "'flowfile_core.notebook.session_main'" in hidden
    assert "collected_hiddenimports" in hidden
    assert "for _pkg in ['flowfile_frame']:" in spec


def test_the_collected_package_holds_the_native_node_modules():
    names = {info.name for info in pkgutil.iter_modules(flowfile_frame.__path__)}
    missing = [name for name in REQUIRED_FRAME_MODULES if name not in names]
    assert not missing, f"flowfile_frame no longer has {missing}; update the packaging gate"
