"""Guard that the frozen core binary ships every frame module the in-core notebook runner imports.

The runner imports ``flowfile_frame`` lazily, on the first plan or push, and the cell namespace reaches the
native-node modules only through lazy imports, so PyInstaller bundles them only when the core build collects
the whole package: ``NOTEBOOK_COLLECTED_PACKAGES`` names it, the core build passes it, and the generated spec
expands it with ``collect_submodules``.

* **L1 coverage**: the core build passes the list and the generated spec collects ``flowfile_frame``.
* **L2 behaviour**: the modules that collection must find exist as submodules of the package.
"""

import ast
import pkgutil
from pathlib import Path
from types import ModuleType

import flowfile_core.notebook
import flowfile_frame

REPO = Path(__file__).resolve().parents[3]
REQUIRED_FRAME_MODULES = (
    "native",
    "gate",
    "run_flow",
    "custom_node",
    "python_script",
    "parameters",
    "notebook",
    "notebook_cells",
    "_fl_namespace",
)


def test_core_build_collects_the_frame_package():
    from build_backends.main import NOTEBOOK_COLLECTED_PACKAGES

    assert NOTEBOOK_COLLECTED_PACKAGES == ["flowfile_frame"]
    source = (REPO / "build_backends" / "build_backends" / "main.py").read_text(encoding="utf-8")
    core_call = source.split('output_name="flowfile_core"', 1)[1].split(")", 1)[0]
    assert "collected_packages=NOTEBOOK_COLLECTED_PACKAGES" in core_call


def test_generated_spec_collects_the_frame_package(tmp_path, monkeypatch):
    from build_backends.main import NOTEBOOK_COLLECTED_PACKAGES, create_spec_file

    monkeypatch.chdir(tmp_path)
    spec_path = create_spec_file(".", "main.py", "probe", ["polars"], NOTEBOOK_COLLECTED_PACKAGES)
    spec = Path(spec_path).read_text(encoding="utf-8")
    hidden = spec.split("hiddenimports=", 1)[1].split("excludes=", 1)[0]
    assert "collected_hiddenimports" in hidden
    assert "for _pkg in ['flowfile_frame']:" in spec


def _frame_modules_the_notebook_package_imports() -> set[str]:
    """Every ``flowfile_frame`` submodule a module of ``flowfile_core.notebook`` imports, lazily or not."""
    names = set()
    for path in Path(flowfile_core.notebook.__file__).parent.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), str(path))):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("flowfile_frame."):
                names.add(node.module.split(".")[1])
            elif isinstance(node, ast.ImportFrom) and node.module == "flowfile_frame":
                names.update(
                    a.name for a in node.names if isinstance(getattr(flowfile_frame, a.name, None), ModuleType)
                )
            elif isinstance(node, ast.Import):
                names.update(a.name.split(".")[1] for a in node.names if a.name.startswith("flowfile_frame."))
    return names


def test_the_collected_package_holds_every_frame_module_the_runner_imports():
    imported = _frame_modules_the_notebook_package_imports()
    assert {"notebook", "notebook_cells"} <= imported
    names = {info.name for info in pkgutil.iter_modules(flowfile_frame.__path__)}
    missing = sorted((imported | set(REQUIRED_FRAME_MODULES)) - names)
    assert not missing, f"flowfile_frame no longer has {missing}; update the packaging gate"
