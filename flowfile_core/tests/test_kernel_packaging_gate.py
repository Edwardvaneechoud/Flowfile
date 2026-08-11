"""Guard that the kernel dependency baseline actually ships.

``kernel/matching.py`` can only report a dependency as *missing* when it can
enumerate the image's contents. That inventory used to be re-derived at runtime
from ``kernel_runtime/poetry.lock``, resolved by walking up to the repo root —
so it existed only in a checkout. Every shipped surface (wheel, Docker image,
PyInstaller sidecar) silently lost it and reported "has all packages" for
kernels that had none, and no existing test noticed because they all ran in the
one layout where the bug is invisible.

The gate has three layers:

* **L0 containment** — the data resolves *inside* the installed package. Nothing
  outside the package root is reachable in any shipped layout, so this is the
  layout-independent check.
* **L1 coverage** — every packaging manifest actually carries it.
* **L2 behaviour** — in a tree containing only what those manifests ship, a
  provably-absent package is still reported missing.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

import flowfile_core
from flowfile_core.kernel import flavours, matching
from flowfile_core.kernel.models import ImageFlavour

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = Path(flowfile_core.__file__).resolve().parent

# Every runtime data file kernel matching depends on. Derived from the modules so
# the gate follows the data if it ever moves.
REQUIRED_DATA_FILES = (flavours._MANIFEST_PATH,)


def _relative_paths() -> list[Path]:
    return [path.resolve().relative_to(PACKAGE_ROOT) for path in REQUIRED_DATA_FILES]


class TestContainment:
    def test_data_exists_and_lives_inside_the_package(self):
        for path in REQUIRED_DATA_FILES:
            assert path.is_file(), f"{path} is missing — run 'make kernel_manifest'"
            resolved = path.resolve()
            assert resolved.is_relative_to(PACKAGE_ROOT), (
                f"{resolved} resolves outside the flowfile_core package ({PACKAGE_ROOT}). "
                "No packaging manifest — wheel, sdist, PyInstaller datas or the Dockerfile's "
                "COPY — can place a file outside the package where a shipped install will "
                "find it, so this data is unreachable everywhere except a repo checkout."
            )

    def test_baseline_actually_loads(self):
        contents = matching._image_contents()
        assert contents is not None
        assert {ImageFlavour.BASE, ImageFlavour.ML, ImageFlavour.LITE} <= set(contents)

    def test_manifest_tracks_the_kernel_runtime_version(self):
        manifest = json.loads(flavours._MANIFEST_PATH.read_text(encoding="utf-8"))
        kernel_pyproject = (REPO_ROOT / "kernel_runtime" / "pyproject.toml").read_text(encoding="utf-8")
        section = re.search(r"^\[tool\.poetry\](.*?)(?=^\[|\Z)", kernel_pyproject, re.MULTILINE | re.DOTALL)
        assert section
        version = re.search(r'^version\s*=\s*"([^"]+)"', section.group(1), re.MULTILINE)
        assert version
        assert manifest["kernel_version"] == version.group(1), (
            "the committed manifest describes a different kernel_runtime version. "
            "Run 'make kernel_manifest' and commit the result."
        )


class TestPackagingCoverage:
    @pytest.mark.parametrize("fmt", ["sdist", "wheel"])
    def test_pyproject_include_covers_the_data(self, fmt):
        text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        for rel in _relative_paths():
            wanted = f"flowfile_core/flowfile_core/{rel.as_posix()}"
            pattern = re.compile(
                rf'path\s*=\s*"{re.escape(wanted)}"\s*,\s*format\s*=\s*"{fmt}"'
            )
            assert pattern.search(text), (
                f"pyproject.toml [tool.poetry].include has no {fmt} entry for {wanted}. "
                "Poetry needs the path spelled out once per format — a single-format "
                "entry ships the file to only half the artifacts."
            )

    def test_pyinstaller_spec_ships_the_data(self, tmp_path, monkeypatch):
        from build_backends.main import create_spec_file

        monkeypatch.chdir(tmp_path)
        spec_text = Path(create_spec_file(".", "run.py", "probe", [])).read_text(encoding="utf-8")

        datas_expr = spec_text.split("datas=", 1)[1].split("hiddenimports=", 1)[0]
        assert "kernel_manifest_datas" in datas_expr, (
            "kernel_manifest_datas is not part of the Analysis(datas=...) expression"
        )
        for rel in _relative_paths():
            src_parts = ("flowfile_core", "flowfile_core", *rel.parts)
            dest_parts = ("flowfile_core", *rel.parent.parts)
            assert ", ".join(f'"{part}"' for part in src_parts) in spec_text, (
                f"the generated spec never collects {rel} as a data file"
            )
            assert ", ".join(f'"{part}"' for part in dest_parts) in spec_text, (
                f"the generated spec does not place {rel} under flowfile_core/{rel.parent}"
            )

    def test_docker_image_copies_the_package(self):
        dockerfile = (REPO_ROOT / "flowfile_core" / "Dockerfile").read_text(encoding="utf-8")
        assert "COPY flowfile_core/flowfile_core /app/flowfile_core" in dockerfile, (
            "the core image no longer copies the package wholesale; confirm the kernel "
            "image manifest still lands in the image."
        )


# Only these modules are needed to evaluate a dependency — matching.py is
# deliberately free of docker and of the rest of flowfile_core.
_L2_MODULES = ("models.py", "images.py", "flavours.py", "matching.py")

_L2_PROBE = """
import sys
from flowfile_core.kernel import flavours, matching
from flowfile_core.kernel.models import ImageFlavour, KernelInfo

assert flavours.__file__.startswith(sys.argv[1]), (
    "the staged package was shadowed by the repo checkout: " + flavours.__file__
)

contents = matching._image_contents()
assert contents is not None, "no baseline in a packaged layout"

kernel = KernelInfo(id="k", name="K", image_flavour=ImageFlavour.BASE)
entry = matching.evaluate_kernel(["scikit-learn>=1.5"], kernel)
assert entry.level == "none", entry.level
assert entry.missing == ["scikit-learn>=1.5"], entry.missing
assert matching.verify_kernel_for_node(kernel, ["scikit-learn>=1.5"]), "run-time gate did not block"
print("ok")
"""


def test_packaged_layout_still_detects_a_missing_dependency(tmp_path):
    """Stage only what the packaging manifests ship, then prove detection works.

    Runs in a subprocess so no repo-resolved ``flowfile_core`` can already be
    imported, and from a tree with no repo above it — which is exactly what makes
    the old ``parents[3]`` lookup impossible.
    """
    staged = tmp_path / "site"
    kernel_dir = staged / "flowfile_core" / "kernel"
    kernel_dir.mkdir(parents=True)
    (staged / "flowfile_core" / "__init__.py").write_text("")
    (kernel_dir / "__init__.py").write_text("")
    for module in _L2_MODULES:
        (kernel_dir / module).write_bytes((PACKAGE_ROOT / "kernel" / module).read_bytes())
    for path in REQUIRED_DATA_FILES:
        rel = path.resolve().relative_to(PACKAGE_ROOT)
        (staged / "flowfile_core" / rel).write_bytes(path.read_bytes())

    probe = tmp_path / "probe.py"
    probe.write_text(_L2_PROBE)
    result = subprocess.run(
        [sys.executable, str(probe), str(staged)],
        cwd=tmp_path,
        env={"PYTHONPATH": str(staged), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "ok" in result.stdout
