"""Guard that the WASM node-support baseline actually ships.

``flowfile/share`` is **fail-closed**: with no manifest every node type reports
``ABSENT``, so a shipped install would degrade every share link to a canvas of
placeholders. The manifest is derived from ``flowfile_wasm``, which is in no
shipped artifact, so nothing but committed package data can carry it — and a
packaging manifest that forgets it fails invisibly in the one layout (a repo
checkout) where every other test runs.

Same three layers as ``test_kernel_packaging_gate.py``:

* **L0 containment** — the data resolves *inside* the installed package.
* **L1 coverage** — every packaging manifest actually carries it.
* **L2 behaviour** — in a tree containing only what those manifests ship, a
  supported node type is still reported as supported.
"""

import re
import subprocess
import sys
from pathlib import Path

import pytest

import flowfile_core
from flowfile_core.flowfile.share import support

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = Path(flowfile_core.__file__).resolve().parent

# Every runtime data file share-link building depends on. Derived from the
# module so the gate follows the data if it ever moves.
REQUIRED_DATA_FILES = (support._MANIFEST_PATH,)


def _relative_paths() -> list[Path]:
    return [path.resolve().relative_to(PACKAGE_ROOT) for path in REQUIRED_DATA_FILES]


class TestContainment:
    def test_data_exists_and_lives_inside_the_package(self):
        for path in REQUIRED_DATA_FILES:
            assert path.is_file(), f"{path} is missing — run 'make wasm_node_manifest'"
            resolved = path.resolve()
            assert resolved.is_relative_to(PACKAGE_ROOT), (
                f"{resolved} resolves outside the flowfile_core package ({PACKAGE_ROOT}). "
                "No packaging manifest — wheel, sdist, PyInstaller datas or the Dockerfile's "
                "COPY — can place a file outside the package where a shipped install will "
                "find it, so this data is unreachable everywhere except a repo checkout."
            )

    def test_baseline_actually_loads(self):
        manifest = support.load_manifest()
        assert manifest is not None
        assert support.tier_for("filter") is support.SupportTier.SUPPORTED

    def test_manifest_tracks_the_browser_palette(self):
        """The committed manifest must still describe flowfile_wasm's palette."""
        sys.path.insert(0, str(REPO_ROOT))
        try:
            from tools.generate_wasm_node_manifest import parse_node_catalog, parse_type_to_core
        finally:
            sys.path.remove(str(REPO_ROOT))

        catalog = REPO_ROOT / "flowfile_wasm" / "src" / "config" / "nodeCatalog.ts"
        if not catalog.is_file():
            pytest.skip("flowfile_wasm is not present in this layout")

        palette, supported_types = parse_node_catalog(catalog.read_text(encoding="utf-8"))
        type_to_core = parse_type_to_core(
            (REPO_ROOT / "flowfile_wasm" / "src" / "utils" / "coreExport.ts").read_text(encoding="utf-8")
        )
        expected = {type_to_core.get(name, name) for name in supported_types}
        actual = {
            name
            for name, entry in support.load_manifest()["nodes"].items()
            if entry["tier"] == support.SupportTier.SUPPORTED.value
        }
        assert actual == expected, (
            "the committed manifest disagrees with flowfile_wasm's palette. "
            "Run 'make wasm_node_manifest' and commit the result."
        )
        assert len(palette) == len(actual) + support.load_manifest()["counts"]["locked"]


class TestPackagingCoverage:
    @pytest.mark.parametrize("fmt", ["sdist", "wheel"])
    def test_pyproject_include_covers_the_data(self, fmt):
        text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        for rel in _relative_paths():
            wanted = f"flowfile_core/flowfile_core/{rel.as_posix()}"
            pattern = re.compile(rf'path\s*=\s*"{re.escape(wanted)}"\s*,\s*format\s*=\s*"{fmt}"')
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
        assert "share_manifest_datas" in datas_expr, (
            "share_manifest_datas is not part of the Analysis(datas=...) expression"
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
            "the core image no longer copies the package wholesale; confirm the WASM node "
            "support manifest still lands in the image."
        )


# support.py is deliberately free of the rest of flowfile_core, so the baseline
# can be evaluated from these files alone.
_L2_PROBE = """
import sys
from flowfile_core.flowfile.share import support

assert support.__file__.startswith(sys.argv[1]), (
    "the staged package was shadowed by the repo checkout: " + support.__file__
)

assert support.load_manifest() is not None, "no baseline in a packaged layout"
assert support.tier_for("filter") is support.SupportTier.SUPPORTED, support.tier_for("filter")
assert support.tier_for("database_reader") is support.SupportTier.LOCKED
assert support.tier_for("run_flow") is support.SupportTier.ABSENT
assert "n_unique" in support.group_by_aggs()
print("ok")
"""


def test_packaged_layout_still_resolves_the_baseline(tmp_path):
    """Stage only what the packaging manifests ship, then prove the baseline loads.

    Runs in a subprocess so no repo-resolved ``flowfile_core`` can already be
    imported, and from a tree with no repo above it.
    """
    staged = tmp_path / "site"
    share_dir = staged / "flowfile_core" / "flowfile" / "share"
    share_dir.mkdir(parents=True)
    (staged / "flowfile_core" / "__init__.py").write_text("")
    (staged / "flowfile_core" / "flowfile" / "__init__.py").write_text("")
    (share_dir / "__init__.py").write_text("")
    (share_dir / "support.py").write_bytes((PACKAGE_ROOT / "flowfile" / "share" / "support.py").read_bytes())
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
