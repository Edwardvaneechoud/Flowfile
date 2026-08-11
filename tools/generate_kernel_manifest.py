"""Generate the kernel image manifest core uses for dependency matching.

``flowfile_core.kernel.matching`` needs to know which packages each kernel image
flavour actually contains, so it can tell a user that a custom node's dependency
is genuinely missing. That answer used to be re-derived at runtime by walking up
to ``kernel_runtime/poetry.lock`` — which only exists in a repo checkout, so
every shipped artifact (wheel, Docker image, PyInstaller sidecar) silently
degraded to "can't prove anything is missing" and reported a false all-clear.

So the closure is computed here, at build time, and committed as package data
next to the code that reads it. Run ``make kernel_manifest`` after changing
``kernel_runtime/pyproject.toml`` or its lockfile; ``make check_kernel_manifest``
fails CI when the committed file drifts.

Version story per flavour, mirroring ``kernel_runtime/Dockerfile``:

* ``base`` / ``ml`` — ``poetry install --only=main`` installs exactly the locked
  versions, so every package maps to its locked version.
* ``lite`` — installs the same packages, but ``SLIM_CONSTRAINTS=true`` trims
  ``/opt/constraints.txt`` to a whitelist, so a later user install can float
  anything outside it. Non-whitelisted versions are therefore emitted as null
  ("present, version not guaranteed").
"""

import argparse
import json
import re
import sys
from pathlib import Path

from packaging.markers import InvalidMarker, Marker
from packaging.utils import canonicalize_name

REPO_ROOT = Path(__file__).resolve().parents[1]
KERNEL_RUNTIME = REPO_ROOT / "kernel_runtime"
MANIFEST_PATH = REPO_ROOT / "flowfile_core" / "flowfile_core" / "kernel" / "kernel_image_manifest.json"

_TOML_ENTRY_RE = re.compile(r"^([A-Za-z0-9_.\-]+)\s*=\s*(.+)$")
_LOCK_NAME_RE = re.compile(r'^name = "([^"]+)"', re.MULTILINE)
_LOCK_VERSION_RE = re.compile(r'^version = "([^"]+)"', re.MULTILINE)
_MARKER_RE = re.compile(r'markers\s*=\s*"((?:[^"\\]|\\.)*)"')
# The Dockerfile's SLIM_CONSTRAINTS branch: grep -E '^(a|b|c)==' ...
_SLIM_RE = re.compile(r"grep -E '\^\(([^)]+)\)=='")
_PYTHON_IMAGE_RE = re.compile(r"^FROM python:(\d+)\.(\d+)", re.MULTILINE)

# The images are published multi-arch, so a dependency gated on the machine is
# present in one and absent in the other. Both are evaluated; a dep that only
# holds for one arch is "sometimes" — recorded without a version so it can never
# produce a confident verdict either way.
_IMAGE_MACHINES = (("x86_64", "Linux"), ("aarch64", "Linux"))

ALWAYS, SOMETIMES, NEVER = "always", "sometimes", "never"


def _image_environments(python_version: str) -> list[dict[str, str]]:
    return [
        {
            "python_version": python_version,
            "python_full_version": f"{python_version}.0",
            "implementation_version": f"{python_version}.0",
            "os_name": "posix",
            "sys_platform": "linux",
            "platform_system": system,
            "platform_machine": machine,
            "platform_release": "",
            "platform_version": "",
            "implementation_name": "cpython",
            "platform_python_implementation": "CPython",
            "extra": "",
        }
        for machine, system in _IMAGE_MACHINES
    ]


def _marker_holds(marker_text: str | None, environments: list[dict[str, str]]) -> str:
    """Whether a dependency's environment marker holds in the image.

    ``always`` / ``never`` / ``sometimes`` (arch-dependent, or an unparseable
    marker we refuse to guess about).
    """
    if not marker_text:
        return ALWAYS
    try:
        marker = Marker(marker_text.replace('\\"', '"'))
    except InvalidMarker:
        return SOMETIMES
    results = {marker.evaluate(env) for env in environments}
    if results == {True}:
        return ALWAYS
    if results == {False}:
        return NEVER
    return SOMETIMES


class ManifestError(RuntimeError):
    """The manifest could not be derived from kernel_runtime's sources."""


def _toml_section_lines(text: str, header: str) -> list[str]:
    lines: list[str] = []
    in_section = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == header:
            in_section = True
            continue
        if in_section:
            if stripped.startswith("["):
                break
            lines.append(stripped)
    return lines


def _project_version(pyproject_text: str) -> str:
    section = re.search(r"^\[tool\.poetry\](.*?)(?=^\[|\Z)", pyproject_text, re.MULTILINE | re.DOTALL)
    if not section:
        raise ManifestError("no [tool.poetry] section in kernel_runtime/pyproject.toml")
    match = re.search(r'^version\s*=\s*"([^"]+)"', section.group(1), re.MULTILINE)
    if not match:
        raise ManifestError("no version in kernel_runtime/pyproject.toml [tool.poetry]")
    return match.group(1)


def _dependency_roots(pyproject_text: str) -> tuple[frozenset[str], frozenset[str]]:
    """(base_roots, ml_extra_roots): the image's non-optional main deps + the ml extras."""
    base: set[str] = set()
    for line in _toml_section_lines(pyproject_text, "[tool.poetry.dependencies]"):
        match = _TOML_ENTRY_RE.match(line)
        if not match or match.group(1) == "python" or "optional = true" in match.group(2):
            continue
        base.add(canonicalize_name(match.group(1)))
    ml: set[str] = set()
    for line in _toml_section_lines(pyproject_text, "[tool.poetry.extras]"):
        if line.startswith("ml"):
            ml.update(canonicalize_name(name) for name in re.findall(r'"([^"]+)"', line))
    if not base:
        raise ManifestError("no non-optional dependencies found in kernel_runtime/pyproject.toml")
    return frozenset(base), frozenset(ml)


def _lock_graph(
    lock_text: str, environments: list[dict[str, str]]
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, str]]:
    """``({name: [(dep, certainty)]}, {name: version})`` from poetry.lock.

    Extras-only edges are skipped entirely; edges whose marker can never hold in
    the image (Windows-only, ``python_version < "3.11"``) are dropped, and
    arch-dependent ones are carried through as ``sometimes``.
    """
    graph: dict[str, list[tuple[str, str]]] = {}
    versions: dict[str, str] = {}
    for block in lock_text.split("[[package]]")[1:]:
        name_match = _LOCK_NAME_RE.search(block)
        if not name_match:
            continue
        name = canonicalize_name(name_match.group(1))
        version_match = _LOCK_VERSION_RE.search(block)
        if version_match:
            versions[name] = version_match.group(1)
        deps: list[tuple[str, str]] = []
        for line in _toml_section_lines(block, "[package.dependencies]"):
            match = _TOML_ENTRY_RE.match(line)
            if not match or "optional = true" in match.group(2):
                continue
            marker = _MARKER_RE.search(match.group(2))
            certainty = _marker_holds(marker.group(1) if marker else None, environments)
            if certainty == NEVER:
                continue
            deps.append((canonicalize_name(match.group(1)), certainty))
        graph[name] = deps
    if not graph:
        raise ManifestError("no [[package]] blocks found in kernel_runtime/poetry.lock")
    return graph, versions


def _slim_pinned_names(dockerfile_text: str) -> frozenset[str]:
    """The packages lite still pins, read out of the Dockerfile's SLIM_CONSTRAINTS grep."""
    match = _SLIM_RE.search(dockerfile_text)
    if not match:
        raise ManifestError(
            "could not find the SLIM_CONSTRAINTS grep pattern in kernel_runtime/Dockerfile. "
            "If the lite build changed, update _SLIM_RE in this generator to match."
        )
    return frozenset(canonicalize_name(name) for name in match.group(1).split("|"))


def _closure(roots: frozenset[str], graph: dict[str, list[tuple[str, str]]]) -> dict[str, bool]:
    """``{name: certainly_installed}`` over the dependency graph.

    A package is certain only when some path to it is unconditional; reachable
    only through an arch-dependent edge means "maybe", which the manifest records
    as a null version so matching stays unable to prove anything about it.
    """
    certain: dict[str, bool] = {}
    stack: list[tuple[str, bool]] = [(root, True) for root in roots]
    while stack:
        name, sure = stack.pop()
        if certain.get(name, False) or (name in certain and not sure):
            continue
        certain[name] = sure
        for dep, edge in graph.get(name, []):
            stack.append((dep, sure and edge == ALWAYS))
    return certain


def build_manifest(kernel_runtime: Path = KERNEL_RUNTIME) -> dict:
    """Build the manifest dict from kernel_runtime's pyproject, lockfile and Dockerfile."""
    try:
        pyproject_text = (kernel_runtime / "pyproject.toml").read_text(encoding="utf-8")
        lock_text = (kernel_runtime / "poetry.lock").read_text(encoding="utf-8")
        dockerfile_text = (kernel_runtime / "Dockerfile").read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"could not read kernel_runtime sources: {exc}") from exc

    python_match = _PYTHON_IMAGE_RE.search(dockerfile_text)
    if not python_match:
        raise ManifestError("could not read the Python version from kernel_runtime/Dockerfile's FROM line")
    environments = _image_environments(f"{python_match.group(1)}.{python_match.group(2)}")

    base_roots, ml_roots = _dependency_roots(pyproject_text)
    graph, versions = _lock_graph(lock_text, environments)
    slim_pinned = _slim_pinned_names(dockerfile_text)

    base_names = _closure(base_roots, graph)
    ml_names = _closure(base_roots | ml_roots, graph)

    def pinned(names: dict[str, bool]) -> dict[str, str | None]:
        return {name: (versions.get(name) if sure else None) for name, sure in sorted(names.items())}

    return {
        "kernel_version": _project_version(pyproject_text),
        "python_version": environments[0]["python_version"],
        "source": "kernel_runtime/{pyproject.toml,poetry.lock,Dockerfile}",
        "generator": "tools/generate_kernel_manifest.py",
        "flavours": {
            "base": pinned(base_names),
            "ml": pinned(ml_names),
            # Lite installs base's packages but only pins the whitelist, so
            # everything else is present-with-unknown-version.
            "lite": {
                name: (versions.get(name) if (name in slim_pinned and sure) else None)
                for name, sure in sorted(base_names.items())
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--kernel-runtime", type=Path, default=KERNEL_RUNTIME)
    args = parser.parse_args()

    try:
        manifest = build_manifest(args.kernel_runtime)
    except ManifestError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    counts = {flavour: len(packages) for flavour, packages in manifest["flavours"].items()}
    print(f"Wrote {args.output} (kernel {manifest['kernel_version']}, packages per flavour: {counts})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
