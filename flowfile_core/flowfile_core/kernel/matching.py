"""Match custom-node dependency specs against kernels, pure and Docker-free.

Powers ``POST /kernels/match`` (picker ranking + create-from-spec suggestion)
and the run-time dependency pre-check in ``flow_graph``. This module must not
import ``kernel.manager`` — that pulls in the docker SDK at module level.

Satisfaction semantics: a dependency is ``missing`` only when we positively
know the kernel lacks it (name absent from a known image, or an installed
version that fails the specifier). Anything we cannot prove — unknown
versions, custom images, extras — is ``unverified`` and never blocks.
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from typing import Literal

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from flowfile_core.kernel.flavours import _KERNEL_LOCK_PATH, _ML_EXTRA_PACKAGE_NAMES, get_flavour_packages
from flowfile_core.kernel.models import (
    ImageFlavour,
    KernelConfig,
    KernelInfo,
    KernelMatchEntry,
    KernelState,
)

DependencyStatus = Literal["satisfied", "unverified", "missing", "invalid"]

_NAME_FALLBACK_RE = re.compile(r"^[A-Za-z0-9_.\-]+")
_SLUG_RE = re.compile(r"[^A-Za-z0-9_-]+")

_LEVEL_RANK = {"full": 0, "partial": 1, "none": 2}
_STATE_RANK = {
    KernelState.IDLE: 0,
    KernelState.EXECUTING: 0,
    KernelState.STARTING: 1,
    KernelState.STOPPED: 2,
    KernelState.ERROR: 3,
}


@dataclass(frozen=True)
class ParsedDependency:
    spec: str  # original string, echoed back in responses
    name: str  # PEP 503 canonical name
    specifier: SpecifierSet
    extras: frozenset[str]


def parse_dependency(spec: str) -> ParsedDependency | None:
    """Parse a pip requirement string; ``None`` means unmatchable (invalid).

    URL/VCS requirements and dependency-confusion shapes (leading ``._/-``)
    are invalid: a baked kernel spec can never satisfy them. Environment
    markers are ignored — kernel images are a single known platform.
    """
    text = (spec or "").strip()
    if not text or text[0] in "._/-" or "://" in text or text.lower().startswith("git+"):
        return None
    try:
        req = Requirement(text)
    except InvalidRequirement:
        # Salvage a bare name from shapes like "pkg==" so one typo'd pin
        # degrades to name-level matching instead of vanishing.
        match = _NAME_FALLBACK_RE.match(text)
        if not match:
            return None
        return ParsedDependency(
            spec=spec, name=canonicalize_name(match.group(0)), specifier=SpecifierSet(), extras=frozenset()
        )
    if req.url:
        return None
    return ParsedDependency(
        spec=spec, name=canonicalize_name(req.name), specifier=req.specifier, extras=frozenset(req.extras)
    )


_KERNEL_PYPROJECT_PATH = _KERNEL_LOCK_PATH.parent / "pyproject.toml"

_TOML_ENTRY_RE = re.compile(r"^([A-Za-z0-9_.\-]+)\s*=\s*(.+)$")
_LOCK_NAME_RE = re.compile(r'^name = "([^"]+)"', re.MULTILINE)


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


def _kernel_pyproject_roots(text: str) -> tuple[frozenset[str], frozenset[str]]:
    """(base_roots, ml_extra_roots): the image's non-optional main deps + the ml extras."""
    base: set[str] = set()
    for line in _toml_section_lines(text, "[tool.poetry.dependencies]"):
        match = _TOML_ENTRY_RE.match(line)
        if not match or match.group(1) == "python" or "optional = true" in match.group(2):
            continue
        base.add(canonicalize_name(match.group(1)))
    ml: set[str] = set()
    for line in _toml_section_lines(text, "[tool.poetry.extras]"):
        if line.startswith("ml"):
            ml.update(canonicalize_name(name) for name in re.findall(r'"([^"]+)"', line))
    return frozenset(base), frozenset(ml)


def _lock_dependency_graph(text: str) -> dict[str, frozenset[str]]:
    """``{canonical_name: canonical dep names}`` from poetry.lock, skipping
    extras-only (``optional = true``) edges."""
    graph: dict[str, frozenset[str]] = {}
    for block in text.split("[[package]]")[1:]:
        name_match = _LOCK_NAME_RE.search(block)
        if not name_match:
            continue
        deps: set[str] = set()
        for line in _toml_section_lines(block, "[package.dependencies]"):
            match = _TOML_ENTRY_RE.match(line)
            if match and "optional = true" not in match.group(2):
                deps.add(canonicalize_name(match.group(1)))
        graph[canonicalize_name(name_match.group(1))] = frozenset(deps)
    return graph


@functools.lru_cache(maxsize=1)
def _image_contents() -> dict[ImageFlavour, frozenset[str]] | None:
    """Canonical names actually installed per flavour image, or ``None``.

    The images install the FULL kernel_runtime main dependency group (plus ml
    extras for the ml image) — far more than the curated display list in
    flavours.py — so provable absence must be judged against the dependency
    closure of pyproject roots over the lockfile graph. ``None`` (files
    unreadable, e.g. a packaged install without the repo) means absence can
    never be proven and callers treat known images as opaque.
    """
    try:
        pyproject_text = _KERNEL_PYPROJECT_PATH.read_text(encoding="utf-8")
        lock_text = _KERNEL_LOCK_PATH.read_text(encoding="utf-8")
    except OSError:
        return None
    base_roots, ml_roots = _kernel_pyproject_roots(pyproject_text)
    if not base_roots:
        return None
    graph = _lock_dependency_graph(lock_text)

    def closure(roots: frozenset[str]) -> frozenset[str]:
        seen: set[str] = set()
        stack = list(roots)
        while stack:
            name = stack.pop()
            if name in seen:
                continue
            seen.add(name)
            stack.extend(graph.get(name, frozenset()))
        return frozenset(seen)

    base = closure(base_roots)
    # Lite installs the same package set as base — only the constraint pins differ.
    return {
        ImageFlavour.BASE: base,
        ImageFlavour.ML: closure(base_roots | ml_roots),
        ImageFlavour.LITE: base,
        ImageFlavour.CUSTOM: frozenset(),
    }


@functools.lru_cache(maxsize=1)
def _flavour_provides() -> dict[ImageFlavour, dict[str, str | None]]:
    """``{flavour: {canonical_name: version | None}}`` for the baked images.

    Name-presence comes from the full image contents closure; known versions
    come from the curated lockfile lists (flavours.py). Lite bakes the same
    packages as base but only pins a whitelist in /opt/constraints.txt, so
    its non-whitelisted versions stay unknown.
    """
    packages = get_flavour_packages()
    contents = _image_contents() or {}

    def to_map(entries: list[tuple[str, str]]) -> dict[str, str | None]:
        return {canonicalize_name(name): (version if version != "—" else None) for name, version in entries}

    def build(flavour: ImageFlavour, versions: dict[str, str | None]) -> dict[str, str | None]:
        provides: dict[str, str | None] = {name: None for name in contents.get(flavour, frozenset())}
        provides.update(versions)
        return provides

    base_versions = to_map(packages[ImageFlavour.BASE])
    lite_versions = {name: None for name in base_versions}
    lite_versions.update(to_map(packages[ImageFlavour.LITE]))
    return {
        ImageFlavour.BASE: build(ImageFlavour.BASE, base_versions),
        ImageFlavour.ML: build(ImageFlavour.ML, to_map(packages[ImageFlavour.ML])),
        ImageFlavour.LITE: build(ImageFlavour.LITE, lite_versions),
        ImageFlavour.CUSTOM: {},
    }


@functools.lru_cache(maxsize=1)
def _ml_extra_names() -> frozenset[str]:
    return frozenset(canonicalize_name(name) for name in _ML_EXTRA_PACKAGE_NAMES)


def canonical_spec(dep: ParsedDependency) -> str:
    """Render a dependency in the strict form the kernel create/update
    endpoints accept: ``name[extras]specifier`` — no whitespace, no markers."""
    extras = f"[{','.join(sorted(dep.extras))}]" if dep.extras else ""
    return f"{dep.name}{extras}{dep.specifier}"


def kernel_provides(kernel: KernelInfo) -> tuple[dict[str, str | None], bool]:
    """Return ``(canonical_name -> version | None, opaque)`` for one kernel.

    Flavour-baked packages, then the requested ``packages`` specs name-only
    (legacy kernels created before resolved_packages existed), then the
    resolved versions, which win. ``opaque`` means the image's contents can't
    be enumerated (custom image, or the kernel_runtime lock/pyproject files
    are unreadable) — absence is then never provable.
    """
    provides = dict(_flavour_provides().get(kernel.image_flavour, {}))
    for spec in kernel.packages:
        parsed = parse_dependency(spec)
        if parsed is not None:
            provides.setdefault(parsed.name, None)
    for pkg in kernel.resolved_packages:
        provides[canonicalize_name(pkg.name)] = pkg.version
    opaque = kernel.image_flavour == ImageFlavour.CUSTOM or _image_contents() is None
    return provides, opaque


def evaluate_dependency(
    dep: ParsedDependency,
    provides: dict[str, str | None],
    opaque: bool,
    raw_packages: list[str],
) -> tuple[DependencyStatus, str | None]:
    if dep.name not in provides:
        if opaque:
            return "unverified", "image contents unknown"
        return "missing", "not installed"
    version = provides[dep.name]
    if version is None:
        status, reason = ("unverified", "version unknown") if dep.specifier else ("satisfied", None)
    else:
        try:
            installed = Version(version)
        except InvalidVersion:
            return "unverified", f"installed {version} (unrecognised version)"
        if not dep.specifier or dep.specifier.contains(installed, prereleases=True):
            status, reason = "satisfied", f"installed {version}"
        else:
            return "missing", f"installed {version}"
    # Extras can't be proven from a version number — cap at unverified unless
    # the kernel was built from this exact spec (extras baked in).
    if status == "satisfied" and dep.extras:
        known_forms = {p.strip() for p in raw_packages}
        for raw in raw_packages:
            parsed = parse_dependency(raw)
            if parsed is not None:
                known_forms.add(canonical_spec(parsed))
        if dep.spec.strip() not in known_forms and canonical_spec(dep) not in known_forms:
            return "unverified", "extras not verifiable"
    return status, reason


def evaluate_kernel(dependencies: list[str], kernel: KernelInfo) -> KernelMatchEntry:
    provides, opaque = kernel_provides(kernel)
    buckets: dict[str, list[str]] = {"satisfied": [], "unverified": [], "missing": []}
    invalid: list[str] = []
    details: dict[str, str] = {}
    for spec in dependencies:
        dep = parse_dependency(spec)
        if dep is None:
            invalid.append(spec)
            details[spec] = "unparseable requirement"
            continue
        status, reason = evaluate_dependency(dep, provides, opaque, kernel.packages)
        # Canonical form throughout: it's what create/PATCH will accept when
        # the UI feeds these strings back (add-missing-packages, suggestions).
        spec_repr = canonical_spec(dep)
        buckets[status].append(spec_repr)
        if reason:
            details[spec_repr] = reason
    missing = buckets["missing"]
    parseable = sum(len(bucket) for bucket in buckets.values())
    level = "none" if missing and len(missing) == parseable else ("partial" if missing else "full")
    return KernelMatchEntry(
        kernel_id=kernel.id,
        kernel_name=kernel.name,
        state=kernel.state,
        image_flavour=kernel.image_flavour,
        satisfied=buckets["satisfied"],
        missing=missing,
        unverified=buckets["unverified"],
        invalid=invalid,
        details=details,
        level=level,
    )


def match_kernels(dependencies: list[str], kernels: list[KernelInfo]) -> list[KernelMatchEntry]:
    """Evaluate every kernel and return entries sorted best-first."""
    entries = [evaluate_kernel(dependencies, kernel) for kernel in kernels]
    entries.sort(
        key=lambda e: (
            _LEVEL_RANK[e.level],
            len(e.missing),
            len(e.unverified),
            _STATE_RANK.get(e.state, 4),
            e.kernel_id,
        )
    )
    return entries


def suggest_kernel_config(
    dependencies: list[str],
    existing_ids: set[str],
    node_name: str | None = None,
) -> tuple[KernelConfig, list[str]]:
    """Derive a ready-to-POST create seed from a node's dependency specs.

    Flavour is ML when any dep name is among the ML image's extra packages,
    else base (never lite/custom). Only deps the flavour's locked versions
    *provably* satisfy are dropped from ``packages`` — an unprovable pin
    stays so the image bake resolves it. Returns ``(config, covered_by_flavour)``.
    """
    parsed = [dep for dep in (parse_dependency(spec) for spec in dependencies) if dep is not None]
    flavour = ImageFlavour.ML if any(dep.name in _ml_extra_names() for dep in parsed) else ImageFlavour.BASE
    provides = _flavour_provides()[flavour]

    covered: list[str] = []
    packages: list[str] = []
    for dep in parsed:
        status, _ = evaluate_dependency(dep, provides, opaque=False, raw_packages=[])
        if status == "satisfied":
            covered.append(canonical_spec(dep))
        else:
            packages.append(canonical_spec(dep))
    packages = list(dict.fromkeys(packages))

    slug = _SLUG_RE.sub("-", (node_name or "").strip()).strip("-").lower() or "custom-node"
    base_id = f"{slug}-kernel"
    kernel_id, suffix = base_id, 2
    while kernel_id in existing_ids:
        kernel_id = f"{base_id}-{suffix}"
        suffix += 1

    display = (node_name or "").strip() or "Custom node"
    config = KernelConfig(id=kernel_id, name=f"{display} kernel", image_flavour=flavour, packages=packages)
    return config, covered


def verify_kernel_for_node(kernel: KernelInfo | None, dependencies: list[str]) -> list[str]:
    """Pre-check: formatted entries for deps the kernel *provably* lacks.

    Empty list means "don't block": unknown kernel, no deps, unverified or
    invalid specs all pass — only positive mismatches stop a run.
    """
    if kernel is None or not dependencies:
        return []
    provides, opaque = kernel_provides(kernel)
    problems: list[str] = []
    for spec in dependencies:
        dep = parse_dependency(spec)
        if dep is None:
            continue
        status, reason = evaluate_dependency(dep, provides, opaque, kernel.packages)
        if status == "missing":
            spec_repr = canonical_spec(dep)
            problems.append(f"{spec_repr} ({reason})" if reason else spec_repr)
    return problems
