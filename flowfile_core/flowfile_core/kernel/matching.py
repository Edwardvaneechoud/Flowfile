"""Match custom-node dependency specs against kernels, pure and Docker-free.

Powers ``POST /kernels/match`` (picker ranking + create-from-spec suggestion)
and the run-time dependency pre-check in ``flow_graph``. This module must not
import ``kernel.manager`` — that pulls in the docker SDK at module level.

Satisfaction semantics, three ways to fail to say "yes":

* ``missing`` — we positively know the kernel lacks it (name absent from an
  image whose contents we know, or an installed version that fails the
  specifier). Only this blocks a run.
* ``unverified`` — we know the image's contents and the name is there, but the
  version or extras can't be proven.
* ``unknown`` — we can't enumerate the image at all (a user's custom image, or
  no shipped baseline for this flavour). Absence is unprovable, so this must
  never render as a match.

The last one is deliberately distinct: collapsing it into ``full`` is what made
every packaged install report "has all packages" for kernels that had none.
"""

from __future__ import annotations

import functools
import logging
import re
from dataclasses import dataclass
from typing import Literal

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from flowfile_core.kernel.flavours import _ML_EXTRA_PACKAGE_NAMES, flavour_contents
from flowfile_core.kernel.models import (
    ImageFlavour,
    KernelConfig,
    KernelInfo,
    KernelMatchEntry,
    KernelState,
)

logger = logging.getLogger(__name__)

DependencyStatus = Literal["satisfied", "unverified", "unknown", "missing", "invalid"]
BaselineSource = Literal["manifest", "declared", "unknown"]

_NAME_FALLBACK_RE = re.compile(r"^[A-Za-z0-9_.\-]+")
_SLUG_RE = re.compile(r"[^A-Za-z0-9_-]+")

_LEVEL_RANK = {"full": 0, "unknown": 1, "partial": 2, "none": 3}
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


@functools.lru_cache(maxsize=1)
def _image_contents() -> dict[ImageFlavour, dict[str, str | None]] | None:
    """``{flavour: {canonical_name: version | None}}`` from the shipped manifest.

    ``None`` means no baseline at all, so absence can never be proven and every
    known image is treated as opaque. A flavour *missing* from the returned dict
    is one the manifest doesn't vouch for (its configured image is a different
    release) and is opaque on its own.
    """
    contents = flavour_contents()
    if contents is None:
        logger.warning(
            "No kernel image baseline available — every kernel dependency will report as "
            "unknown rather than missing. See flowfile_core/kernel/flavours.py."
        )
    return contents


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
    be enumerated (custom image, or no shipped manifest entry for this
    flavour) — absence is then never provable.
    """
    contents = _image_contents() or {}
    provides = dict(contents.get(kernel.image_flavour, {}))
    for spec in kernel.packages:
        parsed = parse_dependency(spec)
        if parsed is not None:
            provides.setdefault(parsed.name, None)
    for pkg in kernel.resolved_packages:
        provides[canonicalize_name(pkg.name)] = pkg.version
    opaque = kernel.image_flavour == ImageFlavour.CUSTOM or kernel.image_flavour not in contents
    return provides, opaque


def kernel_baseline(kernel: KernelInfo) -> BaselineSource:
    """Where this kernel's package inventory came from, for honest UI copy."""
    contents = _image_contents() or {}
    if kernel.image_flavour in contents and kernel.image_flavour != ImageFlavour.CUSTOM:
        return "manifest"
    if kernel.resolved_packages or kernel.packages:
        return "declared"
    return "unknown"


def evaluate_dependency(
    dep: ParsedDependency,
    provides: dict[str, str | None],
    opaque: bool,
    raw_packages: list[str],
) -> tuple[DependencyStatus, str | None]:
    if dep.name not in provides:
        if opaque:
            return "unknown", "image contents unknown"
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
    buckets: dict[str, list[str]] = {"satisfied": [], "unverified": [], "unknown": [], "missing": []}
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
    if missing:
        level = "none" if len(missing) == parseable else "partial"
    elif buckets["unknown"]:
        level = "unknown"
    else:
        level = "full"
    return KernelMatchEntry(
        kernel_id=kernel.id,
        kernel_name=kernel.name,
        state=kernel.state,
        image_flavour=kernel.image_flavour,
        satisfied=buckets["satisfied"],
        missing=missing,
        unverified=buckets["unverified"],
        unknown=buckets["unknown"],
        invalid=invalid,
        details=details,
        level=level,
        baseline=kernel_baseline(kernel),
    )


def match_kernels(dependencies: list[str], kernels: list[KernelInfo]) -> list[KernelMatchEntry]:
    """Evaluate every kernel and return entries sorted best-first."""
    entries = [evaluate_kernel(dependencies, kernel) for kernel in kernels]
    entries.sort(
        key=lambda e: (
            _LEVEL_RANK[e.level],
            len(e.missing),
            len(e.unknown),
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
    contents = _image_contents() or {}
    provides = contents.get(flavour, {})
    # Don't claim provable knowledge we don't have. Without a baseline every dep
    # falls through to `packages` either way, but hardcoding opaque=False here
    # made "not in the curated list" read as "missing" — which is how the
    # baseline-less app ended up baking packages the image already ships.
    opaque = flavour not in contents

    covered: list[str] = []
    packages: list[str] = []
    for dep in parsed:
        status, _ = evaluate_dependency(dep, provides, opaque=opaque, raw_packages=[])
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

    Empty list means "don't block": unknown kernel, no deps, unverified,
    unknown or invalid specs all pass — only positive mismatches stop a run.
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
