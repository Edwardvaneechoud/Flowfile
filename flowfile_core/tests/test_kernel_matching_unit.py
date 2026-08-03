"""Unit tests for kernel/matching.py — pure logic, no Docker, no TestClient."""

import pytest

from flowfile_core.kernel.manager import _spec_to_name
from flowfile_core.kernel.matching import (
    ParsedDependency,
    evaluate_kernel,
    kernel_provides,
    match_kernels,
    parse_dependency,
    suggest_kernel_config,
    verify_kernel_for_node,
)
from flowfile_core.kernel.models import ImageFlavour, KernelInfo, KernelState, ResolvedPackage


def make_kernel(
    kernel_id="k",
    name="Kernel",
    flavour=ImageFlavour.BASE,
    packages=None,
    resolved=None,
    state=KernelState.STOPPED,
):
    return KernelInfo(
        id=kernel_id,
        name=name,
        image_flavour=flavour,
        packages=packages or [],
        resolved_packages=[ResolvedPackage(name=n, version=v) for n, v in (resolved or [])],
        state=state,
    )


class TestParseDependency:
    @pytest.mark.parametrize(
        "spec,name,has_specifier",
        [
            ("polars", "polars", False),
            ("scikit-learn>=1.5", "scikit-learn", True),
            ("Scikit_Learn>=1.5", "scikit-learn", True),
            ("numpy>=1.26,<3", "numpy", True),
            ("pandas~=2.2", "pandas", True),
            ("pkg==", "pkg", False),  # salvaged bare name from a typo'd pin
        ],
    )
    def test_parses(self, spec, name, has_specifier):
        dep = parse_dependency(spec)
        assert dep is not None
        assert dep.name == name
        assert bool(dep.specifier) is has_specifier

    @pytest.mark.parametrize(
        "spec",
        [
            "",
            "   ",
            "git+https://github.com/x/y",
            "pkg @ https://example.com/pkg.whl",
            "https://example.com/pkg.whl",
            "-e .",
            "./local-dir",
            "_private-pkg",
            "/abs/path",
        ],
    )
    def test_invalid(self, spec):
        assert parse_dependency(spec) is None

    def test_extras_captured(self):
        dep = parse_dependency("uvicorn[standard]>=0.30")
        assert dep is not None
        assert dep.extras == frozenset({"standard"})

    @pytest.mark.parametrize(
        "spec",
        ["polars", "Scikit_Learn>=1.5", "some.pkg-name", "PkG[extra]>=1", "numpy>=1.26,<3"],
    )
    def test_name_normalisation_parity_with_manager(self, spec):
        dep = parse_dependency(spec)
        assert dep is not None
        assert dep.name == _spec_to_name(spec)


class TestKernelProvides:
    def test_flavour_union_resolved_wins(self):
        kernel = make_kernel(flavour=ImageFlavour.BASE, packages=["pandas>=2"], resolved=[("pandas", "2.2.3")])
        provides, opaque = kernel_provides(kernel)
        assert not opaque
        assert "polars" in provides  # flavour-baked
        assert provides["pandas"] == "2.2.3"  # resolved wins over the name-only request

    def test_legacy_kernel_packages_name_only(self):
        kernel = make_kernel(packages=["pandas>=2"])  # resolved_packages empty (pre-013 kernel)
        provides, _ = kernel_provides(kernel)
        assert provides["pandas"] is None

    def test_custom_image_is_opaque(self):
        provides, opaque = kernel_provides(make_kernel(flavour=ImageFlavour.CUSTOM))
        assert opaque
        assert provides == {}

    def test_lite_provides_base_names_without_versions(self):
        provides, _ = kernel_provides(make_kernel(flavour=ImageFlavour.LITE))
        # Lite bakes the same set as base; only the lite-pinned entries keep versions.
        assert "numpy" in provides and provides["numpy"] is None
        assert "pyarrow" in provides and provides["pyarrow"] is None
        assert "polars" in provides


class TestEvaluateKernel:
    def test_ml_flavour_satisfies_sklearn(self):
        entry = evaluate_kernel(["scikit-learn>=1.0"], make_kernel(flavour=ImageFlavour.ML))
        assert entry.level == "full"
        assert entry.satisfied == ["scikit-learn>=1.0"]
        assert entry.details["scikit-learn>=1.0"].startswith("installed ")

    def test_absent_name_is_missing(self):
        entry = evaluate_kernel(["scikit-learn>=1.0"], make_kernel(flavour=ImageFlavour.BASE))
        assert entry.level == "none"
        assert entry.missing == ["scikit-learn>=1.0"]
        assert entry.details["scikit-learn>=1.0"] == "not installed"

    def test_version_pin_violation_is_missing_with_detail(self):
        kernel = make_kernel(packages=["pandas"], resolved=[("pandas", "1.5.0")])
        entry = evaluate_kernel(["pandas>=2"], kernel)
        assert entry.missing == ["pandas>=2"]
        assert entry.details["pandas>=2"] == "installed 1.5.0"

    def test_legacy_kernel_degrades_to_unverified(self):
        entry = evaluate_kernel(["pandas>=2"], make_kernel(packages=["pandas>=2"]))
        assert entry.level == "full"  # unverified never blocks
        assert entry.unverified == ["pandas>=2"]
        assert entry.details["pandas>=2"] == "version unknown"

    def test_lite_kernel_never_misses_base_names(self):
        entry = evaluate_kernel(["numpy>=1.20"], make_kernel(flavour=ImageFlavour.LITE))
        assert entry.missing == []
        assert entry.unverified == ["numpy>=1.20"]

    def test_custom_image_everything_unverified(self):
        entry = evaluate_kernel(["anything>=1"], make_kernel(flavour=ImageFlavour.CUSTOM))
        assert entry.level == "full"
        assert entry.unverified == ["anything>=1"]
        assert entry.details["anything>=1"] == "image contents unknown"

    def test_baked_but_unsurfaced_packages_are_not_missing(self):
        # The images install the full kernel_runtime main group, not just the
        # curated display list: deltalake/jedi are main deps, pydantic and
        # friends arrive transitively via fastapi.
        entry = evaluate_kernel(["deltalake", "jedi", "pydantic>=2"], make_kernel(flavour=ImageFlavour.BASE))
        assert entry.missing == []
        assert "deltalake" in entry.satisfied
        assert "jedi" in entry.satisfied
        assert entry.unverified == ["pydantic>=2"]
        assert entry.details["pydantic>=2"] == "version unknown"

    def test_ml_transitives_satisfied_but_not_on_base(self):
        # pandas rides in via statsmodels on the ml image only.
        assert evaluate_kernel(["pandas"], make_kernel(flavour=ImageFlavour.ML)).missing == []
        assert evaluate_kernel(["pandas"], make_kernel(flavour=ImageFlavour.BASE)).missing == ["pandas"]

    def test_unreadable_image_contents_degrades_to_unverified(self, monkeypatch):
        from flowfile_core.kernel import matching

        monkeypatch.setattr(matching, "_image_contents", lambda: None)
        entry = evaluate_kernel(["definitely-not-a-package"], make_kernel(flavour=ImageFlavour.BASE))
        assert entry.missing == []
        assert entry.unverified == ["definitely-not-a-package"]

    def test_spaced_and_markered_specs_canonicalised(self):
        entry = evaluate_kernel(
            ["pandas >= 2.1", 'requests==2.32.0; python_version < "3.12"'],
            make_kernel(flavour=ImageFlavour.BASE),
        )
        # Reported forms are strict pip-safe strings the create/PATCH endpoints accept.
        assert entry.missing == ["pandas>=2.1", "requests==2.32.0"]
        assert entry.details["pandas>=2.1"] == "not installed"

    def test_extras_capped_at_unverified_unless_verbatim(self):
        kernel = make_kernel(resolved=[("uvicorn", "0.32.0")])
        capped = evaluate_kernel(["uvicorn[standard]>=0.30"], kernel)
        assert capped.unverified == ["uvicorn[standard]>=0.30"]

        baked = make_kernel(packages=["uvicorn[standard]>=0.30"], resolved=[("uvicorn", "0.32.0")])
        exact = evaluate_kernel(["uvicorn[standard]>=0.30"], baked)
        assert exact.satisfied == ["uvicorn[standard]>=0.30"]

    def test_invalid_specs_quarantined(self):
        entry = evaluate_kernel(
            ["git+https://github.com/x/y", "scikit-learn>=1.0"], make_kernel(flavour=ImageFlavour.ML)
        )
        assert entry.invalid == ["git+https://github.com/x/y"]
        assert entry.level == "full"  # one bad string must not poison the ranking

    def test_empty_dependencies_is_full(self):
        entry = evaluate_kernel([], make_kernel())
        assert entry.level == "full"
        assert entry.satisfied == [] and entry.missing == []

    def test_prerelease_installed_version_counts(self):
        kernel = make_kernel(resolved=[("pkg", "2.0.0rc1")])
        entry = evaluate_kernel(["pkg>=1.9"], kernel)
        assert entry.satisfied == ["pkg>=1.9"]

    def test_unparseable_installed_version_unverified(self):
        kernel = make_kernel(resolved=[("pkg", "not-a-version")])
        entry = evaluate_kernel(["pkg>=1"], kernel)
        assert entry.unverified == ["pkg>=1"]


class TestMatchKernels:
    def test_sorted_best_first(self):
        # requests is in no flavour image, so its absence stays provable.
        deps = ["scikit-learn>=1.0", "requests>=2"]
        full = make_kernel("full", flavour=ImageFlavour.ML, packages=["requests"], resolved=[("requests", "2.32.0")])
        partial = make_kernel("partial", flavour=ImageFlavour.ML)
        none_k = make_kernel("none", flavour=ImageFlavour.BASE)
        entries = match_kernels(deps, [none_k, partial, full])
        assert [e.kernel_id for e in entries] == ["full", "partial", "none"]
        assert [e.level for e in entries] == ["full", "partial", "none"]

    def test_running_full_match_ranks_above_stopped(self):
        idle = make_kernel("idle", flavour=ImageFlavour.ML, state=KernelState.IDLE)
        stopped = make_kernel("stopped", flavour=ImageFlavour.ML, state=KernelState.STOPPED)
        entries = match_kernels(["scikit-learn"], [stopped, idle])
        assert [e.kernel_id for e in entries] == ["idle", "stopped"]

    def test_fewer_missing_ranks_higher(self):
        deps = ["a-pkg", "b-pkg"]
        one = make_kernel("one", packages=["a-pkg"])
        zero_pkgs = make_kernel("zero")
        entries = match_kernels(deps, [zero_pkgs, one])
        assert entries[0].kernel_id == "one"


class TestSuggestKernelConfig:
    def test_ml_inference_and_covered_dropping(self):
        deps = ["scikit-learn>=1.0", "numpy", "pandas>=2.1"]
        config, covered = suggest_kernel_config(deps, set(), "K-Means Cluster")
        assert config.image_flavour == ImageFlavour.ML
        assert config.packages == ["pandas>=2.1"]
        assert covered == ["scikit-learn>=1.0", "numpy"]
        assert config.id == "k-means-cluster-kernel"
        assert config.name == "K-Means Cluster kernel"

    def test_base_when_no_ml_dep(self):
        config, covered = suggest_kernel_config(["requests>=2"], set(), "API Node")
        assert config.image_flavour == ImageFlavour.BASE
        assert config.packages == ["requests>=2"]
        assert covered == []

    def test_unprovable_pin_stays_in_packages(self):
        # A pin above the locked version can't be proven satisfied by the image.
        config, covered = suggest_kernel_config(["scikit-learn>=999"], set(), "Node")
        assert config.image_flavour == ImageFlavour.ML
        assert config.packages == ["scikit-learn>=999"]
        assert covered == []

    def test_id_collision_suffix(self):
        existing = {"my-node-kernel", "my-node-kernel-2"}
        config, _ = suggest_kernel_config([], existing, "My Node")
        assert config.id == "my-node-kernel-3"

    def test_slug_is_docker_safe(self):
        config, _ = suggest_kernel_config([], set(), "Ümlaut / node! (v2.0)")
        import re

        assert re.fullmatch(r"[A-Za-z0-9_-]+", config.id)

    def test_empty_name_falls_back(self):
        config, _ = suggest_kernel_config([], set(), None)
        assert config.id == "custom-node-kernel"
        assert config.name == "Custom node kernel"

    def test_duplicate_deps_deduped(self):
        config, _ = suggest_kernel_config(["requests>=2", "requests>=2"], set(), "N")
        assert config.packages == ["requests>=2"]

    def test_invalid_deps_excluded(self):
        config, covered = suggest_kernel_config(["git+https://x/y", "requests"], set(), "N")
        assert config.packages == ["requests"]
        assert covered == []

    def test_specs_rendered_valid_for_create(self):
        # PEP 508 allows inner whitespace and markers; _validate_packages doesn't.
        from flowfile_core.kernel.manager import _validate_packages

        config, _ = suggest_kernel_config(
            ["pandas >= 2.1", 'httpx-sse==0.4.0; python_version < "3.13"'], set(), "Spacey"
        )
        assert config.packages == ["pandas>=2.1", "httpx-sse==0.4.0"]
        _validate_packages(config.packages)  # must not raise


class TestVerifyKernelForNode:
    def test_missing_blocks_with_formatted_entries(self):
        kernel = make_kernel(name="Bare", packages=["pandas"], resolved=[("pandas", "1.5.0")])
        problems = verify_kernel_for_node(kernel, ["scikit-learn>=1.0", "pandas>=2"])
        assert problems == [
            "scikit-learn>=1.0 (not installed)",
            "pandas>=2 (installed 1.5.0)",
        ]

    @pytest.mark.parametrize(
        "kernel,deps",
        [
            (None, ["scikit-learn>=1.0"]),  # unknown kernel: never block here
            (make_kernel(), []),  # no deps
            (make_kernel(packages=["pandas"]), ["pandas>=2"]),  # unverified version
            (make_kernel(flavour=ImageFlavour.CUSTOM), ["anything"]),  # opaque image
            (make_kernel(), ["git+https://x/y"]),  # invalid spec
        ],
    )
    def test_never_blocks(self, kernel, deps):
        assert verify_kernel_for_node(kernel, deps) == []

    def test_satisfied_passes(self):
        assert verify_kernel_for_node(make_kernel(flavour=ImageFlavour.ML), ["scikit-learn>=1.0"]) == []


def test_parsed_dependency_is_frozen():
    dep = parse_dependency("polars")
    assert isinstance(dep, ParsedDependency)
    with pytest.raises(AttributeError):
        dep.name = "other"
