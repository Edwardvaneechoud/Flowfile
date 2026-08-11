"""Docker-backed checks that kernel dependency detection tells the truth.

Everything else about dependency matching is unit-tested against the shipped
manifest. This suite asks the question those tests structurally cannot: does the
manifest actually describe the image we launch?

That gap is exactly where the original bug lived — matching was self-consistent
and green while the data it read described nothing that shipped. So these tests
compare the manifest against a real ``pip list`` inside a real image, and drive
the run-time gate against a real running kernel.

Requires Docker. Slow: the image build and a derived-image bake dominate.
"""

from __future__ import annotations

import json
import subprocess

import pytest
from packaging.utils import canonicalize_name

from flowfile_core.kernel import flavours, matching
from flowfile_core.kernel.models import ImageFlavour, KernelInfo

pytestmark = [pytest.mark.kernel, pytest.mark.slow]

# Absent from the base image on purpose — they are the packages the unit tests
# rely on being *provably* missing, so a silent change here would hollow out the
# whole gate without failing anything else.
_EXPECTED_ABSENT_FROM_BASE = ("scikit-learn", "xgboost", "lightgbm", "statsmodels", "pandas")
_EXPECTED_PRESENT_IN_ML = ("scikit-learn", "xgboost", "lightgbm", "statsmodels", "pandas")

_BUILD_ARGS = {ImageFlavour.BASE: [], ImageFlavour.ML: ["--build-arg", "EXTRAS=ml"]}


def _image_exists(tag: str) -> bool:
    return subprocess.run(["docker", "image", "inspect", tag], capture_output=True).returncode == 0


def _resolve_or_build(flavour: ImageFlavour) -> str:
    """Pinned image if it's already local, else the dev tag, else build it."""
    from flowfile_core.kernel.images import _flavour_images

    pinned = _flavour_images()[flavour]
    if _image_exists(pinned):
        return pinned
    local = f"flowfile-kernel-{flavour.value}:local"
    if _image_exists(local):
        return local

    from tests.kernel_fixtures import _REPO_ROOT  # noqa: PLC0415

    context = _REPO_ROOT / "kernel_runtime"
    result = subprocess.run(
        ["docker", "build", "-t", local, *_BUILD_ARGS[flavour], "-f", str(context / "Dockerfile"), str(context)],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if result.returncode != 0:
        pytest.skip(f"could not obtain a {flavour.value} kernel image: {result.stderr[-2000:]}")
    return local


def _installed_packages(tag: str) -> dict[str, str]:
    result = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "pip", tag, "list", "--format=json", "--disable-pip-version-check"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        pytest.skip(f"could not read the package list from {tag}: {result.stderr[-2000:]}")
    return {canonicalize_name(row["name"]): row["version"] for row in json.loads(result.stdout)}


@pytest.fixture(scope="module")
def base_image() -> str:
    return _resolve_or_build(ImageFlavour.BASE)


@pytest.fixture(scope="module")
def base_installed(base_image) -> dict[str, str]:
    return _installed_packages(base_image)


@pytest.fixture(scope="module")
def ml_installed() -> dict[str, str]:
    return _installed_packages(_resolve_or_build(ImageFlavour.ML))


def _manifest_for(flavour: ImageFlavour) -> dict[str, str | None]:
    contents = flavours.flavour_contents()
    assert contents is not None, "no kernel image manifest — run 'make kernel_manifest'"
    assert flavour in contents, f"the manifest does not describe the {flavour.value} image"
    return contents[flavour]


class TestManifestMatchesTheImage:
    """The generator reads the lockfile; the image is built from it. Prove they agree."""

    @pytest.mark.parametrize("flavour", [ImageFlavour.BASE, ImageFlavour.ML])
    def test_every_confidently_claimed_package_is_installed(self, flavour, request):
        # A pinned entry is a positive claim, and a wrong one produces a false
        # "satisfied". A null entry only says "may be present" (arch-dependent
        # deps like nvidia-nccl-cu12), which is unfalsifiable on one arch by design.
        installed = request.getfixturevalue("base_installed" if flavour is ImageFlavour.BASE else "ml_installed")
        claimed = _manifest_for(flavour)
        absent = sorted(name for name, version in claimed.items() if version is not None and name not in installed)
        assert not absent, (
            f"the manifest claims the {flavour.value} image ships {absent}, but pip does not list them. "
            "Run 'make kernel_manifest'; if it is already current, the Dockerfile and the lockfile disagree."
        )

    def test_arch_dependent_packages_are_never_claimed_with_a_version(self, ml_installed):
        # The images are multi-arch, so a dep gated on platform_machine is present
        # in one and absent in the other. Recording a version would make it
        # provable — and therefore wrong on half the fleet.
        nccl = _manifest_for(ImageFlavour.ML).get("nvidia-nccl-cu12")
        assert nccl is None
        kernel = KernelInfo(id="k", name="K", image_flavour=ImageFlavour.ML)
        entry = matching.evaluate_kernel(["nvidia-nccl-cu12>=2"], kernel)
        assert entry.missing == []
        assert entry.unverified == ["nvidia-nccl-cu12>=2"]

    def test_no_package_is_claimed_that_can_never_be_installed(self, base_installed):
        # colorama (Windows) and exceptiongroup (python < 3.11) are in the lock's
        # graph but can never reach a Linux/py3.12 image. Claiming them would make
        # a dependency on them report as satisfied when it is genuinely absent.
        base = _manifest_for(ImageFlavour.BASE)
        for name in ("colorama", "exceptiongroup"):
            assert name not in base
            assert name not in base_installed

    @pytest.mark.parametrize("flavour", [ImageFlavour.BASE, ImageFlavour.ML])
    def test_pinned_versions_are_exact(self, flavour, request):
        installed = request.getfixturevalue("base_installed" if flavour is ImageFlavour.BASE else "ml_installed")
        wrong = {
            name: (version, installed[name])
            for name, version in _manifest_for(flavour).items()
            if version is not None and name in installed and installed[name] != version
        }
        assert not wrong, (
            f"the {flavour.value} manifest pins versions the image does not have "
            f"(manifest, installed): {wrong}. A wrong version here produces false "
            "'missing' verdicts on pinned dependencies."
        )

    def test_lite_only_promises_versions_it_pins(self):
        # Lite installs base's packages but trims /opt/constraints.txt, so anything
        # outside the whitelist may float — the manifest must not claim a version.
        lite = _manifest_for(ImageFlavour.LITE)
        assert lite["polars"] is not None
        assert lite["numpy"] is None
        assert lite["pyarrow"] is None


class TestAbsenceIsReal:
    """A `missing` verdict is only honest if the package is genuinely not there."""

    @pytest.mark.parametrize("package", _EXPECTED_ABSENT_FROM_BASE)
    def test_base_image_really_lacks_it(self, package, base_installed):
        assert canonicalize_name(package) not in base_installed
        entry = matching.evaluate_kernel(
            [package], KernelInfo(id="k", name="K", image_flavour=ImageFlavour.BASE)
        )
        assert entry.missing == [canonicalize_name(package)]
        assert entry.level == "none"

    @pytest.mark.parametrize("package", _EXPECTED_PRESENT_IN_ML)
    def test_ml_image_really_has_it(self, package, ml_installed):
        assert canonicalize_name(package) in ml_installed
        entry = matching.evaluate_kernel(
            [package], KernelInfo(id="k", name="K", image_flavour=ImageFlavour.ML)
        )
        assert entry.missing == []
        assert entry.level == "full"

    def test_a_real_version_pin_the_image_fails_is_missing(self, base_installed):
        # numpy is pinned to 1.26.x in the image, so >=2 is a genuine mismatch —
        # the case that used to render as a green check in every packaged install.
        assert base_installed["numpy"].startswith("1.")
        entry = matching.evaluate_kernel(
            ["numpy>=2"], KernelInfo(id="k", name="K", image_flavour=ImageFlavour.BASE)
        )
        assert entry.missing == ["numpy>=2"]
        assert entry.details["numpy>=2"] == f"installed {base_installed['numpy']}"


class TestRunTimeGate:
    """What flow_graph asks before executing a kernel-environment custom node."""

    def test_blocks_a_provably_missing_dependency(self):
        kernel = KernelInfo(id="k", name="K", image_flavour=ImageFlavour.BASE)
        problems = matching.verify_kernel_for_node(kernel, ["scikit-learn>=1.5"])
        assert problems == ["scikit-learn>=1.5 (not installed)"]

    def test_does_not_block_what_the_image_has(self):
        kernel = KernelInfo(id="k", name="K", image_flavour=ImageFlavour.ML)
        assert matching.verify_kernel_for_node(kernel, ["scikit-learn>=1.0", "polars"]) == []

    def test_never_blocks_an_unenumerable_image(self):
        # A custom image can't be inspected, so a run must proceed and let the
        # real ImportError speak rather than refusing on a guess.
        kernel = KernelInfo(
            id="k", name="K", image_flavour=ImageFlavour.CUSTOM, custom_image="someone/theirs:1.0"
        )
        assert matching.verify_kernel_for_node(kernel, ["scikit-learn>=1.5"]) == []
        assert matching.evaluate_kernel(["scikit-learn>=1.5"], kernel).level == "unknown"


class TestBakedKernelOverridesTheManifest:
    """A derived image adds packages on top; resolved_packages must win."""

    def test_baking_a_package_flips_missing_to_satisfied(self):
        from tests.kernel_fixtures import managed_kernel  # noqa: PLC0415

        before = KernelInfo(id="k", name="K", image_flavour=ImageFlavour.BASE)
        assert matching.verify_kernel_for_node(before, ["scikit-learn>=1.5"])

        with managed_kernel(packages=["scikit-learn>=1.5"]) as (manager, kernel_id):
            baked = manager.get_kernel_sync(kernel_id)
            assert baked is not None
            resolved = {canonicalize_name(p.name): p.version for p in baked.resolved_packages}
            assert "scikit-learn" in resolved, (
                "create_kernel did not record the baked version; matching would fall back "
                "to the base manifest and keep reporting the package as missing"
            )
            assert matching.verify_kernel_for_node(baked, ["scikit-learn>=1.5"]) == []
            entry = matching.evaluate_kernel(["scikit-learn>=1.5"], baked)
            assert entry.level == "full"
            assert entry.satisfied == ["scikit-learn>=1.5"]
            assert entry.details["scikit-learn>=1.5"] == f"installed {resolved['scikit-learn']}"
