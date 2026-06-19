"""Unit tests for KernelManager helpers, image GC, and the package-update path.

These tests mock the Docker client end-to-end, so they don't require a Docker
daemon. They cover the surface introduced in the kernel-structure refactor
(image flavours, derived-image bake, PATCH route) which the kernel integration
tests on their own can't verify cheaply.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import MagicMock, patch

import docker.errors
import pytest

from flowfile_core.kernel import manager as kernel_manager
from flowfile_core.kernel.manager import (
    KernelManager,
    _derived_image_tag,
    _friendly_pull_error,
    _resolve_image,
    _resolve_local_image,
    _spec_to_name,
    _validate_custom_image,
    _validate_packages,
    newest_installed_version,
    parse_image_version,
)
from flowfile_core.kernel.models import (
    ImageFlavour,
    KernelInfo,
    KernelState,
    ResolvedPackage,
)


def _bare_manager() -> KernelManager:
    """Construct a KernelManager with a mocked docker client, no real init."""
    with patch.object(KernelManager, "__init__", lambda self, *a, **kw: None):
        mgr = KernelManager.__new__(KernelManager)
        mgr._docker = MagicMock()
        mgr._core_instance_id = "test-core-id"
        mgr._kernels = {}
        mgr._kernel_owners = {}
        mgr._scratch_flow_ids = {}
        mgr._scratch_flow_lock = threading.Lock()
        mgr._shared_volume = "/tmp/test"
        mgr._catalog_tables_dir = "/__catalog_tables_unused__"
        mgr._docker_network = None
        mgr._kernel_volume = None
        mgr._kernel_volume_type = None
        mgr._kernel_mount_target = None
        mgr._catalog_volume = None
        mgr._catalog_volume_type = None
        mgr._catalog_mount_target = None
        mgr._pull_state = {}
        mgr._pull_state_lock = threading.Lock()
    return mgr


def _kernel(
    kernel_id: str = "k1",
    state: KernelState = KernelState.STOPPED,
    packages: list[str] | None = None,
    flavour: ImageFlavour = ImageFlavour.BASE,
    resolved: list[ResolvedPackage] | None = None,
) -> KernelInfo:
    return KernelInfo(
        id=kernel_id,
        name=f"test-{kernel_id}",
        state=state,
        packages=packages or [],
        resolved_packages=resolved or [],
        image_flavour=flavour,
    )


# Pure helpers


class TestValidatePackages:
    def test_accepts_pep508_specs(self):
        _validate_packages(["pandas", "scikit-learn==1.7.2", "polars>=1.0,<2.0", "pkg[extra]"])

    def test_empty_list_is_ok(self):
        _validate_packages([])

    @pytest.mark.parametrize(
        "spec",
        [
            "pandas; rm -rf /",
            "pandas`whoami`",
            "pandas && echo hi",
            "pandas | cat",
            "pandas\n",
            "pandas $(echo)",
            "../etc/passwd",
        ],
    )
    def test_rejects_shell_metacharacters(self, spec):
        with pytest.raises(ValueError, match="Invalid package specifier"):
            _validate_packages([spec])


class TestValidateCustomImage:
    def test_accepts_explicit_tag(self):
        _validate_custom_image("myorg/kernel:1.2.3")

    def test_accepts_registry_with_port_and_tag(self):
        _validate_custom_image("registry.local:5000/team/kernel:1.0")

    def test_accepts_sha256_digest(self):
        _validate_custom_image(
            "myorg/kernel@sha256:abcdef0123456789abcdef0123456789abcdef0123"
        )

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="empty"):
            _validate_custom_image("")
        with pytest.raises(ValueError, match="empty"):
            _validate_custom_image("   ")

    def test_rejects_missing_tag(self):
        with pytest.raises(ValueError, match="no explicit tag"):
            _validate_custom_image("myorg/kernel")

    def test_rejects_empty_tag(self):
        with pytest.raises(ValueError, match="empty after the colon|invalid tag"):
            _validate_custom_image("myorg/kernel:")

    def test_rejects_bad_tag_characters(self):
        with pytest.raises(ValueError, match="invalid tag"):
            _validate_custom_image("myorg/kernel:bad tag")


class TestResolveImage:
    def test_base_returns_default(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE", raising=False)
        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE_BASE", raising=False)
        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE_ML", raising=False)
        assert _resolve_image(ImageFlavour.BASE, None) == kernel_manager._KERNEL_IMAGE_BASE_DEFAULT

    def test_ml_returns_ml_default(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE_ML", raising=False)
        assert _resolve_image(ImageFlavour.ML, None) == kernel_manager._KERNEL_IMAGE_ML_DEFAULT

    def test_env_overrides_default(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_KERNEL_IMAGE_BASE", "ghcr.io/me/base:9.9.9")
        assert _resolve_image(ImageFlavour.BASE, None) == "ghcr.io/me/base:9.9.9"

    def test_empty_env_falls_back_to_default(self, monkeypatch):
        # Compose's ${VAR:-} writes an empty string — treat that as unset.
        monkeypatch.setenv("FLOWFILE_KERNEL_IMAGE_BASE", "")
        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE", raising=False)
        assert _resolve_image(ImageFlavour.BASE, None) == kernel_manager._KERNEL_IMAGE_BASE_DEFAULT

    def test_legacy_envvar_overrides_base(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE_BASE", raising=False)
        monkeypatch.setenv("FLOWFILE_KERNEL_IMAGE", "legacy:1.0")
        assert _resolve_image(ImageFlavour.BASE, None) == "legacy:1.0"

    def test_specific_envvar_wins_over_legacy(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_KERNEL_IMAGE", "legacy:1.0")
        monkeypatch.setenv("FLOWFILE_KERNEL_IMAGE_BASE", "specific:2.0")
        assert _resolve_image(ImageFlavour.BASE, None) == "specific:2.0"

    def test_custom_requires_image(self):
        with pytest.raises(ValueError, match="custom_image must be provided"):
            _resolve_image(ImageFlavour.CUSTOM, None)
        with pytest.raises(ValueError, match="custom_image must be provided"):
            _resolve_image(ImageFlavour.CUSTOM, "")

    def test_custom_validates_tag(self):
        with pytest.raises(ValueError, match="no explicit tag"):
            _resolve_image(ImageFlavour.CUSTOM, "myorg/kernel")

    def test_custom_returns_uri(self):
        assert _resolve_image(ImageFlavour.CUSTOM, "myorg/kernel:1.0") == "myorg/kernel:1.0"

    def test_falls_back_to_local_tag_when_registry_missing(self, monkeypatch):
        """When the registry default isn't local but :local is, prefer :local."""
        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE_LITE", raising=False)
        client = MagicMock()
        client.images.get.side_effect = docker.errors.ImageNotFound("not here")
        local_img = MagicMock()
        local_img.tags = ["flowfile-kernel-lite:local"]
        local_img.attrs = {"Created": "2026-05-14T10:00:00Z"}
        client.images.list.return_value = [local_img]
        assert _resolve_image(ImageFlavour.LITE, None, client) == "flowfile-kernel-lite:local"


class TestResolveLocalImage:
    def test_registry_default_wins_when_present(self):
        client = MagicMock()
        client.images.get.return_value = MagicMock()
        result = _resolve_local_image(
            ImageFlavour.BASE, client, "edwardvaneechoud/flowfile-kernel-base:0.3.0"
        )
        assert result == "edwardvaneechoud/flowfile-kernel-base:0.3.0"
        client.images.list.assert_not_called()

    def test_prefers_local_tag_over_other_variants(self):
        client = MagicMock()
        client.images.get.side_effect = docker.errors.ImageNotFound("nope")
        local_img = MagicMock()
        local_img.tags = ["flowfile-kernel-base:local"]
        local_img.attrs = {"Created": "2026-05-14T10:00:00Z"}
        older_img = MagicMock()
        older_img.tags = ["flowfile-kernel-base:dev-2025"]
        older_img.attrs = {"Created": "2025-01-01T00:00:00Z"}
        client.images.list.return_value = [older_img, local_img]
        result = _resolve_local_image(
            ImageFlavour.BASE, client, "edwardvaneechoud/flowfile-kernel-base:0.3.0"
        )
        assert result == "flowfile-kernel-base:local"

    def test_falls_back_to_newest_when_no_local_tag(self):
        client = MagicMock()
        client.images.get.side_effect = docker.errors.ImageNotFound("nope")
        old = MagicMock()
        old.tags = ["flowfile-kernel-base:dev-2025"]
        old.attrs = {"Created": "2025-01-01T00:00:00Z"}
        newer = MagicMock()
        newer.tags = ["flowfile-kernel-base:dev-2026"]
        newer.attrs = {"Created": "2026-05-14T10:00:00Z"}
        client.images.list.return_value = [old, newer]
        result = _resolve_local_image(
            ImageFlavour.BASE, client, "edwardvaneechoud/flowfile-kernel-base:0.3.0"
        )
        assert result == "flowfile-kernel-base:dev-2026"

    def test_returns_none_when_nothing_local(self):
        client = MagicMock()
        client.images.get.side_effect = docker.errors.ImageNotFound("nope")
        client.images.list.return_value = []
        assert (
            _resolve_local_image(
                ImageFlavour.LITE, client, "edwardvaneechoud/flowfile-kernel-lite:0.3.0"
            )
            is None
        )

    def test_returns_none_on_docker_api_error(self):
        client = MagicMock()
        client.images.get.side_effect = docker.errors.APIError("docker down")
        # The first APIError on .get short-circuits to None without listing
        assert (
            _resolve_local_image(
                ImageFlavour.LITE, client, "edwardvaneechoud/flowfile-kernel-lite:0.3.0"
            )
            is None
        )


class TestImagePull:
    def test_start_pull_is_idempotent(self):
        mgr = _bare_manager()
        with patch("flowfile_core.kernel.manager.threading.Thread") as thread_cls:
            thread_cls.return_value.start.return_value = None
            first = mgr.start_image_pull(ImageFlavour.LITE)
            second = mgr.start_image_pull(ImageFlavour.LITE)
            assert first == "pulling"
            assert second == "pulling"
            assert thread_cls.call_count == 1

    def test_custom_flavour_cannot_be_pulled(self):
        mgr = _bare_manager()
        with pytest.raises(ValueError, match="custom"):
            mgr.start_image_pull(ImageFlavour.CUSTOM)

    def test_do_pull_clears_state_on_success(self):
        mgr = _bare_manager()
        mgr._docker.images.pull.return_value = MagicMock()
        mgr._pull_state["edwardvaneechoud/flowfile-kernel-lite:0.3.0"] = "pulling"
        mgr._do_pull("edwardvaneechoud/flowfile-kernel-lite:0.3.0")
        assert mgr.get_pull_state("edwardvaneechoud/flowfile-kernel-lite:0.3.0") is None

    def test_do_pull_records_error_on_failure(self):
        mgr = _bare_manager()
        mgr._docker.images.pull.side_effect = docker.errors.APIError("registry exploded")
        mgr._pull_state["x:1"] = "pulling"
        mgr._do_pull("x:1")
        state = mgr.get_pull_state("x:1")
        assert state is not None
        assert state.startswith("error:")
        assert "registry exploded" in state


class TestSpecToName:
    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            ("pandas", "pandas"),
            ("Pandas", "pandas"),
            ("scikit_learn", "scikit-learn"),
            ("scikit.learn", "scikit-learn"),
            ("pandas==2.0", "pandas"),
            ("pandas[extra]>=1.0", "pandas"),
            ("  pandas  ", "pandas"),
        ],
    )
    def test_normalises_per_pep503(self, spec, expected):
        assert _spec_to_name(spec) == expected


class TestDerivedImageTag:
    def test_lowercases_and_replaces_unsafe_chars(self):
        assert _derived_image_tag("My Kernel!") == "flowfile-kernel-derived-my-kernel-:latest"

    def test_preserves_safe_chars(self):
        assert _derived_image_tag("k1.test_99-a") == "flowfile-kernel-derived-k1.test_99-a:latest"


# Orphan derived-image GC


class TestImageVersionHelpers:
    def test_parse_image_version_parses_dotted_numeric(self):
        assert parse_image_version("edwardvaneechoud/flowfile-kernel-base:0.3.1") == (0, 3, 1)

    def test_parse_image_version_none_for_non_numeric(self):
        assert parse_image_version("flowfile-kernel-base:local") is None
        assert parse_image_version("noversion") is None

    def test_newest_installed_version_picks_highest(self):
        client = MagicMock()
        v030 = MagicMock()
        v030.tags = ["edwardvaneechoud/flowfile-kernel-base:0.3.0"]
        v031 = MagicMock()
        v031.tags = ["edwardvaneechoud/flowfile-kernel-base:0.3.1"]
        client.images.list.return_value = [v030, v031]
        result = newest_installed_version("edwardvaneechoud/flowfile-kernel-base", client)
        assert result == ("edwardvaneechoud/flowfile-kernel-base:0.3.1", (0, 3, 1))

    def test_newest_installed_version_ignores_local_and_other_repos(self):
        client = MagicMock()
        local = MagicMock()
        local.tags = ["edwardvaneechoud/flowfile-kernel-base:local"]
        other = MagicMock()
        other.tags = ["someoneelse/flowfile-kernel-base:9.9.9"]
        client.images.list.return_value = [local, other]
        assert newest_installed_version("edwardvaneechoud/flowfile-kernel-base", client) is None

    def test_newest_installed_version_none_when_empty(self):
        client = MagicMock()
        client.images.list.return_value = []
        assert newest_installed_version("edwardvaneechoud/flowfile-kernel-base", client) is None


class TestFriendlyPullError:
    def test_not_found_is_clean(self):
        exc = docker.errors.ImageNotFound(
            "404 Client Error for http+docker://localhost/v1.47/images/create: "
            "Not Found (manifest for edwardvaneechoud/flowfile-kernel-ml:0.3.1 not found)"
        )
        msg = _friendly_pull_error(exc, "edwardvaneechoud/flowfile-kernel-ml:0.3.1")
        assert msg == "edwardvaneechoud/flowfile-kernel-ml:0.3.1 isn't available on the registry yet."
        assert "http+docker" not in msg and "404" not in msg

    def test_unauthorized(self):
        exc = docker.errors.APIError("unauthorized: authentication required")
        assert "Not authorized" in _friendly_pull_error(exc, "repo:1.0")

    def test_network(self):
        assert "Check your connection" in _friendly_pull_error(Exception("connection timed out"), "repo:1.0")

    def test_fallback_uses_first_line(self):
        exc = Exception("Some unexpected error\nsecond line of noise")
        assert _friendly_pull_error(exc, "repo:1.0") == "Some unexpected error"


def _image_with_tag(tag: str, labels: dict[str, str] | None = None) -> MagicMock:
    img = MagicMock()
    img.tags = [tag]
    # Derive a sensible default labels dict from the tag so tests written
    # before label-based GC don't have to spell it out.
    if labels is None:
        prefix = "flowfile-kernel-derived-"
        if tag.startswith(prefix):
            safe_kernel_id = tag.split(":", 1)[0][len(prefix) :]
            labels = {
                "flowfile_core_instance": "test-core-id",
                "flowfile_kernel_id": safe_kernel_id,
            }
        else:
            labels = {}
    img.labels = labels
    return img


class TestRemoveOrphanDerivedImages:
    def test_removes_images_with_no_matching_kernel(self):
        mgr = _bare_manager()
        mgr._kernels = {"k1": _kernel("k1")}
        mgr._docker.images.list.return_value = [
            _image_with_tag("flowfile-kernel-derived-k1:latest"),
            _image_with_tag("flowfile-kernel-derived-orphan:latest"),
            _image_with_tag("flowfile-kernel-derived-another-orphan:latest"),
        ]

        mgr._remove_orphan_derived_images()

        removed = {call.args[0] for call in mgr._docker.images.remove.call_args_list}
        assert removed == {
            "flowfile-kernel-derived-orphan:latest",
            "flowfile-kernel-derived-another-orphan:latest",
        }

    def test_keeps_image_for_restored_kernel(self):
        mgr = _bare_manager()
        mgr._kernels = {"k1": _kernel("k1"), "k2": _kernel("k2")}
        mgr._docker.images.list.return_value = [
            _image_with_tag("flowfile-kernel-derived-k1:latest"),
            _image_with_tag("flowfile-kernel-derived-k2:latest"),
        ]

        mgr._remove_orphan_derived_images()

        mgr._docker.images.remove.assert_not_called()

    def test_ignores_unrelated_images(self):
        mgr = _bare_manager()
        mgr._kernels = {}
        mgr._docker.images.list.return_value = [
            _image_with_tag("postgres:15"),
            _image_with_tag("edwardvaneechoud/flowfile-kernel-base:0.3.0"),
            _image_with_tag("python:3.12-slim"),
        ]

        mgr._remove_orphan_derived_images()

        mgr._docker.images.remove.assert_not_called()

    def test_matches_sanitised_kernel_id(self):
        # Kernel ids with characters that get rewritten by _KERNEL_ID_TAG_RE
        # must still match their derived image.
        mgr = _bare_manager()
        mgr._kernels = {"My Kernel!": _kernel("My Kernel!")}
        mgr._docker.images.list.return_value = [
            _image_with_tag("flowfile-kernel-derived-my-kernel-:latest"),
        ]

        mgr._remove_orphan_derived_images()

        mgr._docker.images.remove.assert_not_called()

    def test_handles_docker_api_error_gracefully(self):
        mgr = _bare_manager()
        mgr._kernels = {}
        mgr._docker.images.list.side_effect = docker.errors.APIError("daemon gone")

        # Must not raise.
        mgr._remove_orphan_derived_images()
        mgr._docker.images.remove.assert_not_called()

    def test_continues_after_remove_failure(self):
        mgr = _bare_manager()
        mgr._kernels = {}
        mgr._docker.images.list.return_value = [
            _image_with_tag("flowfile-kernel-derived-bad:latest"),
            _image_with_tag("flowfile-kernel-derived-good:latest"),
        ]
        mgr._docker.images.remove.side_effect = [
            docker.errors.APIError("in use"),
            None,
        ]

        mgr._remove_orphan_derived_images()

        assert mgr._docker.images.remove.call_count == 2


# update_kernel


def _run(coro):
    return asyncio.run(coro)


class TestUpdateKernel:
    def test_unknown_kernel_raises(self):
        mgr = _bare_manager()
        with pytest.raises(KeyError, match="not found"):
            _run(mgr.update_kernel("nope", ["pandas"]))

    @pytest.mark.parametrize(
        "blocking_state",
        [KernelState.IDLE, KernelState.STARTING, KernelState.EXECUTING],
    )
    def test_rejects_edit_while_running(self, blocking_state):
        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1", state=blocking_state)
        with pytest.raises(RuntimeError, match="Stop the kernel first"):
            _run(mgr.update_kernel("k1", ["pandas"]))

    def test_rejects_invalid_package_spec(self):
        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1")
        with pytest.raises(ValueError, match="Invalid package specifier"):
            _run(mgr.update_kernel("k1", ["pandas; rm -rf /"]))

    def test_no_op_when_packages_unchanged(self):
        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1", packages=["pandas"])
        mgr._build_derived_image = MagicMock(return_value="should-not-be-called")
        mgr._persist_kernel = MagicMock()

        result = _run(mgr.update_kernel("k1", ["pandas"]))

        assert result.packages == ["pandas"]
        mgr._build_derived_image.assert_not_called()
        mgr._persist_kernel.assert_not_called()

    def test_happy_path_rebuilds_and_persists(self):
        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1", packages=["pandas"])
        mgr._kernel_owners["k1"] = 42
        mgr._build_derived_image = MagicMock(
            return_value="flowfile-kernel-derived-k1:latest"
        )
        mgr._resolve_installed_versions = MagicMock(
            return_value=[
                ResolvedPackage(name="numpy", version="2.0.0"),
                ResolvedPackage(name="matplotlib", version="3.8.0"),
            ]
        )
        mgr._remove_derived_image = MagicMock()
        mgr._persist_kernel = MagicMock()

        result = _run(mgr.update_kernel("k1", ["numpy", "matplotlib"]))

        assert result.packages == ["numpy", "matplotlib"]
        assert [p.name for p in result.resolved_packages] == ["numpy", "matplotlib"]
        mgr._remove_derived_image.assert_called_once_with("k1")
        mgr._build_derived_image.assert_called_once()
        mgr._persist_kernel.assert_called_once()
        assert mgr._persist_kernel.call_args.args[1] == 42

    def test_clears_packages_when_set_to_empty(self):
        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel(
            "k1",
            packages=["pandas"],
            resolved=[ResolvedPackage(name="pandas", version="2.3.0")],
        )
        mgr._kernel_owners["k1"] = 42
        mgr._build_derived_image = MagicMock()
        mgr._resolve_installed_versions = MagicMock()
        mgr._remove_derived_image = MagicMock()
        mgr._persist_kernel = MagicMock()

        result = _run(mgr.update_kernel("k1", []))

        assert result.packages == []
        assert result.resolved_packages == []
        mgr._remove_derived_image.assert_called_once_with("k1")
        mgr._build_derived_image.assert_not_called()
        mgr._resolve_installed_versions.assert_not_called()
        mgr._persist_kernel.assert_called_once()

    def test_rollback_restores_old_packages_on_build_failure(self):
        old_resolved = [ResolvedPackage(name="pandas", version="2.3.0")]
        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1", packages=["pandas"], resolved=old_resolved)
        mgr._kernel_owners["k1"] = 42

        build_calls: list[list[str]] = []

        def fake_build(kernel: KernelInfo) -> str:
            # First call (with new packages) fails; second call (rollback to
            # old packages) succeeds.
            build_calls.append(list(kernel.packages))
            if kernel.packages == ["numpy", "matplotlib"]:
                raise RuntimeError("pip install exploded")
            return "flowfile-kernel-derived-k1:latest"

        mgr._build_derived_image = MagicMock(side_effect=fake_build)
        mgr._resolve_installed_versions = MagicMock(return_value=[])
        mgr._remove_derived_image = MagicMock()
        mgr._persist_kernel = MagicMock()

        with pytest.raises(ValueError, match="Failed to update kernel image"):
            _run(mgr.update_kernel("k1", ["numpy", "matplotlib"]))

        kernel = mgr._kernels["k1"]
        assert kernel.packages == ["pandas"]
        assert kernel.resolved_packages == old_resolved
        assert build_calls == [["numpy", "matplotlib"], ["pandas"]]
        # We don't re-persist on failure — caller already has stale-but-correct
        # state in the DB.
        mgr._persist_kernel.assert_not_called()


# Sanity check: __init__'s startup sequence now calls the GC


class TestStartupSequence:
    def test_init_calls_orphan_gc(self, monkeypatch):
        """Document the contract: any new startup helper must be wired in."""
        # We use a real __init__ but stub out everything it touches.
        monkeypatch.setattr(kernel_manager.docker, "from_env", lambda: MagicMock())
        monkeypatch.setattr(KernelManager, "_detect_docker_network", lambda self: None)
        monkeypatch.setattr(
            KernelManager,
            "_discover_volume_for_path",
            lambda self, path: (None, None, None),
        )

        called: list[str] = []
        monkeypatch.setattr(
            KernelManager,
            "_restore_kernels_from_db",
            lambda self: called.append("restore"),
        )
        monkeypatch.setattr(
            KernelManager,
            "_reclaim_running_containers",
            lambda self: called.append("reclaim"),
        )
        monkeypatch.setattr(
            KernelManager,
            "_remove_orphan_derived_images",
            lambda self: called.append("gc"),
        )

        KernelManager(shared_volume_path="/tmp/x")

        assert called == ["restore", "reclaim", "gc"]


# Container liveness + execute self-heal


class TestContainerLiveness:
    def test_running_container_is_alive(self):
        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1", state=KernelState.IDLE)
        container = MagicMock()
        container.status = "running"
        mgr._docker.containers.get.return_value = container
        assert mgr._is_container_running("k1") is True

    def test_exited_container_is_down(self):
        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1", state=KernelState.IDLE)
        container = MagicMock()
        container.status = "exited"
        mgr._docker.containers.get.return_value = container
        assert mgr._is_container_running("k1") is False

    def test_missing_container_is_down(self):
        import docker

        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1", state=KernelState.IDLE)
        mgr._docker.containers.get.side_effect = docker.errors.NotFound("gone")
        assert mgr._is_container_running("k1") is False

    def test_transient_docker_error_assumed_alive(self):
        import docker

        mgr = _bare_manager()
        mgr._kernels["k1"] = _kernel("k1", state=KernelState.IDLE)
        mgr._docker.containers.get.side_effect = docker.errors.DockerException("boom")
        assert mgr._is_container_running("k1") is True


class TestExecuteKernelDown:
    def test_marks_stopped_with_clear_error_when_container_gone(self, monkeypatch):
        import docker
        import httpx

        from flowfile_core.kernel.manager import _KERNEL_DOWN_MSG
        from flowfile_core.kernel.models import ExecuteRequest

        mgr = _bare_manager()
        k = _kernel("k1", state=KernelState.IDLE)
        k.port = 19000
        k.container_id = "abc"
        mgr._kernels["k1"] = k
        # Container is gone — both the OOM probe and the liveness probe see NotFound.
        mgr._docker.containers.get.side_effect = docker.errors.NotFound("gone")

        class _FailingClient:
            def __init__(self, *a, **kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def post(self, *a, **kw):
                raise httpx.ConnectError("connection failed")

        monkeypatch.setattr(httpx, "AsyncClient", _FailingClient)

        req = ExecuteRequest(node_id=1, code="1", source_registration_id=1)
        result = _run(mgr.execute("k1", req))

        assert result.success is False
        assert result.error == _KERNEL_DOWN_MSG
        assert k.state == KernelState.STOPPED
        assert k.container_id is None
