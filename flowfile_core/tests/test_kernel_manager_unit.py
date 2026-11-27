"""Unit tests for KernelManager helpers, image GC, and the package-update path.

These tests mock the Docker client end-to-end, so they don't require a Docker
daemon. They cover the surface introduced in the kernel-structure refactor
(image flavours, derived-image bake, PATCH route) which the kernel integration
tests on their own can't verify cheaply.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
import uuid
from unittest.mock import MagicMock, patch

import docker.errors
import httpx
import pytest
from fastapi import HTTPException
from packaging.version import Version

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
    ExecuteRequest,
    ImageFlavour,
    KernelConfig,
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
        mgr._runtime_id = "test-runtime-id"
        mgr._started_here = set()
        mgr._kernels = {}
        mgr._kernel_owners = {}
        mgr._kernels_lock = threading.RLock()
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
        mgr._start_flights = {}
        mgr._start_flights_lock = threading.Lock()
        mgr._build_locks = {}
        mgr._build_locks_lock = threading.Lock()
        mgr._exec_locks = {}
        mgr._exec_locks_lock = threading.Lock()
        mgr._active_execs = {}
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
        _validate_custom_image("myorg/kernel@sha256:abcdef0123456789abcdef0123456789abcdef0123")

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

    def test_notebook_is_tagged_by_the_app_version(self, monkeypatch):
        from shared._version import get_version

        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE_NOTEBOOK", raising=False)
        assert _resolve_image(ImageFlavour.NOTEBOOK, None) == f"edwardvaneechoud/flowfile-kernel-notebook:{get_version()}"

    def test_notebook_env_override(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_KERNEL_IMAGE_NOTEBOOK", "flowfile-kernel-notebook:local")
        assert _resolve_image(ImageFlavour.NOTEBOOK, None) == "flowfile-kernel-notebook:local"

    def test_notebook_falls_back_to_local_build(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_KERNEL_IMAGE_NOTEBOOK", raising=False)
        client = MagicMock()
        client.images.get.side_effect = docker.errors.ImageNotFound("not here")
        local_img = MagicMock()
        local_img.tags = ["flowfile-kernel-notebook:local"]
        local_img.attrs = {"Created": "2026-10-01T10:00:00Z"}
        client.images.list.return_value = [local_img]
        assert _resolve_image(ImageFlavour.NOTEBOOK, None, client) == "flowfile-kernel-notebook:local"

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
        result = _resolve_local_image(ImageFlavour.BASE, client, "edwardvaneechoud/flowfile-kernel-base:0.3.0")
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
        result = _resolve_local_image(ImageFlavour.BASE, client, "edwardvaneechoud/flowfile-kernel-base:0.3.0")
        assert result == "flowfile-kernel-base:local"

    def test_skips_local_retag_of_older_release(self):
        """A :local tag that is a retag of an older release must not shadow the newer pin."""
        client = MagicMock()
        client.images.get.side_effect = docker.errors.ImageNotFound("nope")
        stale = MagicMock()
        stale.tags = ["edwardvaneechoud/flowfile-kernel-lite:0.3.2", "flowfile-kernel-lite:local"]
        stale.attrs = {"Created": "2026-06-06T14:50:09Z"}
        client.images.list.return_value = [stale]
        result = _resolve_local_image(ImageFlavour.LITE, client, "edwardvaneechoud/flowfile-kernel-lite:0.6.0")
        assert result is None

    def test_keeps_local_retag_of_newer_release(self):
        client = MagicMock()
        client.images.get.side_effect = docker.errors.ImageNotFound("nope")
        fresh = MagicMock()
        fresh.tags = ["edwardvaneechoud/flowfile-kernel-lite:0.6.0", "flowfile-kernel-lite:local"]
        fresh.attrs = {"Created": "2026-09-01T00:00:00Z"}
        client.images.list.return_value = [fresh]
        result = _resolve_local_image(ImageFlavour.LITE, client, "edwardvaneechoud/flowfile-kernel-lite:0.6.0")
        assert result == "flowfile-kernel-lite:local"

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
        result = _resolve_local_image(ImageFlavour.BASE, client, "edwardvaneechoud/flowfile-kernel-base:0.3.0")
        assert result == "flowfile-kernel-base:dev-2026"

    def test_returns_none_when_nothing_local(self):
        client = MagicMock()
        client.images.get.side_effect = docker.errors.ImageNotFound("nope")
        client.images.list.return_value = []
        assert _resolve_local_image(ImageFlavour.LITE, client, "edwardvaneechoud/flowfile-kernel-lite:0.3.0") is None

    def test_returns_none_on_docker_api_error(self):
        client = MagicMock()
        client.images.get.side_effect = docker.errors.APIError("docker down")
        # The first APIError on .get short-circuits to None without listing
        assert _resolve_local_image(ImageFlavour.LITE, client, "edwardvaneechoud/flowfile-kernel-lite:0.3.0") is None


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


_LITE_TAG = "edwardvaneechoud/flowfile-kernel-lite:0.6.1"


def _missing_until_pulled(mgr: KernelManager):
    """images.get side effect: every tag is absent until images.pull has run."""

    def _images_get(tag):
        if not mgr._docker.images.pull.called:
            raise docker.errors.ImageNotFound(tag)
        return MagicMock()

    return _images_get


class TestEnsureImage:
    def test_noop_when_image_present(self):
        mgr = _bare_manager()
        mgr._ensure_image(_LITE_TAG)
        mgr._docker.images.pull.assert_not_called()
        assert mgr.get_pull_state(_LITE_TAG) is None

    def test_pulls_missing_image_inline(self):
        mgr = _bare_manager()
        mgr._docker.images.get.side_effect = _missing_until_pulled(mgr)
        mgr._ensure_image(_LITE_TAG)
        mgr._docker.images.pull.assert_called_once_with("edwardvaneechoud/flowfile-kernel-lite", tag="0.6.1")
        assert mgr.get_pull_state(_LITE_TAG) is None

    def test_pulls_registry_port_ref_with_repo_and_tag_intact(self):
        mgr = _bare_manager()
        mgr._docker.images.get.side_effect = _missing_until_pulled(mgr)
        mgr._ensure_image("localhost:5000/team/kernel:1.2.3")
        mgr._docker.images.pull.assert_called_once_with("localhost:5000/team/kernel", tag="1.2.3")

    def test_pulls_digest_ref_as_sha256_tag(self):
        mgr = _bare_manager()
        mgr._docker.images.get.side_effect = _missing_until_pulled(mgr)
        digest = "sha256:" + "a" * 64
        mgr._ensure_image(f"myorg/kernel@{digest}")
        mgr._docker.images.pull.assert_called_once_with("myorg/kernel", tag=digest)

    def test_pull_failure_raises_friendly_message_and_keeps_state(self):
        mgr = _bare_manager()
        mgr._docker.images.get.side_effect = docker.errors.ImageNotFound("nope")
        mgr._docker.images.pull.side_effect = docker.errors.APIError("registry exploded")

        with pytest.raises(RuntimeError) as excinfo:
            mgr._ensure_image("x:1")

        assert str(excinfo.value) == _friendly_pull_error(docker.errors.APIError("registry exploded"), "x:1")
        assert not str(excinfo.value).startswith("error:")
        assert mgr.get_pull_state("x:1") == f"error:{excinfo.value}"

    def test_retries_after_an_earlier_failed_pull(self):
        mgr = _bare_manager()
        mgr._pull_state[_LITE_TAG] = "error:stale"
        mgr._docker.images.get.side_effect = _missing_until_pulled(mgr)
        mgr._ensure_image(_LITE_TAG)
        mgr._docker.images.pull.assert_called_once()
        assert mgr.get_pull_state(_LITE_TAG) is None

    def test_waits_for_background_pull_instead_of_pulling(self):
        mgr = _bare_manager()
        mgr._pull_state[_LITE_TAG] = "pulling"

        def _images_get(tag):
            if mgr.get_pull_state(tag) == "pulling":
                raise docker.errors.ImageNotFound(tag)
            return MagicMock()

        mgr._docker.images.get.side_effect = _images_get
        sleeps: list[float] = []

        def _background_pull_finishes(seconds):
            sleeps.append(seconds)
            with mgr._pull_state_lock:
                mgr._pull_state.pop(_LITE_TAG, None)

        fake_time = MagicMock()
        fake_time.sleep.side_effect = _background_pull_finishes
        with patch.object(kernel_manager, "time", fake_time):
            mgr._ensure_image(_LITE_TAG)

        assert sleeps == [0.5]
        mgr._docker.images.pull.assert_not_called()

    def test_surfaces_background_pull_failure_without_pulling(self):
        mgr = _bare_manager()
        mgr._pull_state[_LITE_TAG] = "pulling"
        mgr._docker.images.get.side_effect = docker.errors.ImageNotFound("nope")

        def _background_pull_fails(_seconds):
            with mgr._pull_state_lock:
                mgr._pull_state[_LITE_TAG] = "error:boom"

        fake_time = MagicMock()
        fake_time.sleep.side_effect = _background_pull_fails
        with patch.object(kernel_manager, "time", fake_time):
            with pytest.raises(RuntimeError, match="^boom$"):
                mgr._ensure_image(_LITE_TAG)

        mgr._docker.images.pull.assert_not_called()
        assert mgr.get_pull_state(_LITE_TAG) == "error:boom"

    def test_build_pulls_missing_base_image_before_building(self):
        mgr = _bare_manager()
        kernel = _kernel(packages=["xgboost"], flavour=ImageFlavour.LITE)
        derived_tag = _derived_image_tag(kernel.id)
        missing_until_pulled = _missing_until_pulled(mgr)

        def _images_get(tag):
            if tag == derived_tag:
                raise docker.errors.ImageNotFound(tag)
            return missing_until_pulled(tag)

        mgr._docker.images.get.side_effect = _images_get

        assert mgr._build_derived_image_locked(kernel, _LITE_TAG, derived_tag) == derived_tag

        mgr._docker.images.pull.assert_called_once_with("edwardvaneechoud/flowfile-kernel-lite", tag="0.6.1")
        mgr._docker.images.build.assert_called_once()
        assert mgr._docker.images.build.call_args.kwargs["tag"] == derived_tag

    def test_build_surfaces_pull_failure(self):
        mgr = _bare_manager()
        kernel = _kernel(packages=["xgboost"], flavour=ImageFlavour.LITE)
        mgr._docker.images.get.side_effect = docker.errors.ImageNotFound("nope")
        mgr._docker.images.pull.side_effect = docker.errors.APIError("registry exploded")

        with pytest.raises(RuntimeError, match="^registry exploded$"):
            mgr._build_derived_image_locked(kernel, _LITE_TAG, _derived_image_tag(kernel.id))

        mgr._docker.images.build.assert_not_called()


class TestDerivedImageBase:
    """A derived image is reused only while it was baked on the base the pin names today."""

    def _existing(self, mgr: KernelManager, base_label: str | None):
        derived = MagicMock()
        derived.labels = {} if base_label is None else {kernel_manager._IMAGE_LABEL_BASE_IMAGE: base_label}
        mgr._docker.images.get.return_value = derived
        return derived

    def test_reused_when_built_on_the_same_base(self):
        mgr = _bare_manager()
        kernel = _kernel(packages=["xgboost"], flavour=ImageFlavour.LITE)
        self._existing(mgr, _LITE_TAG)

        assert mgr._build_derived_image_locked(kernel, _LITE_TAG, _derived_image_tag(kernel.id))
        mgr._docker.images.build.assert_not_called()
        mgr._docker.images.remove.assert_not_called()

    @pytest.mark.parametrize("stale_label", ["edwardvaneechoud/flowfile-kernel-lite:0.6.0", None])
    def test_rebuilt_when_the_pin_moved(self, stale_label):
        mgr = _bare_manager()
        kernel = _kernel(packages=["xgboost"], flavour=ImageFlavour.LITE)
        derived_tag = _derived_image_tag(kernel.id)
        self._existing(mgr, stale_label)
        dockerfiles: list[str] = []
        mgr._docker.images.build.side_effect = lambda path, **_: dockerfiles.append(
            (open(os.path.join(path, "Dockerfile")).read())
        )

        assert mgr._build_derived_image_locked(kernel, _LITE_TAG, derived_tag) == derived_tag

        mgr._docker.images.remove.assert_called_once_with(derived_tag, force=True)
        assert len(dockerfiles) == 1
        assert f'LABEL {kernel_manager._IMAGE_LABEL_BASE_IMAGE}="{_LITE_TAG}"' in dockerfiles[0]


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
        assert parse_image_version("edwardvaneechoud/flowfile-kernel-base:0.3.1") == Version("0.3.1")

    def test_parse_image_version_orders_prereleases_below_their_release(self):
        rc = parse_image_version("edwardvaneechoud/flowfile-kernel-notebook:0.24.0-rc.1")
        assert rc == Version("0.24.0rc1")
        assert parse_image_version("edwardvaneechoud/flowfile-kernel-notebook:0.23.3") < rc
        assert rc < parse_image_version("edwardvaneechoud/flowfile-kernel-notebook:0.24.0")

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
        assert result == ("edwardvaneechoud/flowfile-kernel-base:0.3.1", Version("0.3.1"))

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
    # Older than the GC grace window, so these stay reapable by default.
    img.attrs = {"Created": "2020-01-01T00:00:00.000000000Z"}
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
        mgr._build_derived_image = MagicMock(return_value="flowfile-kernel-derived-k1:latest")
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


def _config(kernel_id: str = "k1", packages: list[str] | None = None) -> KernelConfig:
    return KernelConfig(id=kernel_id, name=f"test-{kernel_id}", packages=packages or [])


def _create_manager() -> KernelManager:
    mgr = _bare_manager()
    mgr._persist_kernel = MagicMock()
    mgr._provision_scratch_flow = MagicMock()
    return mgr


class TestCreateKernel:
    def test_create_without_packages_registers_stopped(self):
        mgr = _create_manager()
        mgr._build_derived_image = MagicMock(return_value="should-not-be-called")

        kernel = _run(mgr.create_kernel(_config(), user_id=42))

        assert kernel.state == KernelState.STOPPED
        assert mgr._kernels["k1"] is kernel
        assert mgr._kernel_owners["k1"] == 42
        mgr._build_derived_image.assert_not_called()
        mgr._docker.images.pull.assert_not_called()
        mgr._persist_kernel.assert_called_once()
        mgr._provision_scratch_flow.assert_called_once_with("k1", 42)

    def test_create_without_packages_pulls_missing_image(self):
        mgr = _create_manager()
        mgr._docker.images.get.side_effect = _missing_until_pulled(mgr)
        mgr._docker.images.list.return_value = []
        image = kernel_manager._flavour_images()[ImageFlavour.BASE]

        kernel = _run(mgr.create_kernel(_config(), user_id=42))

        repo, _, tag = image.partition(":")
        mgr._docker.images.pull.assert_called_once_with(repo, tag=tag)
        assert kernel.state == KernelState.STOPPED
        assert mgr._kernels["k1"] is kernel
        assert mgr.get_pull_state(image) is None
        mgr._persist_kernel.assert_called_once()

    def test_create_custom_image_with_registry_port_pulls_exact_ref(self):
        mgr = _create_manager()
        mgr._docker.images.get.side_effect = _missing_until_pulled(mgr)
        config = KernelConfig(
            id="k1",
            name="test-k1",
            image_flavour=ImageFlavour.CUSTOM,
            custom_image="localhost:5000/team/kernel:1.2.3",
        )

        kernel = _run(mgr.create_kernel(config, user_id=42))

        mgr._docker.images.pull.assert_called_once_with("localhost:5000/team/kernel", tag="1.2.3")
        assert kernel.state == KernelState.STOPPED

    def test_create_without_packages_surfaces_pull_failure(self):
        mgr = _create_manager()
        mgr._docker.images.get.side_effect = docker.errors.ImageNotFound("nope")
        mgr._docker.images.list.return_value = []
        mgr._docker.images.pull.side_effect = docker.errors.APIError("registry exploded")

        with pytest.raises(ValueError, match="^Failed to prepare kernel image: registry exploded$"):
            _run(mgr.create_kernel(_config(), user_id=42))

        assert mgr._kernels == {}
        assert mgr._kernel_owners == {}
        mgr._persist_kernel.assert_not_called()

    def test_kernel_is_listed_as_creating_during_build(self):
        mgr = _create_manager()
        started = threading.Event()
        release = threading.Event()

        def slow_build(kernel):
            started.set()
            assert release.wait(timeout=5)
            return "derived:tag"

        mgr._build_derived_image = MagicMock(side_effect=slow_build)
        mgr._resolve_installed_versions = MagicMock(return_value=[ResolvedPackage(name="xgboost", version="2.0.0")])

        async def scenario():
            task = asyncio.ensure_future(mgr.create_kernel(_config(packages=["xgboost"]), user_id=42))
            await asyncio.to_thread(started.wait, 5)
            listed = await mgr.list_kernels(user_id=42)
            assert [(k.id, k.state) for k in listed] == [("k1", KernelState.CREATING)]
            release.set()
            return await task

        kernel = asyncio.run(scenario())
        assert kernel.state == KernelState.STOPPED
        assert kernel.resolved_packages == [ResolvedPackage(name="xgboost", version="2.0.0")]

    def test_duplicate_create_rejected_while_building(self):
        mgr = _create_manager()
        started = threading.Event()
        release = threading.Event()

        def slow_build(kernel):
            started.set()
            assert release.wait(timeout=5)
            return "derived:tag"

        mgr._build_derived_image = MagicMock(side_effect=slow_build)
        mgr._resolve_installed_versions = MagicMock(return_value=[])

        async def scenario():
            task = asyncio.ensure_future(mgr.create_kernel(_config(packages=["xgboost"]), user_id=42))
            await asyncio.to_thread(started.wait, 5)
            with pytest.raises(ValueError, match="already exists"):
                await mgr.create_kernel(_config(packages=["numpy"]), user_id=42)
            release.set()
            await task

        asyncio.run(scenario())
        assert mgr._build_derived_image.call_count == 1

    def test_failed_build_removes_creating_kernel(self):
        mgr = _create_manager()
        mgr._build_derived_image = MagicMock(side_effect=RuntimeError("pip exploded"))

        with pytest.raises(ValueError, match="pip exploded"):
            _run(mgr.create_kernel(_config(packages=["xgboost"]), user_id=42))

        assert mgr._kernels == {}
        assert mgr._kernel_owners == {}
        mgr._persist_kernel.assert_not_called()

    def test_cancelled_create_removes_creating_kernel(self):
        mgr = _create_manager()
        started = threading.Event()
        release = threading.Event()

        def slow_build(kernel):
            started.set()
            assert release.wait(timeout=5)
            return "derived:tag"

        mgr._build_derived_image = MagicMock(side_effect=slow_build)

        async def scenario():
            task = asyncio.ensure_future(mgr.create_kernel(_config(packages=["xgboost"]), user_id=42))
            await asyncio.to_thread(started.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()

        asyncio.run(scenario())
        assert mgr._kernels == {}
        mgr._persist_kernel.assert_not_called()

    def test_lifecycle_ops_blocked_while_creating(self):
        mgr = _create_manager()
        mgr._kernels["k1"] = _kernel(state=KernelState.CREATING)
        mgr._kernel_owners["k1"] = 42

        with pytest.raises(ValueError, match="still being created"):
            mgr._get_or_start_kernel("k1")
        with pytest.raises(ValueError, match="still being created"):
            _run(mgr.stop_kernel("k1"))
        with pytest.raises(ValueError, match="still being created"):
            _run(mgr.delete_kernel("k1"))
        with pytest.raises(RuntimeError, match="still being created"):
            _run(mgr.update_kernel("k1", ["numpy"]))
        with pytest.raises(RuntimeError, match="still being created"):
            _run(mgr._ensure_running("k1"))
        with pytest.raises(RuntimeError, match="still being created"):
            mgr._ensure_running_sync("k1")


# Sanity check: __init__'s startup sequence now calls the GC


class TestDeletedKernelsArtifacts:
    """Deleting a kernel removes the folder it persisted its artifacts in, and nothing else. Real files."""

    @staticmethod
    def _manager(tmp_path) -> KernelManager:
        mgr = _bare_manager()
        mgr._shared_volume = str(tmp_path)
        mgr._remove_kernel_from_db = MagicMock()
        mgr._discard_scratch_flow = MagicMock()
        mgr._end_kernel_token = MagicMock()
        return mgr

    @staticmethod
    def _file(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
        return path

    def test_delete_removes_the_kernels_own_folder(self, tmp_path):
        mgr = self._manager(tmp_path)
        mgr._kernels["k1"] = _kernel("k1")
        self._file(tmp_path / "artifacts" / "k1" / "9001" / "model" / "data.artifact")
        of_another_kernel = self._file(tmp_path / "artifacts" / "k2" / "9001" / "model" / "data.artifact")

        _run(mgr.delete_kernel("k1"))

        assert not (tmp_path / "artifacts" / "k1").exists()
        assert of_another_kernel.exists()
        assert "k1" not in mgr._kernels

    def test_delete_of_a_kernel_that_persisted_nothing_is_fine(self, tmp_path):
        mgr = self._manager(tmp_path)
        mgr._kernels["k1"] = _kernel("k1")

        _run(mgr.delete_kernel("k1"))

        assert "k1" not in mgr._kernels

    @pytest.mark.parametrize("kernel_id", ["..", ".", "", "k2/9001", "../user_files"])
    def test_an_id_that_names_no_folder_of_its_own_removes_nothing(self, tmp_path, kernel_id):
        mgr = self._manager(tmp_path)
        kept = [
            self._file(tmp_path / "artifacts" / "k2" / "9001" / "model" / "data.artifact"),
            self._file(tmp_path / "user_files" / "report.csv"),
        ]

        mgr._remove_persisted_artifacts(kernel_id)

        assert all(path.exists() for path in kept)

    def test_a_folder_core_may_not_remove_does_not_fail_the_delete(self, tmp_path, monkeypatch, caplog):
        mgr = self._manager(tmp_path)
        mgr._kernels["k1"] = _kernel("k1")
        self._file(tmp_path / "artifacts" / "k1" / "9001" / "model" / "data.artifact")
        # A kernel writes as root; a core that runs as another user is refused, which a test cannot be.
        monkeypatch.setattr(kernel_manager.shutil, "rmtree", MagicMock(side_effect=PermissionError("root owns it")))

        with caplog.at_level("WARNING", logger=kernel_manager.logger.name):
            _run(mgr.delete_kernel("k1"))

        assert "k1" not in mgr._kernels
        assert "Could not remove the persisted artifacts of kernel 'k1'" in caplog.text


class TestStartupSequence:
    def test_init_calls_orphan_gc(self, monkeypatch):
        """Document the contract: any new startup helper must be wired in."""
        # We use a real __init__ but stub out everything it touches.
        monkeypatch.setattr(kernel_manager.docker, "from_env", lambda **_: MagicMock())
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
            lambda self: (called.append("restore"), True)[1],
        )
        monkeypatch.setattr(
            KernelManager,
            "_reclaim_running_containers",
            lambda self, prune_orphans=True: called.append(f"reclaim(prune={prune_orphans})"),
        )
        monkeypatch.setattr(
            KernelManager,
            "_remove_orphan_build_containers",
            lambda self: called.append("build_gc"),
        )
        monkeypatch.setattr(
            KernelManager,
            "_remove_orphan_derived_images",
            lambda self: called.append("gc"),
        )

        KernelManager(shared_volume_path="/tmp/x")

        assert called == ["restore", "reclaim(prune=True)", "build_gc", "gc"]


# Registry-mutation concurrency (N3): _kernels_lock now guards the launch-path
# port allocation and the create/delete registry commits, not just reconcile.
# These tests are Docker-free — they drive the same locked critical sections the
# manager uses, with a widened race window, and assert no corruption.


class TestRegistryConcurrency:
    def test_concurrent_port_allocation_yields_unique_ports(self, monkeypatch):
        """The launch-path 'allocate port under the lock, then register' pattern
        must never hand the same free port to two concurrent launches."""
        mgr = _bare_manager()

        # Make the availability probe slow so an unlocked allocate would let two
        # threads observe the same 'used_ports' snapshot and pick one port twice.
        def _slow_available(port: int) -> bool:
            time.sleep(0.0005)
            return True

        monkeypatch.setattr(kernel_manager, "_is_port_available", _slow_available)

        n = 25
        barrier = threading.Barrier(n)
        errors: list[Exception] = []

        def _launch(idx: int) -> None:
            barrier.wait()
            try:
                kid = f"k{idx}"
                # Mirror start_kernel_sync: allocate + reserve under the lock.
                with mgr._kernels_lock:
                    port = mgr._allocate_port()
                    mgr._kernels[kid] = _kernel(kid)
                    mgr._kernels[kid].port = port
                    mgr._kernel_owners[kid] = idx
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_launch, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        ports = [k.port for k in mgr._kernels.values()]
        assert len(ports) == n
        assert len(set(ports)) == n, "two launches were assigned the same port"

    def test_concurrent_create_and_delete_keep_registry_consistent(self):
        """Interleaved create-commit and delete critical sections must keep
        _kernels and _kernel_owners in lockstep (no orphaned owner entry)."""
        mgr = _bare_manager()
        n = 40
        barrier = threading.Barrier(2 * n)

        def _create(idx: int) -> None:
            barrier.wait()
            kid = f"k{idx}"
            with mgr._kernels_lock:
                if kid in mgr._kernels:
                    return
                mgr._kernels[kid] = _kernel(kid)
                mgr._kernel_owners[kid] = idx

        def _delete(idx: int) -> None:
            barrier.wait()
            kid = f"k{idx}"
            with mgr._kernels_lock:
                mgr._kernels.pop(kid, None)
                mgr._kernel_owners.pop(kid, None)

        threads = [threading.Thread(target=_create, args=(i,)) for i in range(n)]
        threads += [threading.Thread(target=_delete, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Whatever survived the race, every live kernel has exactly one owner and
        # there is no owner row without a kernel.
        assert set(mgr._kernels) == set(mgr._kernel_owners)

    def test_restore_from_db_holds_lock_during_registry_mutation(self, monkeypatch):
        """Regression guard for N3: the real _restore_kernels_from_db must mutate
        the in-memory registry under _kernels_lock. Wrap the lock to record the
        depth at the moment a kernel row is written and assert it was held."""
        mgr = _bare_manager()

        held_depth_at_write: list[int] = []

        class _RecordingLock:
            def __init__(self) -> None:
                self._lock = threading.RLock()
                self.depth = 0

            def __enter__(self):
                self.depth += 1
                return self._lock.__enter__()

            def __exit__(self, *exc):
                self.depth -= 1
                return self._lock.__exit__(*exc)

        mgr._kernels_lock = _RecordingLock()

        config = MagicMock()
        config.id = "k1"
        config.name = "test-k1"
        config.packages = []
        config.memory_gb = 1
        config.cpu_cores = 1
        config.gpu = False
        config.image_flavour = ImageFlavour.BASE
        config.custom_image = None

        class _RecordingDict(dict):
            def __setitem__(self, key, value):
                held_depth_at_write.append(mgr._kernels_lock.depth)
                super().__setitem__(key, value)

        mgr._kernels = _RecordingDict()

        import contextlib

        @contextlib.contextmanager
        def _fake_db_context():
            yield MagicMock()

        monkeypatch.setattr(
            "flowfile_core.database.connection.get_db_context",
            _fake_db_context,
        )
        monkeypatch.setattr(
            "flowfile_core.kernel.persistence.get_all_kernels",
            lambda db: [(config, [], 7)],
        )
        monkeypatch.setattr(
            "flowfile_core.kernel.persistence.get_all_kernel_scratch_ids",
            lambda db: {},
        )

        mgr._restore_kernels_from_db()

        assert "k1" in mgr._kernels
        assert mgr._kernel_owners["k1"] == 7
        assert held_depth_at_write == [1], "registry was mutated without holding _kernels_lock"
        assert mgr._kernels_lock.depth == 0


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

        # execute() delegates to the sync path, so patch the sync client.
        class _FailingClient:
            def __init__(self, *a, **kw):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def post(self, *a, **kw):
                raise httpx.ConnectError("connection failed")

        monkeypatch.setattr(httpx, "Client", _FailingClient)

        req = ExecuteRequest(node_id=1, code="1", source_registration_id=1)
        result = _run(mgr.execute("k1", req))

        assert result.success is False
        assert result.error == _KERNEL_DOWN_MSG
        assert k.state == KernelState.STOPPED
        # The container reference is kept so the next start's tracked cleanup
        # replaces the name-holder instead of 409ing on it.
        assert k.container_id == "abc"


class TestExecuteSyncKernelTokenStamp:
    """execute_sync is the one choke point where the kernel's own token gets attached.

    The notebook /execute and /execute_cell routes deserialize the ExecuteRequest
    straight from the client body (no token), and a reclaimed or adopted
    container may still hold the token of an earlier run.
    """

    @pytest.fixture
    def kernel_row(self):
        """A kernel row of user 1 in the test database, in a run; yields its id."""
        from flowfile_core.database import models as db_models
        from flowfile_core.database.connection import get_db_context

        kernel_id = f"k-{uuid.uuid4().hex[:8]}"
        with get_db_context() as db:
            db.add(db_models.Kernel(id=kernel_id, name="k", user_id=1, token_id="run-1"))
            db.commit()
        yield kernel_id
        with get_db_context() as db:
            db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).delete()
            db.commit()

    @staticmethod
    def _manager_capturing_request(monkeypatch, kernel_id: str = "k1", fail_with: Exception | None = None):
        """A manager with one idle kernel; ``captured["request"]`` is what the kernel was sent.

        ``fail_with`` is raised once the request was captured, as a kernel that died under the call would."""
        mgr = _bare_manager()
        kernel = KernelInfo(id=kernel_id, name="k1", state=KernelState.IDLE, port=19001)
        mgr._kernels[kernel_id] = kernel

        captured = {}

        class _Answer:
            def raise_for_status(self):
                return None

            def json(self):
                return {"success": True}

        class _Client:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def post(self, url, json=None):
                captured["request"] = ExecuteRequest(**json)
                if fail_with is not None:
                    raise fail_with
                return _Answer()

        monkeypatch.setattr(httpx, "Client", _Client)
        return mgr, captured

    def test_execute_sync_stamps_the_kernels_token(self, monkeypatch, kernel_row):
        from flowfile_core.auth.jwt import create_kernel_token, get_internal_token

        mgr, captured = self._manager_capturing_request(monkeypatch, kernel_row)
        expected = create_kernel_token(1, kernel_row, "run-1")

        mgr.execute_sync(kernel_row, ExecuteRequest(node_id=1, code="", source_registration_id=1))
        assert captured["request"].kernel_token == expected
        assert captured["request"].internal_token == expected, "where an older kernel image reads its token"
        assert expected != get_internal_token()

        # A client-supplied token in the request body is overwritten, not forwarded.
        supplied = ExecuteRequest(
            node_id=1, code="", source_registration_id=1, internal_token="supplied", kernel_token="supplied"
        )
        mgr.execute_sync(kernel_row, supplied)
        assert (captured["request"].kernel_token, captured["request"].internal_token) == (expected, expected)

    def test_execute_sync_stamp_tolerates_a_kernel_without_a_token(self, monkeypatch):
        """A kernel core has no row for must not break execution — it stays unstamped."""
        mgr, captured = self._manager_capturing_request(monkeypatch)

        request = ExecuteRequest(node_id=1, code="", source_registration_id=1, internal_token="supplied")
        result = mgr.execute_sync("k1", request)

        assert result.success is True
        assert captured["request"].kernel_token is None and captured["request"].internal_token is None

    def test_a_start_begins_a_token_and_a_stop_ends_it(self, kernel_row):
        from flowfile_core.auth.jwt import create_kernel_token, kernel_caller
        from flowfile_core.database.connection import get_db_context

        mgr = _bare_manager()
        before = create_kernel_token(1, kernel_row, "run-1")
        assert mgr.kernel_token(kernel_row) == before

        run_id, started = mgr._kernel_run(kernel_row, "run-2")
        assert run_id == "run-2" and started and started != before
        assert mgr.kernel_token(kernel_row) == before, "a run that has not begun changes nothing"
        with get_db_context() as db, pytest.raises(HTTPException):
            kernel_caller(started, db)

        assert mgr._begin_kernel_run(kernel_row, "run-2") and mgr.kernel_token(kernel_row) == started
        with get_db_context() as db:
            assert kernel_caller(started, db).kernel_id == kernel_row
            with pytest.raises(HTTPException):
                kernel_caller(before, db)

        mgr._end_kernel_token(kernel_row)
        assert mgr.kernel_token(kernel_row) is None
        with get_db_context() as db, pytest.raises(HTTPException):
            kernel_caller(started, db)

    def test_a_kernel_in_a_run_keeps_it_when_asked_and_only_its_own_run_is_ended(self, kernel_row):
        from flowfile_core.auth.jwt import create_kernel_token

        mgr = _bare_manager()
        current = create_kernel_token(1, kernel_row, "run-1")

        assert mgr._begin_kernel_run(kernel_row, "run-2", keep_current=True)
        assert mgr.kernel_token(kernel_row) == current, "it is in a run, so it keeps it"
        mgr._end_kernel_token(kernel_row, "run-0")
        assert mgr.kernel_token(kernel_row) == current, "run-0 is not its run any more, so nothing ends"
        assert mgr._kernel_run_ids()[kernel_row] == "run-1"

        mgr._end_kernel_token(kernel_row, "run-1")
        assert mgr.kernel_token(kernel_row) is None and kernel_row not in mgr._kernel_run_ids()
        assert mgr._begin_kernel_run(kernel_row, "run-2", keep_current=True)
        assert mgr.kernel_token(kernel_row) == create_kernel_token(1, kernel_row, "run-2"), "in none, so it begins"

    def test_a_cell_that_waited_is_sent_with_the_token_of_the_run_it_executes_in(self, monkeypatch, kernel_row):
        """The kernel can start again, here or in another core process, while a cell waits for the one before it."""
        from flowfile_core.auth.jwt import create_kernel_token

        mgr, captured = self._manager_capturing_request(monkeypatch, kernel_row)
        request = ExecuteRequest(node_id=1, code="", source_registration_id=1)
        lock = mgr._exec_lock_for(kernel_row)
        lock.acquire()
        waiting = threading.Thread(target=mgr.execute_sync, args=(kernel_row, request))
        waiting.start()
        try:
            deadline = time.monotonic() + 5
            while not request.exec_token and time.monotonic() < deadline:
                time.sleep(0.01)
            assert request.exec_token, "the cell reached the wait for the kernel"
            assert mgr._begin_kernel_run(kernel_row, "run-2")
        finally:
            lock.release()
            waiting.join(timeout=5)

        assert captured["request"].kernel_token == create_kernel_token(1, kernel_row, "run-2")

    @pytest.mark.parametrize("oom_killed", [False, True], ids=["its container is gone", "it ran out of memory"])
    def test_a_kernel_found_dead_under_a_cell_has_its_token_ended(self, monkeypatch, kernel_row, oom_killed):
        """The token has no expiry of its own, so a dead kernel's would stay good until its next start or stop."""
        mgr, _ = self._manager_capturing_request(monkeypatch, kernel_row, fail_with=httpx.ConnectError("down"))
        mgr._check_oom_killed = MagicMock(return_value=oom_killed)
        mgr._is_container_running = MagicMock(return_value=False)

        result = mgr.execute_sync(kernel_row, ExecuteRequest(node_id=1, code="", source_registration_id=1))

        assert result.success is False
        assert mgr._kernels[kernel_row].state == (KernelState.ERROR if oom_killed else KernelState.STOPPED)
        assert mgr.kernel_token(kernel_row) is None

    def test_a_dead_container_does_not_end_a_run_begun_since(self, monkeypatch, kernel_row):
        """This process's container was replaced by another core process's start, whose run is not over."""
        from flowfile_core.auth.jwt import create_kernel_token

        mgr, _ = self._manager_capturing_request(monkeypatch, kernel_row, fail_with=httpx.ConnectError("down"))
        mgr._check_oom_killed = MagicMock(return_value=False)
        mgr._is_container_running = MagicMock(side_effect=lambda _: not mgr._begin_kernel_run(kernel_row, "run-2"))

        result = mgr.execute_sync(kernel_row, ExecuteRequest(node_id=1, code="", source_registration_id=1))

        assert result.success is False and mgr._kernels[kernel_row].state == KernelState.STOPPED
        assert mgr.kernel_token(kernel_row) == create_kernel_token(1, kernel_row, "run-2")

    def test_a_cell_that_only_failed_keeps_the_kernels_token(self, monkeypatch, kernel_row):
        from flowfile_core.auth.jwt import create_kernel_token

        mgr, _ = self._manager_capturing_request(monkeypatch, kernel_row, fail_with=httpx.ReadTimeout("slow"))
        mgr._check_oom_killed = MagicMock(return_value=False)
        mgr._is_container_running = MagicMock(return_value=True)
        mgr.interrupt_execution_sync = MagicMock(return_value=True)

        result = mgr.execute_sync(kernel_row, ExecuteRequest(node_id=1, code="", source_registration_id=1))

        assert result.success is False
        assert mgr.kernel_token(kernel_row) == create_kernel_token(1, kernel_row, "run-1")

    @pytest.mark.parametrize("mode", ["electron", "package", "docker", "server"])
    def test_a_kernels_environment_holds_its_token_and_never_the_internal_one(self, monkeypatch, mode):
        monkeypatch.setenv("FLOWFILE_MODE", mode)
        monkeypatch.setenv("FLOWFILE_INTERNAL_TOKEN", "the-internal-token")
        mgr = _bare_manager()
        kernel = KernelInfo(id="k1", name="k1", port=19001)

        env = mgr._build_kernel_env("k1", kernel, "the-kernels-token")

        assert env["FLOWFILE_KERNEL_TOKEN"] == "the-kernels-token"
        assert "FLOWFILE_INTERNAL_TOKEN" not in env and "the-internal-token" not in env.values()
        assert "FLOWFILE_KERNEL_TOKEN" not in mgr._build_kernel_env("k1", kernel)


class TestCoreCallbackUrl:
    """Kernels must call back to the port core actually bound.

    The desktop shell scans for a free (core, worker) pair, so core is on 63580
    whenever 63578 is taken — a dev core, a stale sidecar, a second window. The
    URL used to hardcode 63578, sending every kernel API callback to whatever
    else held that port while the log callback went to the right one.
    """

    def _env(self, monkeypatch, *, port: int, on_network: bool) -> dict[str, str]:
        from flowfile_core.configs import settings

        monkeypatch.setattr(settings, "SERVER_PORT", port)
        monkeypatch.delenv("FLOWFILE_CORE_URL", raising=False)
        mgr = _bare_manager()
        mgr._docker_network = "flowfile-network" if on_network else None
        kernel = KernelInfo(id="k1", name="K", image_flavour=ImageFlavour.BASE)
        return mgr._build_kernel_env("k1", kernel)

    def test_host_gateway_branch_uses_the_bound_port(self, monkeypatch):
        env = self._env(monkeypatch, port=63580, on_network=False)
        assert env["FLOWFILE_CORE_URL"] == "http://host.docker.internal:63580"

    def test_docker_network_branch_uses_the_bound_port(self, monkeypatch):
        env = self._env(monkeypatch, port=63580, on_network=True)
        assert env["FLOWFILE_CORE_URL"] == "http://flowfile-core:63580"

    def test_default_port_is_unchanged(self, monkeypatch):
        # Docker mode never sets CORE_PORT and passes no --port, so the emitted
        # string must stay byte-identical to what shipped before.
        assert self._env(monkeypatch, port=63578, on_network=True)["FLOWFILE_CORE_URL"] == (
            "http://flowfile-core:63578"
        )
        assert self._env(monkeypatch, port=63578, on_network=False)["FLOWFILE_CORE_URL"] == (
            "http://host.docker.internal:63578"
        )

    def test_explicit_override_still_wins(self, monkeypatch):
        # kernel_fixtures.py sets this to point kernels at the test server.
        from flowfile_core.configs import settings

        monkeypatch.setattr(settings, "SERVER_PORT", 63580)
        monkeypatch.setenv("FLOWFILE_CORE_URL", "http://somewhere-else:1234")
        mgr = _bare_manager()
        mgr._docker_network = "flowfile-network"
        kernel = KernelInfo(id="k1", name="K", image_flavour=ImageFlavour.BASE)

        env = mgr._build_kernel_env("k1", kernel)

        assert env["FLOWFILE_CORE_URL"] == "http://somewhere-else:1234"

    def test_api_and_log_callbacks_agree(self, monkeypatch):
        """The two used to be built independently and could point at different
        processes. Pin that they now resolve from the same base."""
        from flowfile_core.configs import settings
        from flowfile_core.kernel.execution import build_execute_request

        monkeypatch.setattr(settings, "SERVER_PORT", 63580)
        monkeypatch.delenv("FLOWFILE_CORE_URL", raising=False)
        mgr = _bare_manager()
        mgr._docker_network = "flowfile-network"
        kernel = KernelInfo(id="k1", name="K", image_flavour=ImageFlavour.BASE)

        api_url = mgr._build_kernel_env("k1", kernel)["FLOWFILE_CORE_URL"]
        request = build_execute_request(
            node_id=1,
            code="",
            input_paths={},
            output_dir="/tmp",
            flow_id=1,
            manager=mgr,
            source_registration_id=1,
        )

        assert request.log_callback_url == f"{api_url}/raw_logs"
        assert "63580" in request.log_callback_url

    def test_execute_request_is_built_without_a_token(self):
        """``execute_sync`` stamps the kernel's token, since only it knows which kernel the request goes to."""
        from flowfile_core.kernel.execution import build_execute_request

        request = build_execute_request(
            node_id=1,
            code="",
            input_paths={},
            output_dir="/tmp",
            flow_id=1,
            manager=_bare_manager(),
            source_registration_id=1,
        )

        assert request.kernel_token is None and request.internal_token is None


class TestStartupGcSafety:
    """A registry that failed to load must not be read as 'nothing should exist'.

    One swallowed database error used to leave `_kernels` empty, and the three
    GC passes then destroyed every kernel container and every derived image —
    turning a transient DB fault into a multi-minute rebuild of every kernel.
    """

    def _stub_docker(self, monkeypatch):
        docker_client = MagicMock()
        container = MagicMock()
        container.name = "flowfile-kernel-k1"
        container.status = "running"
        container.attrs = {"Created": "2020-01-01T00:00:00Z", "NetworkSettings": {"Ports": {}}}
        container.labels = {}  # unlabelled legacy container: counts as ours
        image = MagicMock()
        image.tags = ["flowfile-kernel-derived-k1:latest"]
        image.labels = {"flowfile_core_instance": "test-core-id", "flowfile_kernel_id": "k1"}
        image.attrs = {"Created": "2020-01-01T00:00:00Z"}
        docker_client.containers.list.return_value = [container]
        docker_client.images.list.return_value = [image]

        monkeypatch.setattr(kernel_manager.docker, "from_env", lambda **_: docker_client)
        monkeypatch.setattr(KernelManager, "_detect_docker_network", lambda self: None)
        monkeypatch.setattr(KernelManager, "_discover_volume_for_path", lambda self, path: (None, None, None))
        monkeypatch.setattr(KernelManager, "_remove_orphan_build_containers", lambda self: None)
        monkeypatch.setattr(KernelManager, "_restore_kernels_from_db", lambda self: True)
        return docker_client, container

    def test_failed_restore_destroys_nothing(self, monkeypatch):
        docker_client, container = self._stub_docker(monkeypatch)
        monkeypatch.setattr(KernelManager, "_restore_kernels_from_db", lambda self: False)

        KernelManager(shared_volume_path="/tmp/x")

        container.stop.assert_not_called()
        container.remove.assert_not_called()
        docker_client.images.remove.assert_not_called()

    def test_successful_restore_still_collects_orphans(self, monkeypatch):
        """The control: the gate must not become 'never GC'."""
        docker_client, container = self._stub_docker(monkeypatch)

        KernelManager(shared_volume_path="/tmp/x")

        container.stop.assert_called_once()
        docker_client.images.remove.assert_called_once()

    def test_kill_switch_disables_gc(self, monkeypatch):
        docker_client, container = self._stub_docker(monkeypatch)
        monkeypatch.setenv("FLOWFILE_KERNEL_GC", "0")

        KernelManager(shared_volume_path="/tmp/x")

        container.stop.assert_not_called()
        docker_client.images.remove.assert_not_called()


class TestReapGrace:
    def test_recent_containers_are_left_alone(self):
        from datetime import datetime, timezone

        mgr = _bare_manager()
        mgr._kernels = {}
        container = MagicMock()
        container.name = "flowfile-kernel-fresh"
        container.status = "running"
        container.labels = {"flowfile_core_instance": "test-core-id"}
        container.attrs = {"Created": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
        mgr._docker.containers.list.return_value = [container]

        mgr._reclaim_running_containers()

        container.stop.assert_not_called()

    def test_another_cores_container_is_left_alone(self):
        mgr = _bare_manager()
        mgr._kernels = {}
        container = MagicMock()
        container.name = "flowfile-kernel-theirs"
        container.status = "running"
        container.labels = {"flowfile_core_instance": "some-other-install"}
        container.attrs = {"Created": "2020-01-01T00:00:00Z"}
        mgr._docker.containers.list.return_value = [container]

        mgr._reclaim_running_containers()

        container.stop.assert_not_called()


class TestShutdownOwnership:
    """Quitting one core must not stop kernels another core is using."""

    def _running_kernel(self, mgr, kernel_id="k1"):
        kernel = _kernel(kernel_id)
        kernel.state = KernelState.IDLE
        kernel.container_id = "c-1"
        mgr._kernels[kernel_id] = kernel
        return kernel

    def test_stops_only_what_this_process_started(self):
        mgr = _bare_manager()
        mgr._start_flights = {}
        mgr._start_flights_lock = threading.Lock()
        mine = self._running_kernel(mgr, "mine")
        theirs = self._running_kernel(mgr, "theirs")
        mgr._started_here = {"mine"}
        mgr._cleanup_container = MagicMock()

        mgr.shutdown_all()

        mgr._cleanup_container.assert_called_once_with("mine")
        assert mine.state == KernelState.STOPPED
        assert theirs.state == KernelState.IDLE

    def test_adopted_kernels_survive_shutdown(self):
        mgr = _bare_manager()
        mgr._start_flights = {}
        mgr._start_flights_lock = threading.Lock()
        self._running_kernel(mgr, "adopted")
        mgr._started_here = set()
        mgr._cleanup_container = MagicMock()

        mgr.shutdown_all()

        mgr._cleanup_container.assert_not_called()


class TestAdoptedOwnership:
    """A core killed without cleaning up hands its kernels to whoever restarts.

    Without this, every hard kill leaks a container forever: the next core
    adopts it, reuses it, and then refuses to stop it at exit because it wasn't
    the one that started it.
    """

    def _adopted(self, mgr, *, labels: dict[str, str]):
        mgr._kernels["k1"] = _kernel("k1")
        container = MagicMock()
        container.name = "flowfile-kernel-k1"
        container.status = "running"
        container.id = "c-1"
        container.labels = labels
        container.attrs = {"Created": "2020-01-01T00:00:00Z", "NetworkSettings": {"Ports": {}}}
        mgr._docker.containers.list.return_value = [container]
        return container

    def test_dead_owner_transfers_shutdown_duty(self):
        mgr = _bare_manager()
        # pid 1 is init: alive. Use a pid that cannot be running.
        self._adopted(
            mgr,
            labels={
                "flowfile_core_instance": "test-core-id",
                "flowfile_core_runtime": "a-previous-boot",
                "flowfile_core_pid": "2147483646",
            },
        )

        mgr._reclaim_running_containers()

        assert "k1" in mgr._started_here

    def test_live_owner_keeps_its_kernel(self):
        mgr = _bare_manager()
        self._adopted(
            mgr,
            labels={
                "flowfile_core_instance": "test-core-id",
                "flowfile_core_runtime": "another-live-core",
                "flowfile_core_pid": str(os.getpid()),
            },
        )

        mgr._reclaim_running_containers()

        assert "k1" not in mgr._started_here

    def test_other_install_is_never_claimed(self):
        mgr = _bare_manager()
        self._adopted(
            mgr,
            labels={
                "flowfile_core_instance": "some-other-install",
                "flowfile_core_pid": "2147483646",
            },
        )

        mgr._reclaim_running_containers()

        assert "k1" not in mgr._started_here

    def test_prelabel_container_is_not_claimed(self):
        mgr = _bare_manager()
        self._adopted(mgr, labels={})

        mgr._reclaim_running_containers()

        assert "k1" not in mgr._started_here

    def test_claimed_kernel_is_stopped_at_shutdown(self):
        mgr = _bare_manager()
        container = self._adopted(
            mgr,
            labels={
                "flowfile_core_instance": "test-core-id",
                "flowfile_core_pid": "2147483646",
            },
        )
        container.id = "c-1"
        mgr._reclaim_running_containers()
        mgr._cleanup_container = MagicMock()

        mgr.shutdown_all()

        mgr._cleanup_container.assert_called_once_with("k1")


def test_bake_failure_summary_keeps_the_last_meaningful_pip_lines():
    log = [
        "Step 1/4 : FROM flowfile-kernel:lite",
        " ---> 1234abcd",
        "Collecting flowfile==0.21.0",
        "  Downloading flowfile-0.21.0-py3-none-any.whl (5.1 MB)",
        "     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 5.1/5.1 MB 9.0 MB/s eta 0:00:00",
        "Collecting polars-grouper",
        "",
        *[f"  compile line {i}" for i in range(40)],
        "  error: can't find Rust compiler",
        "  ERROR: Failed building wheel for polars-grouper",
        "ERROR: Could not build wheels for polars-grouper, which is required to install pyproject.toml-based projects",
    ]
    summary = kernel_manager.bake_failure_summary(["flowfile==0.21.0"], log)
    lines = summary.splitlines()
    assert lines[0] == "Installing flowfile==0.21.0 into the kernel image failed."
    assert len(lines) <= 16 and len(summary) <= 1600
    assert lines[-1].startswith("ERROR: Could not build wheels for polars-grouper")
    assert "can't find Rust compiler" in summary
    assert not any(noise in summary for noise in ("Step 1/4", "Downloading", "━", "Collecting"))


def test_bake_failure_summary_strips_pip_colour_codes():
    log = ["\x1b[91mERROR: ResolutionImpossible\x1b[0m\n"]
    summary = kernel_manager.bake_failure_summary(["flowfile==0.22.0"], log)
    assert "ERROR: ResolutionImpossible" in summary
    assert "\x1b" not in summary


def test_bake_failure_summary_ends_with_dockers_reason_after_a_successful_pip_run():
    log = [
        *[f"  noise line {i}" for i in range(40)],
        "Successfully installed flowfile-0.22.0 polars-1.39.0",
        "WARNING: Running pip as the 'root' user can result in broken permissions",
    ]
    reason = "failed to register layer: write /usr/local/lib/python3.12/site-packages/x.so: no space left on device"
    summary = kernel_manager.bake_failure_summary(["flowfile==0.22.0"], log, reason)
    lines = summary.splitlines()
    assert lines[-1] == reason
    assert lines[-2].startswith("WARNING: Running pip as the 'root' user")
    assert len(lines) <= 16


def test_derived_image_build_error_surfaces_dockers_reason():
    mgr = _bare_manager()
    reason = "failed to register layer: no space left on device"
    mgr._docker.images.get.side_effect = [docker.errors.ImageNotFound("derived"), MagicMock()]
    mgr._docker.images.build.side_effect = docker.errors.BuildError(
        reason, iter([{"stream": "Successfully installed flowfile-0.22.0\n"}, {"error": reason}])
    )
    kernel = KernelInfo(id="k1", name="k1", packages=["flowfile==0.22.0"])

    with pytest.raises(RuntimeError) as exc_info:
        mgr._build_derived_image_locked(kernel, "flowfile-kernel-lite:local", "derived:k1")

    assert str(exc_info.value).splitlines()[-1] == reason


def test_bake_failure_summary_caps_characters_and_handles_an_empty_log():
    assert kernel_manager.bake_failure_summary(["x"], []) == "Installing x into the kernel image failed."
    summary = kernel_manager.bake_failure_summary(["x"], ["E" * 5000])
    assert len(summary.splitlines()[1]) == 1500


class TestCallsToAKernelAreSigned:
    """Core signs every call to a kernel with the kernel's own token; the runtime takes no other call."""

    @pytest.fixture
    def running_kernel(self, monkeypatch):
        """A manager with one idle kernel that has a row and a run; yields ``(manager, kernel id, client kwargs)``."""
        from flowfile_core.database import models as db_models
        from flowfile_core.database.connection import get_db_context

        kernel_id = f"k-{uuid.uuid4().hex[:8]}"
        with get_db_context() as db:
            db.add(db_models.Kernel(id=kernel_id, name="k", user_id=1, token_id="run-1"))
            db.commit()
        mgr = _bare_manager()
        mgr._kernels[kernel_id] = KernelInfo(
            id=kernel_id, name="k", state=KernelState.IDLE, port=19001, container_id="c-1"
        )
        mgr._docker.containers.get.return_value.attrs = {"Config": {"Env": ["FLOWFILE_KERNEL_TOKEN=token-of-its-start"]}}
        made: list[dict] = []

        class _Answer:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"success": True, "used": 1, "limit": 2, "percent": 50.0}

        class _Client:
            def __init__(self, *args, **kwargs):
                made.append(kwargs)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            def post(self, *args, **kwargs):
                return _Answer()

        class _AsyncClient(_Client):
            async def get(self, *args, **kwargs):
                return _Answer()

            async def post(self, *args, **kwargs):
                return _Answer()

        monkeypatch.setattr(httpx, "Client", _Client)
        monkeypatch.setattr(httpx, "AsyncClient", _AsyncClient)
        yield mgr, kernel_id, made
        with get_db_context() as db:
            db.query(db_models.Kernel).filter(db_models.Kernel.id == kernel_id).delete()
            db.commit()

    def test_an_execution_a_clear_an_interrupt_and_an_async_call_carry_the_kernels_token(self, running_kernel):
        mgr, kernel_id, made = running_kernel
        signed = {"Authorization": "Bearer token-of-its-start"}

        mgr.execute_sync(kernel_id, ExecuteRequest(node_id=1, code="", source_registration_id=1))
        mgr.clear_artifacts_sync(kernel_id)
        _run(mgr.clear_namespace(kernel_id, 1))
        mgr._kernels[kernel_id].state = KernelState.EXECUTING
        assert mgr.interrupt_execution_sync(kernel_id) is True

        assert len(made) == 4
        assert [kwargs.get("headers") for kwargs in made] == [signed] * 4
        assert mgr._docker.containers.get.call_count == 1, "the container is asked for its token once"

    def test_a_kernel_without_a_token_is_called_unsigned(self):
        from flowfile_core.kernel.manager import _signed

        assert _signed(None) == {} and _signed("t") == {"Authorization": "Bearer t"}
        assert _bare_manager()._call_headers(KernelInfo(id="k", name="k")) == {}
        started_without = _bare_manager()
        started_without._docker.containers.get.return_value.attrs = {"Config": {"Env": ["KERNEL_PACKAGES="]}}
        assert started_without._call_headers(KernelInfo(id="k", name="k", container_id="c-1")) == {}

    def test_the_token_a_container_was_started_with_is_read_from_its_environment(self):
        container = MagicMock()
        container.attrs = {"Config": {"Env": ["A=b", "FLOWFILE_KERNEL_TOKEN=a.b.c", "C"]}}
        assert KernelManager._container_token(container) == "a.b.c"
        container.attrs = {"Config": {"Env": ["A=b"]}}
        assert KernelManager._container_token(container) is None
        assert KernelManager._container_token(MagicMock()) is None
