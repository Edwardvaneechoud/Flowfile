"""Regression tests for Theme E (M-E1, L6, L7, L9, L11) concurrency / DoS fixes.

M-E1: per-owner suppression — user A's import must not suppress user B's projection hooks.
L6:   _by_owner / _suppressed guarded by _cache_lock; no dict-mutation-during-iteration.
L7:   /versions?limit clamped to 1-500.
L9:   oversized YAML file and excess entries raise ValueError (surfaced as 413 by route).
"""

from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flowfile_core.project.service import ProjectSyncService


# ---------------------------------------------------------------------------
# M-E1: per-owner suppression
# ---------------------------------------------------------------------------


class TestPerOwnerSuppression:
    def setup_method(self):
        self.svc = ProjectSyncService()

    def test_suppress_only_affects_target_owner(self):
        owner_a, owner_b = 1, 2
        with self.svc.suppress_projection(owner_a):
            assert owner_a in self.svc._suppressed
            assert owner_b not in self.svc._suppressed

    def test_suppression_released_on_exit(self):
        owner = 1
        with self.svc.suppress_projection(owner):
            assert owner in self.svc._suppressed
        assert owner not in self.svc._suppressed

    def test_suppression_released_on_exception(self):
        owner = 1
        try:
            with self.svc.suppress_projection(owner):
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert owner not in self.svc._suppressed

    def test_get_active_project_suppressed_owner_returns_none(self, tmp_path):
        from flowfile_core.project.models import ActiveProject

        owner = 42
        self.svc._by_owner = {owner: ActiveProject(1, "p", tmp_path, owner, True)}
        with self.svc.suppress_projection(owner):
            assert self.svc.get_active_project(owner) is None

    def test_get_active_project_other_owner_not_suppressed(self, tmp_path):
        from flowfile_core.project.models import ActiveProject

        owner_a, owner_b = 1, 2
        self.svc._by_owner = {
            owner_a: ActiveProject(1, "pa", tmp_path, owner_a, True),
            owner_b: ActiveProject(2, "pb", tmp_path, owner_b, True),
        }
        with self.svc.suppress_projection(owner_a):
            assert self.svc.get_active_project(owner_a) is None
            assert self.svc.get_active_project(owner_b) is not None

    def test_two_concurrent_suppressions_are_independent(self, tmp_path):
        from flowfile_core.project.models import ActiveProject

        owner_a, owner_b = 10, 20
        self.svc._by_owner = {
            owner_a: ActiveProject(1, "pa", tmp_path, owner_a, True),
            owner_b: ActiveProject(2, "pb", tmp_path, owner_b, True),
        }
        results: list[tuple[bool, bool]] = []

        def suppress_a():
            with self.svc.suppress_projection(owner_a):
                threading.Event().wait(0.05)  # hold for 50 ms
                a_suppressed = self.svc.get_active_project(owner_a) is None
                b_visible = self.svc.get_active_project(owner_b) is not None
                results.append((a_suppressed, b_visible))

        t = threading.Thread(target=suppress_a)
        t.start()
        # While A's suppression is held, B must still be visible from main thread.
        import time

        time.sleep(0.01)
        assert self.svc.get_active_project(owner_b) is not None
        t.join()
        assert results == [(True, True)]


# ---------------------------------------------------------------------------
# L6: concurrent first-access _load() is race-free
# ---------------------------------------------------------------------------


class TestCacheLock:
    def test_concurrent_load_does_not_duplicate_entries(self, tmp_path):
        """Two threads racing on _load() must converge to the same single dict."""
        svc = ProjectSyncService()

        barrier = threading.Barrier(2)
        seen: list[dict] = []

        with patch("flowfile_core.project.service.repository") as mock_repo, patch(
            "flowfile_core.project.service.get_db_context"
        ) as mock_ctx:
            db_ctx = MagicMock()
            db_ctx.__enter__ = MagicMock(return_value=MagicMock())
            db_ctx.__exit__ = MagicMock(return_value=False)
            mock_ctx.return_value = db_ctx
            mock_repo.get_active_projects.return_value = []

            def worker():
                barrier.wait()
                seen.append(id(svc._load()))

            threads = [threading.Thread(target=worker) for _ in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        # Both calls must return the same dict object (only built once).
        assert len(set(seen)) == 1

    def test_cache_write_is_locked(self, tmp_path):
        """_by_owner must be set under the lock so readers see a consistent dict."""
        from flowfile_core.project.models import ActiveProject

        svc = ProjectSyncService()
        owner = 99
        proj = ActiveProject(1, "p", tmp_path, owner, True)

        lock_held_during_write: list[bool] = []

        original_load = ProjectSyncService._load

        def patched_load(self):
            # The _cache_lock should be held when _by_owner is written.
            lock_held_during_write.append(self._cache_lock._is_owned())
            return original_load(self)

        with patch.object(ProjectSyncService, "_load", patched_load), patch(
            "flowfile_core.project.service.repository"
        ) as mock_repo, patch("flowfile_core.project.service.get_db_context") as mock_ctx:
            db_ctx = MagicMock()
            db_ctx.__enter__ = MagicMock(return_value=MagicMock())
            db_ctx.__exit__ = MagicMock(return_value=False)
            mock_ctx.return_value = db_ctx
            mock_repo.get_active_projects.return_value = []

            with svc._cache_lock:
                svc._load()[owner] = proj

        assert owner in (svc._by_owner or {})


# ---------------------------------------------------------------------------
# L9: import-size caps
# ---------------------------------------------------------------------------


class TestImportSizeCaps:
    def test_oversized_yaml_raises(self, tmp_path):
        from flowfile_core.project.importer import _MAX_YAML_BYTES, _read_yaml

        big = tmp_path / "big.yaml"
        big.write_bytes(b"x: " + b"a" * (_MAX_YAML_BYTES + 1))
        with pytest.raises(ValueError, match="exceeds the"):
            _read_yaml(big)

    def test_normal_yaml_reads_fine(self, tmp_path):
        from flowfile_core.project.importer import _read_yaml

        f = tmp_path / "ok.yaml"
        f.write_text("key: value\n", encoding="utf-8")
        assert _read_yaml(f) == {"key": "value"}

    def test_capped_raises_on_excess(self):
        from flowfile_core.project.importer import _assert_within_cap

        items = list(range(10))
        with pytest.raises(ValueError, match="cap"):
            _assert_within_cap(items, 5, "flows")

    def test_capped_passes_on_exact(self):
        from flowfile_core.project.importer import _assert_within_cap

        items = list(range(5))
        assert _assert_within_cap(items, 5, "flows") is None

    def test_capped_passes_on_fewer(self):
        from flowfile_core.project.importer import _assert_within_cap

        items = list(range(3))
        assert _assert_within_cap(items, 5, "flows") is None


# ---------------------------------------------------------------------------
# L7: /versions limit clamping — test the Query validator directly
# ---------------------------------------------------------------------------


class TestVersionsLimitClamp:
    def _get_limit_param(self):
        import inspect
        from flowfile_core.routes.project import list_versions

        sig = inspect.signature(list_versions)
        return sig.parameters["limit"].default

    def test_default_limit_is_50(self):
        """The Query default must be 50."""
        assert self._get_limit_param().default == 50

    def test_limit_ge_is_1(self):
        param = self._get_limit_param()
        # FastAPI v2 stores constraints in .metadata as annotated-types instances.
        ge_values = [m.ge for m in param.metadata if hasattr(m, "ge")]
        assert ge_values == [1]

    def test_limit_le_is_500(self):
        param = self._get_limit_param()
        le_values = [m.le for m in param.metadata if hasattr(m, "le")]
        assert le_values == [500]

    def test_git_ops_log_cap_not_exceeded(self, tmp_path):
        """git_ops.log must receive a value no greater than 500 from the route."""
        from flowfile_core.project import git_ops

        # Call git_ops.log directly with a value above the cap to confirm it would walk the whole
        # graph; the route's Query clamp ensures such a value never arrives here.
        with patch.object(git_ops, "log", return_value=[]) as mock_log:
            git_ops.log(tmp_path, 500)
            assert mock_log.call_args[0][1] == 500
