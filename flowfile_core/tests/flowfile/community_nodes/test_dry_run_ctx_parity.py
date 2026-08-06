"""Drift guard: the dry-run ``flowfile_ctx`` fake must track the real kernel client.

Every public API must be modelled on the fake or listed in ``_VOID_APIS``, else a
new kernel API silently returns ``None`` in CI dry runs.

Both sides are read with ``ast``, not imported: ``kernel_runtime`` is not on the
core test path, and the fake lives inside the ``_RUNNER`` source string. Same
approach as ``test_project_exporter.py::test_shim_covers_kernel_public_api``.
"""

import ast
from pathlib import Path

import pytest

from flowfile_core.flowfile.community_nodes import dry_run_local


def _repo_root() -> Path:
    root = Path(__file__).resolve()
    while not (root / "kernel_runtime").is_dir():
        root = root.parent
        assert root != root.parent, "could not locate the repo root"
    return root


def _arg_names(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    a = node.args
    return [arg.arg for arg in (*a.posonlyargs, *a.args, *a.kwonlyargs)]


def _accepts_anything(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """A ``*args, **kwargs`` passthrough cannot drift out of signature parity."""
    return node.args.vararg is not None and node.args.kwarg is not None


def _kernel_client_api() -> dict[str, list[str]]:
    """Public module-level callables of the real client, name -> arg names."""
    path = _repo_root() / "kernel_runtime" / "kernel_runtime" / "flowfile_client.py"
    module = ast.parse(path.read_text(encoding="utf-8"))
    return {
        stmt.name: _arg_names(stmt)
        for stmt in module.body
        if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef) and not stmt.name.startswith("_")
    }


def _fake_ctx_class() -> ast.ClassDef:
    for stmt in ast.parse(dry_run_local._RUNNER).body:
        if isinstance(stmt, ast.ClassDef) and stmt.name == "_FlowfileCtx":
            return stmt
    pytest.fail("_FlowfileCtx class not found in dry_run_local._RUNNER")


def _fake_methods() -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    return {
        stmt.name: stmt
        for stmt in _fake_ctx_class().body
        if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def test_fake_covers_every_public_kernel_api():
    """No public kernel API may fall through to the fake's no-op ``__getattr__``."""
    missing = sorted(set(_kernel_client_api()) - set(_fake_methods()) - dry_run_local._VOID_APIS)
    assert not missing, (
        f"kernel flowfile_ctx APIs neither implemented on the dry-run fake nor listed in "
        f"_VOID_APIS: {missing}. Implement them in dry_run_local._RUNNER's _FlowfileCtx, or add "
        f"them to _VOID_APIS if returning None really is the right dry-run behaviour."
    )


def test_fake_signatures_match_the_kernel_client():
    kernel_api = _kernel_client_api()
    mismatched = []
    for name, method in _fake_methods().items():
        if name not in kernel_api or _accepts_anything(method):
            continue
        fake_args = _arg_names(method)
        assert fake_args and fake_args[0] == "self", f"_FlowfileCtx.{name} must be a bound method"
        if fake_args[1:] != kernel_api[name]:
            mismatched.append(f"{name}: fake{fake_args[1:]} != client{kernel_api[name]}")
    assert not mismatched, "dry-run fake signatures drifted from the kernel client: " + "; ".join(mismatched)


def test_void_apis_are_real_kernel_apis():
    """A stale _VOID_APIS entry would silently weaken the coverage assertion."""
    unknown = sorted(dry_run_local._VOID_APIS - set(_kernel_client_api()))
    assert not unknown, f"_VOID_APIS lists names that are not public kernel APIs: {unknown}"


def test_fake_does_not_invent_apis_the_kernel_lacks():
    """A fake-only method works in CI and fails in production — ``log_debug`` was one."""
    kernel_api = _kernel_client_api()
    invented = sorted(
        name for name in _fake_methods() if not name.startswith("_") and name not in kernel_api
    )
    assert not invented, (
        f"the dry-run fake defines APIs the real kernel client does not have: {invented}. "
        f"Add them to kernel_runtime/kernel_runtime/flowfile_client.py or remove them."
    )


def test_is_dry_run_is_on_both_sides():
    """The signal itself must not be the thing that drifts."""
    assert "is_dry_run" in _kernel_client_api(), "is_dry_run() is missing from the real kernel client"
    assert "is_dry_run" in _fake_methods(), "is_dry_run() is missing from the dry-run fake"
