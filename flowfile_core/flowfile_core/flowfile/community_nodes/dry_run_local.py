"""Local CI dry-run for a community-node bundle.

Executes ``node.py``'s ``process()`` against its own ``example_inputs`` /
``example_settings`` in a **child process that never imports flowfile_core**
(importing core runs DB migrations and is unavailable on the CI/fork runner).
The child imports only ``shared.node_designer.loading`` + polars, mirroring how
the worker loads a node from shipped source text.

Row/timeout caps mirror ``flowfile_core/flowfile/user_defined/dry_run.py``.
"""

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from pydantic import BaseModel

from flowfile_core.flowfile.node_designer.parsing import (
    extract_example_inputs,
    extract_example_settings,
    extract_manifest,
)

_MAX_ROW_LIMIT = 1000
_MAX_TIMEOUT = 120

# Kernel APIs for which returning None is deliberately correct; test_dry_run_ctx_parity
# fails for any public API neither modelled on _FlowfileCtx nor listed here.
_VOID_APIS: frozenset[str] = frozenset()

# Runs in a child `python -c` with cwd inheriting the parent's sys.path (the poetry
# venv), so `import shared` resolves. Must NOT import flowfile_core. argv: folder, config.
_RUNNER = r'''
import contextlib
import json
import logging
import os
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import polars as pl

from shared.node_designer.loading import find_custom_node_class, install_import_aliases, load_node_module


@dataclass
class _ArtifactInfo:
    """Flow-local artifact metadata; mirrors ``kernel_runtime.schemas.ArtifactInfo``.

    Redeclared rather than imported: the dry-run child may import neither
    flowfile_core (DB migrations) nor kernel_runtime (not shipped in the
    ``flowfile`` PyPI distribution).
    """

    name: str
    type_name: str
    module: str
    node_id: int
    flow_id: int
    created_at: datetime
    size_bytes: int
    persisted: bool = False
    persist_pending: bool = False


@dataclass
class _GlobalArtifactInfo:
    """Global artifact metadata; mirrors ``kernel_runtime.schemas.GlobalArtifactInfo``.

    Field-for-field with the real model: no ``description``, because the real one
    has none. Inventing a field here would let a node read ``info.description``
    in CI and AttributeError in a kernel.
    """

    id: int
    name: str
    version: int
    status: str
    source_registration_id: int
    serialization_format: str
    created_at: datetime
    namespace_id: int | None = None
    python_type: str | None = None
    size_bytes: int | None = None
    tags: list = field(default_factory=list)
    owner_id: int = -1


class _CatalogUnavailableError(RuntimeError):
    """A catalog API was called in a dry run, where there is no Flowfile server."""


_DRY_RUN_FLOW_ID = -1
_DRY_RUN_NODE_ID = -1


class _FlowfileCtx:
    """In-memory stand-in for the kernel runtime's ``flowfile_ctx`` global.

    Mirrors ``kernel_runtime/kernel_runtime/flowfile_client.py``: every
    value-returning API returns a real value and raises the same
    ``KeyError``/``ValueError`` the kernel raises, so a node that branches on a
    published artifact id or reads back a stored model behaves here the way it
    will in production. Signatures match the real client's, so a call that
    type-checks against the kernel also works in the dry run.

    Two stores back it, matching the kernel's two scopes: ``_artifacts`` is
    flow-local (``publish_artifact``/``read_artifact``) and ``_globals`` is the
    versioned global store (``publish_global``/``get_global``). Catalog APIs
    need a running Flowfile server and raise a clear error rather than
    pretending; unknown or future APIs fall through to a no-op via
    ``__getattr__`` so an unmodelled call degrades instead of exploding.
    """

    def __init__(self, inputs=None):
        self._inputs: dict[str, list] = {"main": list(inputs or [])}
        self._artifacts: dict[str, tuple[Any, _ArtifactInfo]] = {}
        # name -> {version: {"id", "obj", "info"}}
        self._globals: dict[str, dict[int, dict[str, Any]]] = {}
        self._outputs: dict[str, Any] = {}
        self._next_global_id = 1
        self._shared_dir: str | None = None

    def is_dry_run(self) -> bool:
        """True: this is the community-node dry run, not a real kernel execution."""
        return True

    def read_input(self, name: str = "main") -> pl.LazyFrame:
        frames = self._check_input_available(name)
        if len(frames) == 1:
            return frames[0]
        return pl.concat(frames)

    def read_first(self, name: str = "main") -> pl.LazyFrame:
        return self._check_input_available(name)[0]

    def read_inputs(self) -> dict:
        return {name: list(frames) for name, frames in self._inputs.items()}

    def publish_output(self, df, name: str = "main") -> None:
        """Record an output frame.

        Recorded but unused on this path: the dry-run reads results from
        ``process()``'s return value (a community node must return, since core's
        kernel script publishes the return value for it).
        """
        self._outputs[name] = df.lazy() if isinstance(df, pl.DataFrame) else df

    def _check_input_available(self, name: str) -> list:
        frames = self._inputs.get(name) or []
        if not frames:
            available = sorted(k for k, v in self._inputs.items() if v)
            if not available:
                raise RuntimeError(
                    "This node has no inputs. Add example_inputs to the node class so the dry run has data."
                )
            raise KeyError("Input '%s' not found. Available inputs: %s" % (name, available))
        return frames

    def publish_artifact(self, name: str, obj: Any, preview: bool = False) -> None:
        """Store *obj* under *name*. Mirrors the kernel: a duplicate name raises ValueError.

        ``preview`` is accepted for signature parity; there is no preview panel here.
        """
        if name in self._artifacts:
            raise ValueError(
                "Artifact '%s' already exists (published by node %d). "
                "Delete it first with flowfile_ctx.delete_artifact('%s') "
                "before publishing a new one with the same name." % (name, _DRY_RUN_NODE_ID, name)
            )
        self._seed_artifact(name, obj)

    def read_artifact(self, name: str) -> Any:
        entry = self._artifacts.get(name)
        if entry is None:
            raise KeyError("Artifact '%s' not found" % name)
        return entry[0]

    def delete_artifact(self, name: str) -> None:
        if name not in self._artifacts:
            raise KeyError("Artifact '%s' not found" % name)
        del self._artifacts[name]

    def list_artifacts(self) -> list:
        return [info for _obj, info in self._artifacts.values()]

    def _seed_artifact(self, name: str, obj: Any) -> None:
        """Store without the duplicate-name guard (used by publish and by seeding)."""
        self._artifacts[name] = (
            obj,
            _ArtifactInfo(
                name=name,
                type_name=type(obj).__name__,
                module=type(obj).__module__,
                node_id=_DRY_RUN_NODE_ID,
                flow_id=_DRY_RUN_FLOW_ID,
                created_at=datetime.now(timezone.utc),
                size_bytes=sys.getsizeof(obj),
            ),
        )

    def publish_global(
        self,
        name: str,
        obj: Any,
        description: str | None = None,
        tags: list | None = None,
        namespace_id: int | None = None,
        fmt: str | None = None,
    ) -> int:
        """Persist *obj* globally and return its artifact id (monotonic from 1).

        Republishing a name adds a version, as the real store does.
        """
        return self._seed_global(name, obj, description=description, tags=tags, namespace_id=namespace_id, fmt=fmt)

    def get_global(
        self,
        name: str,
        version: int | None = None,
        namespace_id: int | None = None,
        namespace: str | None = None,
    ) -> Any:
        versions = self._globals.get(name)
        if not versions:
            raise KeyError("Artifact '%s' not found" % name)
        if version is None:
            version = max(versions)
        entry = versions.get(version)
        if entry is None:
            raise KeyError("Artifact '%s' version %s not found" % (name, version))
        return entry["obj"]

    def list_global_artifacts(self, namespace_id: int | None = None, tags: list | None = None) -> list:
        result = []
        for versions in self._globals.values():
            for entry in versions.values():
                info = entry["info"]
                if namespace_id is not None and info.namespace_id != namespace_id:
                    continue
                if tags and not set(tags).issubset(set(info.tags)):
                    continue
                result.append(info)
        return result

    def delete_global_artifact(
        self,
        name: str,
        version: int | None = None,
        namespace_id: int | None = None,
        namespace: str | None = None,
    ) -> None:
        versions = self._globals.get(name)
        if not versions:
            raise KeyError("Artifact '%s' not found" % name)
        if version is None:
            del self._globals[name]
            return
        if version not in versions:
            raise KeyError("Artifact '%s' version %s not found" % (name, version))
        del versions[version]
        if not versions:
            del self._globals[name]

    def _seed_global(
        self,
        name: str,
        obj: Any,
        description: str | None = None,
        tags: list | None = None,
        namespace_id: int | None = None,
        fmt: str | None = None,
    ) -> int:
        """``description`` is accepted (the real API takes it) but not carried:
        ``GlobalArtifactInfo`` has no field for it on either side."""
        versions = self._globals.setdefault(name, {})
        version = max(versions) + 1 if versions else 1
        artifact_id = self._next_global_id
        self._next_global_id += 1
        versions[version] = {
            "id": artifact_id,
            "obj": obj,
            "info": _GlobalArtifactInfo(
                id=artifact_id,
                name=name,
                version=version,
                status="ready",
                source_registration_id=_DRY_RUN_FLOW_ID,
                serialization_format=fmt or "pickle",
                created_at=datetime.now(timezone.utc),
                namespace_id=namespace_id,
                python_type="%s.%s" % (type(obj).__module__, type(obj).__name__),
                size_bytes=sys.getsizeof(obj),
                tags=list(tags or []),
            ),
        }
        return artifact_id

    def get_shared_location(self, filename: str) -> str:
        """Absolute path under a real temp dir, so a node can actually write files."""
        if self._shared_dir is None:
            self._shared_dir = tempfile.mkdtemp(prefix="ff-dry-run-shared-")
        full_path = os.path.join(self._shared_dir, "user_files", filename)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        return full_path

    def log(self, message, level: str = "INFO") -> None:
        print("[node:%s]" % str(level).lower(), message, file=sys.stderr)

    def log_info(self, message) -> None:
        self.log(message, "INFO")

    def log_warning(self, message) -> None:
        self.log(message, "WARNING")

    def log_error(self, message) -> None:
        self.log(message, "ERROR")

    def display(self, obj, title: str = "", max_rows: int = 10000) -> None:
        if title:
            print("=== %s ===" % title, file=sys.stderr)
        print(obj, file=sys.stderr)

    def explore(self, obj, title: str = "", max_rows: int = 10000) -> None:
        self.display(obj, title)

    def _catalog_unavailable(self, api_name: str):
        raise _CatalogUnavailableError(
            "flowfile_ctx.%s() needs a running Flowfile server and is not available in a dry run. "
            "Guard the call with `if not flowfile_ctx.is_dry_run():` so the node still dry-runs." % api_name
        )

    def read_catalog_table(self, *args, **kwargs):
        self._catalog_unavailable("read_catalog_table")

    def write_catalog_table(self, *args, **kwargs):
        self._catalog_unavailable("write_catalog_table")

    def list_catalog_tables(self, *args, **kwargs):
        self._catalog_unavailable("list_catalog_tables")

    def list_catalogs(self, *args, **kwargs):
        self._catalog_unavailable("list_catalogs")

    def get_catalog(self, *args, **kwargs):
        self._catalog_unavailable("get_catalog")

    def list_schemas(self, *args, **kwargs):
        self._catalog_unavailable("list_schemas")

    def default_schema(self, *args, **kwargs):
        self._catalog_unavailable("default_schema")

    def __getattr__(self, name):
        """No-op for APIs this fake does not model, so future SDK additions degrade.

        Dunders still raise: handing ``copy``/``pickle``/``len`` a no-op lambda
        instead of an AttributeError makes them misbehave in confusing ways.
        """
        if name.startswith("__"):
            raise AttributeError(name)
        return lambda *a, **k: None


def _artifact_aliases(settings_values):
    """bare artifact name -> qualified "catalog.schema::name" settings values naming it.

    Mirrors core's dry-run prologue: an artifact-select stores a qualified string while
    example_artifacts() is keyed by the bare name, so both spellings must be seeded.
    """
    aliases = {}

    def _record(value):
        if isinstance(value, list):
            for item in value:
                _record(item)
            return
        if not isinstance(value, str) or "::" not in value:
            return
        bare = value.rsplit("::", 1)[1]
        if bare and value not in aliases.setdefault(bare, []):
            aliases[bare].append(value)

    for components in (settings_values or {}).values():
        if not isinstance(components, dict):
            continue
        for value in components.values():
            _record(value)
    return aliases


def _seed_example_artifacts(node, ctx, settings_values=None):
    """Seed ``node.example_artifacts()`` into both artifact stores before process().

    A flat ``{name: obj}`` dict lands in the flow-local store *and* the global
    store, so a node reading via either ``read_artifact`` or ``get_global`` finds
    it without the author having to know which scope the dry run uses. Each entry
    is also seeded under any qualified reference the example settings select.
    """
    hook = getattr(node, "example_artifacts", None)
    if hook is None:
        return
    artifacts = hook()
    if artifacts is None:
        return
    # Before the emptiness check, so `[]` fails like `[1]` does.
    if not isinstance(artifacts, dict):
        raise TypeError("example_artifacts() must return a dict, got %s" % type(artifacts).__name__)
    aliases = _artifact_aliases(settings_values)
    for name, obj in artifacts.items():
        for key in [name] + aliases.get(name, []):
            ctx._seed_artifact(key, obj)
            ctx._seed_global(key, obj)


def _normalize(result, output_names):
    if isinstance(result, dict):
        unknown = sorted(set(result) - set(output_names))
        if unknown:
            raise ValueError("process() returned unknown outputs %s; declared %s" % (unknown, output_names))
        missing = [n for n in output_names if n not in result]
        if missing:
            raise ValueError("process() did not return declared outputs %s" % missing)
        frames = result
    elif isinstance(result, (pl.LazyFrame, pl.DataFrame)):
        if len(output_names) > 1:
            raise ValueError(
                "node declares %d outputs %s but process() returned a single frame" % (len(output_names), output_names)
            )
        frames = {output_names[0]: result}
    elif result is None:
        raise ValueError("process() returned None; return a DataFrame or LazyFrame")
    else:
        raise TypeError("process() must return a DataFrame, LazyFrame or dict, got %s" % type(result).__name__)
    return {name: (f.lazy() if isinstance(f, pl.DataFrame) else f) for name, f in frames.items()}


def _run(folder, config):
    install_import_aliases()
    with open(folder + "/node.py", encoding="utf-8") as f:
        source = f.read()
    module = load_node_module(source=source)
    module.__dict__.setdefault("logging", logging)
    lazy_inputs = [pl.LazyFrame(d) for d in config.get("example_inputs") or []]
    ctx = _FlowfileCtx(inputs=lazy_inputs)
    module.__dict__["flowfile_ctx"] = ctx
    node = find_custom_node_class(module, config["class_name"])()
    settings = config.get("example_settings") or {}
    if settings and node.settings_schema:
        node.settings_schema.populate_values(settings)
    node.set_execution_context(-1, resolver=None)
    _seed_example_artifacts(node, ctx, settings)
    outputs = _normalize(node.process(*lazy_inputs), config["output_names"])
    names = []
    for name, lf in outputs.items():
        lf.head(config["row_limit"]).collect()
        names.append(name)
    return {"success": True, "error": None, "output_names": names}


def main():
    folder = sys.argv[1]
    with open(sys.argv[2], encoding="utf-8") as f:
        config = json.load(f)
    real_stdout = sys.stdout
    verdict = {"success": False, "error": "dry-run runner did not complete", "output_names": []}
    try:
        with contextlib.redirect_stdout(sys.stderr):
            verdict = _run(folder, config)
    except Exception as e:
        verdict = {"success": False, "error": str(e), "output_names": []}
    real_stdout.write(json.dumps(verdict))
    real_stdout.flush()


if __name__ == "__main__":
    main()
'''


class DryRunOutcome(BaseModel):
    success: bool = False
    error: str | None = None
    duration_ms: float = 0.0
    output_names: list[str] = []


def run_bundle_dry_run(folder: Path, *, timeout_seconds: int = 120, row_limit: int = 1000) -> DryRunOutcome:
    """Execute a bundle's ``process()`` against its example data in a child process.

    Returns a typed verdict; never raises for user-code failures (they surface in
    ``error``). The child is killed if it exceeds ``timeout_seconds``.
    """
    folder = Path(folder)
    node_py = folder / "node.py"
    if not node_py.is_file():
        return DryRunOutcome(success=False, error="node.py not found")

    row_limit = max(1, min(row_limit, _MAX_ROW_LIMIT))
    timeout_seconds = max(1, min(timeout_seconds, _MAX_TIMEOUT))

    source = node_py.read_text(encoding="utf-8")
    manifest = extract_manifest(source)
    if not manifest.class_name:
        return DryRunOutcome(success=False, error="No CustomNodeBase subclass found in node.py")

    config = {
        "class_name": manifest.class_name,
        # The AST lift keeps a declared `output_names = []` verbatim; the exec side defaults it.
        "output_names": manifest.output_names or ["main"],
        "number_of_inputs": manifest.number_of_inputs,
        "row_limit": row_limit,
        "example_inputs": extract_example_inputs(source) or [],
        "example_settings": extract_example_settings(source) or {},
    }

    started = time.monotonic()
    # Windows cannot open a NamedTemporaryFile that the parent still holds, so the
    # config lives in its own temp dir and is closed before the child starts.
    with tempfile.TemporaryDirectory(prefix="ff-dry-run-") as tmp_dir:
        cfg_path = Path(tmp_dir) / "config.json"
        cfg_path.write_text(json.dumps(config), encoding="utf-8")
        try:
            completed = subprocess.run(
                [sys.executable, "-c", _RUNNER, str(folder), str(cfg_path)],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            return DryRunOutcome(
                success=False,
                error=f"Dry run exceeded the {timeout_seconds}s time limit and was killed.",
                duration_ms=(time.monotonic() - started) * 1000,
            )

    duration_ms = (time.monotonic() - started) * 1000
    verdict = _parse_verdict(completed.stdout)
    if verdict is None:
        detail = (completed.stderr or completed.stdout or "no output").strip()[-500:]
        return DryRunOutcome(success=False, error=f"Dry-run runner failed: {detail}", duration_ms=duration_ms)
    return DryRunOutcome(
        success=bool(verdict.get("success")),
        error=verdict.get("error"),
        duration_ms=duration_ms,
        output_names=list(verdict.get("output_names") or []),
    )


def _parse_verdict(stdout: str) -> dict | None:
    stdout = (stdout or "").strip()
    if not stdout:
        return None
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None
