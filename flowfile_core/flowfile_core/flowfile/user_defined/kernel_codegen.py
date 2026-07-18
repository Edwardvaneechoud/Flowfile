"""Kernel-script generation v2 for custom nodes.

Builds a self-contained Python script from a custom node's source file that
runs inside an isolated Docker kernel (``flowfile_ctx`` + polars only, no SDK
in the image). Unlike the deprecated ``CustomNodeBase.generate_kernel_code``
this does no ``return``-rewriting: it defines a real class and calls its real
``process`` method. Settings are baked as JSON, not ``repr()``. Generation-time
problems raise ``KernelCodegenError`` before dispatch.
"""

import ast
import json
import textwrap
from typing import Any

# Import spellings the SDK is reachable under — dropped from the generated
# script (the kernel image has no SDK; the node body only needs polars + its
# own third-party imports, and settings come through the JSON-baked proxy).
_SDK_IMPORT_ROOTS = (
    "flowfile_core.flowfile.node_designer",
    "flowfile_core.types",
    "flowfile.node_designer",
    "flowfile",
    "shared.node_designer",
)


class KernelCodegenError(RuntimeError):
    """Raised when a kernel script cannot be generated (surfaces as a node error before dispatch)."""


def _is_sdk_import(stmt: ast.Import | ast.ImportFrom) -> bool:
    if isinstance(stmt, ast.ImportFrom):
        module = stmt.module or ""
        return any(module == root or module.startswith(root + ".") for root in _SDK_IMPORT_ROOTS)
    for alias in stmt.names:
        if any(alias.name == root or alias.name.startswith(root + ".") for root in _SDK_IMPORT_ROOTS):
            return True
    return False


def _find_node_class(module: ast.Module, class_name: str) -> ast.ClassDef:
    for stmt in module.body:
        if isinstance(stmt, ast.ClassDef) and stmt.name == class_name:
            return stmt
    raise KernelCodegenError(f"Class '{class_name}' not found in node source")


def _extract_class_body(cls: ast.ClassDef, source: str) -> str:
    """Return the class's process method and helper methods, dedented, verbatim.

    Class-level attribute assignments (node_name, settings_schema, …) are
    dropped: the kernel proxy supplies ``settings_schema`` and the node's
    metadata is irrelevant at execution time. Only ``process`` and helper
    methods (and nested classes / other defs) are carried over.
    """
    lines = source.splitlines()
    members: list[str] = []
    has_process = False
    for stmt in cls.body:
        if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef):
            if stmt.name == "process":
                has_process = True
            members.append(_verbatim(stmt, lines))
        elif isinstance(stmt, ast.ClassDef):
            members.append(_verbatim(stmt, lines))
        # Attribute assignments and docstrings are intentionally skipped.
    if not has_process:
        raise KernelCodegenError(f"Class '{cls.name}' does not define a process() method")
    return "\n\n".join(members)


def _verbatim(stmt: ast.stmt, lines: list[str]) -> str:
    start = stmt.lineno
    for deco in getattr(stmt, "decorator_list", []):
        start = min(start, deco.lineno)
    segment = "\n".join(lines[start - 1 : stmt.end_lineno])
    return textwrap.dedent(segment)


def _extract_node_imports(module: ast.Module, source: str) -> list[str]:
    """Lift the node's own non-SDK imports verbatim so its process body resolves."""
    lines = source.splitlines()
    out: list[str] = []
    for stmt in module.body:
        if isinstance(stmt, ast.Import | ast.ImportFrom) and not _is_sdk_import(stmt):
            out.append(_verbatim(stmt, lines))
    return out


def _bake_settings(settings_values: dict[str, dict[str, Any]]) -> str:
    """JSON-encode the settings; raise a precise error naming the offending path."""
    for section_name, components in (settings_values or {}).items():
        if not isinstance(components, dict):
            continue
        for comp_name, value in components.items():
            try:
                json.dumps(value)
            except (TypeError, ValueError) as exc:
                raise KernelCodegenError(
                    f"Setting '{section_name}.{comp_name}' is not JSON-serializable for kernel execution: {exc}"
                ) from exc
    return json.dumps(settings_values or {})


def _indent(block: str, spaces: int = 4) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line.strip() else line for line in block.splitlines())


def _build_publish_epilogue(output_names: list[str]) -> str:
    names = output_names or ["main"]
    if len(names) == 1:
        name = names[0]
        return textwrap.dedent(
            f"""\
            if _result is None:
                raise RuntimeError("process() returned None; node '{name}' expects a frame")
            if isinstance(_result, dict):
                if {name!r} not in _result:
                    raise RuntimeError("process() returned a dict without the declared output {name!r}")
                flowfile_ctx.publish_output(_result[{name!r}], name={name!r})
            else:
                flowfile_ctx.publish_output(_result, name={name!r})"""
        )
    declared = json.dumps(names)
    return textwrap.dedent(
        f"""\
        _declared = {declared}
        if not isinstance(_result, dict):
            raise RuntimeError(
                "process() must return a dict for multi-output nodes; declared outputs: " + repr(_declared)
            )
        _missing = [_n for _n in _declared if _n not in _result]
        if _missing:
            raise RuntimeError("process() did not return declared outputs: " + repr(_missing))
        for _n in _declared:
            flowfile_ctx.publish_output(_result[_n], name=_n)"""
    )


def generate_kernel_script(
    *,
    node_source: str,
    class_name: str,
    settings_values: dict[str, dict[str, Any]],
    output_names: list[str],
    number_of_inputs: int,
) -> str:
    """Generate a self-contained kernel script for a custom node.

    Args:
        node_source: Full ``.py`` text of the node file.
        class_name: The ``CustomNodeBase`` subclass to run.
        settings_values: ``{section: {component: value}}`` (JSON-safe).
        output_names: Declared output handle names.
        number_of_inputs: Declared input-port count (documentation only; the
            script passes every LazyFrame ``read_inputs`` yields, positionally).

    Raises:
        KernelCodegenError: unparseable source, missing class/process, or a
            non-JSON setting value (named by ``section.component``).
    """
    try:
        module = ast.parse(node_source)
    except SyntaxError as exc:
        raise KernelCodegenError(f"Custom node source is not valid Python: {exc.msg}") from exc

    cls = _find_node_class(module, class_name)
    class_body = _extract_class_body(cls, node_source)
    node_imports = _extract_node_imports(module, node_source)
    settings_json = _bake_settings(settings_values)
    epilogue = _build_publish_epilogue(output_names)

    imports_block = "\n".join(node_imports)
    if imports_block:
        imports_block += "\n"

    # Route the node's logging (and any bare `logging.*`) to stderr at DEBUG while
    # process() runs; the kernel runtime captures stderr into the Test panel. The
    # handler is torn down in `finally` so the long-lived kernel never leaks handlers.
    # Root-DEBUG also unmutes chatty libraries — the log callback itself makes httpx
    # calls, and matplotlib's font manager logs hundreds of findfont lines per figure
    # — so pin the known-noisy ones to WARNING to keep the Test panel readable.
    exec_body = (
        "_inputs_by_name = flowfile_ctx.read_inputs()\n"
        "_inputs = [_lf for _lfs in _inputs_by_name.values() for _lf in _lfs]\n"
        "_result = _Node().process(*_inputs)\n"
        "\n"
        f"{epilogue}"
    )
    capture_tail = (
        "_log_handler = logging.StreamHandler(_sys.stderr)\n"
        '_log_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))\n'
        "_root_logger = logging.getLogger()\n"
        "_prev_log_level = _root_logger.level\n"
        "_root_logger.addHandler(_log_handler)\n"
        "_root_logger.setLevel(logging.DEBUG)\n"
        '_noisy_libs = ("httpx", "httpcore", "urllib3", "asyncio",\n'
        '               "matplotlib", "PIL", "fontTools", "kernel_runtime")\n'
        "for _noisy in _noisy_libs:\n"
        "    logging.getLogger(_noisy).setLevel(logging.WARNING)\n"
        "try:\n"
        f"{_indent(exec_body)}\n"
        "finally:\n"
        "    _root_logger.removeHandler(_log_handler)\n"
        "    _root_logger.setLevel(_prev_log_level)"
    )

    script = f"""\
import polars as pl
import json as _json
import logging
import sys as _sys
{imports_block}
flowfile_ctx = globals()["flowfile_ctx"]


class _AttrDict(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


def _wrap(value):
    if isinstance(value, dict):
        return _AttrDict({{_k: _wrap(_v) for _k, _v in value.items()}})
    if isinstance(value, list):
        return [_wrap(_v) for _v in value]
    return value


class _Component:
    def __init__(self, value):
        self.value = _wrap(value)

    @property
    def secret_value(self):
        raise RuntimeError("Secrets are not available inside isolated kernels yet")


class _Section:
    def __init__(self, components):
        for _name, _value in components.items():
            setattr(self, _name, _Component(_value))


class _Settings:
    def __init__(self, sections):
        for _name, _value in sections.items():
            setattr(self, _name, _Section(_value))


_SETTINGS = _json.loads({settings_json!r})


class _Node:
    settings_schema = _Settings(_SETTINGS)

{_indent(class_body)}


{capture_tail}
"""

    try:
        ast.parse(script)
    except SyntaxError as exc:  # pragma: no cover - defensive; verbatim blocks are already valid
        raise KernelCodegenError(f"Generated kernel script is not valid Python: {exc.msg}") from exc

    return script
