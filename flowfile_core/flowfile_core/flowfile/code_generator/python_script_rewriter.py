"""
AST-based rewriter for python_script node code generation.

Transforms flowfile.* API calls in user code into plain Python equivalents,
enabling code generation for python_script nodes that normally execute inside
Docker kernel containers.

Artifacts are scoped per kernel — each kernel gets its own sub-dict inside
``_artifacts``, matching the runtime behaviour where every kernel container
has an independent artifact store.

Mapping:
    flowfile.read_input()             → function parameter (input_df)
    flowfile.read_inputs()            → function parameter (inputs)
    flowfile.publish_output(expr)     → return statement
    flowfile.publish_artifact("n", o) → _artifacts["<kernel_id>"]["n"] = o
    flowfile.read_artifact("n")       → _artifacts["<kernel_id>"]["n"]
    flowfile.delete_artifact("n")     → del _artifacts["<kernel_id>"]["n"]
    flowfile.list_artifacts()         → _artifacts["<kernel_id>"]
    flowfile.log(msg, level)          → print(f"[{level}] {msg}")
"""

from __future__ import annotations

import ast
import textwrap
from dataclasses import dataclass, field
from typing import Literal

# Maps pip package names to their Python import module names
# when they differ from the package name.
PACKAGE_TO_IMPORT_MAP: dict[str, list[str]] = {
    "scikit-learn": ["sklearn"],
    "pillow": ["PIL"],
    "opencv-python": ["cv2"],
    "opencv-python-headless": ["cv2"],
    "beautifulsoup4": ["bs4"],
    "pyyaml": ["yaml"],
    "pytorch": ["torch"],
    "tensorflow-gpu": ["tensorflow"],
}


def get_import_names(package: str) -> list[str]:
    """Return the import name(s) for a pip package."""
    return PACKAGE_TO_IMPORT_MAP.get(package, [package.replace("-", "_")])


@dataclass
class FlowfileUsageAnalysis:
    """Results of analyzing flowfile.* API usage in user code."""

    input_mode: Literal["none", "single", "multi"] = "none"
    has_read_input: bool = False
    has_read_inputs: bool = False
    has_output: bool = False
    output_exprs: list[ast.expr] = field(default_factory=list)
    passthrough_output: bool = False

    artifacts_published: list[tuple[str, ast.expr]] = field(default_factory=list)
    artifacts_consumed: list[str] = field(default_factory=list)
    artifacts_deleted: list[str] = field(default_factory=list)

    has_logging: bool = False
    has_list_artifacts: bool = False

    # For error reporting
    dynamic_artifact_names: list[ast.AST] = field(default_factory=list)
    unsupported_calls: list[tuple[str, ast.AST]] = field(default_factory=list)


def _is_flowfile_call(node: ast.Call, method: str | None = None) -> bool:
    """Check if an AST Call node is a flowfile.* method call."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute):
        if isinstance(func.value, ast.Name) and func.value.id == "flowfile":
            if method is None:
                return True
            return func.attr == method
    return False


def _is_passthrough_output(node: ast.Call) -> bool:
    """Check if publish_output argument is flowfile.read_input()."""
    if not node.args:
        return False
    arg = node.args[0]
    return _is_flowfile_call(arg, "read_input") if isinstance(arg, ast.Call) else False


class _FlowfileUsageVisitor(ast.NodeVisitor):
    """Walk the AST to collect information about flowfile.* API usage."""

    def __init__(self) -> None:
        self.analysis = FlowfileUsageAnalysis()

    def visit_Call(self, node: ast.Call) -> None:
        if _is_flowfile_call(node):
            method = node.func.attr
            if method == "read_input":
                self.analysis.has_read_input = True
                if not self.analysis.has_read_inputs:
                    self.analysis.input_mode = "single"
            elif method == "read_inputs":
                self.analysis.has_read_inputs = True
                self.analysis.input_mode = "multi"
            elif method == "publish_output":
                self.analysis.has_output = True
                if node.args:
                    self.analysis.output_exprs.append(node.args[0])
                    if _is_passthrough_output(node):
                        self.analysis.passthrough_output = True
            elif method == "publish_artifact":
                if len(node.args) >= 2:
                    name_node = node.args[0]
                    if isinstance(name_node, ast.Constant) and isinstance(name_node.value, str):
                        self.analysis.artifacts_published.append((name_node.value, node.args[1]))
                    else:
                        self.analysis.dynamic_artifact_names.append(node)
            elif method == "read_artifact":
                if node.args:
                    name_node = node.args[0]
                    if isinstance(name_node, ast.Constant) and isinstance(name_node.value, str):
                        self.analysis.artifacts_consumed.append(name_node.value)
                    else:
                        self.analysis.dynamic_artifact_names.append(node)
            elif method == "delete_artifact":
                if node.args:
                    name_node = node.args[0]
                    if isinstance(name_node, ast.Constant) and isinstance(name_node.value, str):
                        self.analysis.artifacts_deleted.append(name_node.value)
                    else:
                        self.analysis.dynamic_artifact_names.append(node)
            elif method == "log" or method in ("log_info", "log_warning", "log_error"):
                self.analysis.has_logging = True
            elif method == "list_artifacts":
                self.analysis.has_list_artifacts = True
            elif method in ("display", "publish_global", "get_global",
                            "list_global_artifacts", "delete_global_artifact"):
                self.analysis.unsupported_calls.append((method, node))
        self.generic_visit(node)


def analyze_flowfile_usage(code: str) -> FlowfileUsageAnalysis:
    """Parse user code and analyze flowfile.* API usage.

    Args:
        code: The raw Python source code from a python_script node.

    Returns:
        FlowfileUsageAnalysis with details about how the flowfile API is used.

    Raises:
        SyntaxError: If the code cannot be parsed.
    """
    tree = ast.parse(code)
    visitor = _FlowfileUsageVisitor()
    visitor.visit(tree)
    return visitor.analysis


class _FlowfileCallRewriter(ast.NodeTransformer):
    """Rewrite flowfile.* API calls to plain Python equivalents.

    Artifact operations are scoped to a kernel-specific sub-dict so that
    each kernel's artifacts stay isolated, matching runtime semantics.
    """

    def __init__(self, analysis: FlowfileUsageAnalysis, kernel_id: str | None = None) -> None:
        self.analysis = analysis
        self.kernel_id = kernel_id or "_default"
        self.input_var = "input_df" if analysis.input_mode == "single" else "inputs"
        self._last_output_expr: ast.expr | None = None
        # Track which publish_output call is the last one
        if analysis.output_exprs:
            self._last_output_expr = analysis.output_exprs[-1]

    # --- helpers for kernel-scoped artifact access ---

    def _kernel_artifacts_node(self, ctx: type[ast.expr_context] = ast.Load) -> ast.Subscript:
        """Build ``_artifacts["<kernel_id>"]`` AST node."""
        return ast.Subscript(
            value=ast.Name(id="_artifacts", ctx=ast.Load()),
            slice=ast.Constant(value=self.kernel_id),
            ctx=ctx(),
        )

    def _artifact_subscript(self, name_node: ast.expr, ctx: type[ast.expr_context] = ast.Load) -> ast.Subscript:
        """Build ``_artifacts["<kernel_id>"]["<name>"]`` AST node."""
        return ast.Subscript(
            value=self._kernel_artifacts_node(),
            slice=name_node,
            ctx=ctx(),
        )

    # --- visitors ---

    def visit_Call(self, node: ast.Call) -> ast.AST:
        # First transform any nested calls
        self.generic_visit(node)

        if not _is_flowfile_call(node):
            return node

        method = node.func.attr

        if method == "read_input":
            if self.analysis.input_mode == "multi":
                # Both read_input and read_inputs used — read_input() → inputs["main"][0]
                return ast.Subscript(
                    value=ast.Subscript(
                        value=ast.Name(id="inputs", ctx=ast.Load()),
                        slice=ast.Constant(value="main"),
                        ctx=ast.Load(),
                    ),
                    slice=ast.Constant(value=0),
                    ctx=ast.Load(),
                )
            # flowfile.read_input() → input_df
            return ast.Name(id=self.input_var, ctx=ast.Load())

        if method == "read_inputs":
            # flowfile.read_inputs() → inputs
            return ast.Name(id=self.input_var, ctx=ast.Load())

        if method == "read_artifact":
            # flowfile.read_artifact("name") → _artifacts["kernel_id"]["name"]
            return self._artifact_subscript(node.args[0])

        if method == "list_artifacts":
            # flowfile.list_artifacts() → _artifacts["kernel_id"]
            return self._kernel_artifacts_node()

        if method == "log":
            return self._make_log_print(node)

        if method == "log_info":
            return self._make_log_print_with_level(node, "INFO")

        if method == "log_warning":
            return self._make_log_print_with_level(node, "WARNING")

        if method == "log_error":
            return self._make_log_print_with_level(node, "ERROR")

        return node

    def visit_Expr(self, node: ast.Expr) -> ast.AST | None:
        # Transform nested calls first
        self.generic_visit(node)

        if not isinstance(node.value, ast.Call):
            return node

        call = node.value
        if not _is_flowfile_call(call):
            return node

        method = call.func.attr

        if method == "publish_output":
            # Remove publish_output statements — we handle via return
            return None

        if method == "publish_artifact":
            # flowfile.publish_artifact("name", obj) → _artifacts["kernel_id"]["name"] = obj
            if len(call.args) >= 2:
                return ast.Assign(
                    targets=[self._artifact_subscript(call.args[0], ctx=ast.Store)],
                    value=call.args[1],
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                )
            return node

        if method == "delete_artifact":
            # flowfile.delete_artifact("name") → del _artifacts["kernel_id"]["name"]
            if call.args:
                return ast.Delete(
                    targets=[self._artifact_subscript(call.args[0], ctx=ast.Del)],
                    lineno=node.lineno,
                    col_offset=node.col_offset,
                )
            return node

        return node

    @staticmethod
    def _make_log_print(node: ast.Call) -> ast.Call:
        """Transform flowfile.log("msg", "LEVEL") → print("[LEVEL] msg")."""
        msg_arg = node.args[0] if node.args else ast.Constant(value="")

        # Get level from second arg or keyword
        level: ast.expr | None = None
        if len(node.args) >= 2:
            level = node.args[1]
        else:
            for kw in node.keywords:
                if kw.arg == "level":
                    level = kw.value
                    break

        if level is None:
            level = ast.Constant(value="INFO")

        # Build print(f"[{level}] {msg}")
        format_str = ast.JoinedStr(
            values=[
                ast.Constant(value="["),
                ast.FormattedValue(value=level, conversion=-1),
                ast.Constant(value="] "),
                ast.FormattedValue(value=msg_arg, conversion=-1),
            ]
        )

        return ast.Call(
            func=ast.Name(id="print", ctx=ast.Load()),
            args=[format_str],
            keywords=[],
        )

    @staticmethod
    def _make_log_print_with_level(node: ast.Call, level_str: str) -> ast.Call:
        """Transform flowfile.log_info("msg") → print("[INFO] msg")."""
        msg_arg = node.args[0] if node.args else ast.Constant(value="")

        format_str = ast.JoinedStr(
            values=[
                ast.Constant(value=f"[{level_str}] "),
                ast.FormattedValue(value=msg_arg, conversion=-1),
            ]
        )

        return ast.Call(
            func=ast.Name(id="print", ctx=ast.Load()),
            args=[format_str],
            keywords=[],
        )


def rewrite_flowfile_calls(
    code: str,
    analysis: FlowfileUsageAnalysis,
    kernel_id: str | None = None,
) -> str:
    """Rewrite flowfile.* API calls in user code to plain Python.

    This removes/replaces flowfile API calls but does NOT add function
    wrapping, return statements, or import stripping. Those are handled
    by ``build_function_code``.

    Args:
        code: The raw Python source from a python_script node.
        analysis: Pre-computed analysis of flowfile usage.
        kernel_id: The kernel ID for scoping artifact operations.

    Returns:
        The rewritten source code with flowfile calls replaced.
    """
    tree = ast.parse(code)
    rewriter = _FlowfileCallRewriter(analysis, kernel_id=kernel_id)
    new_tree = rewriter.visit(tree)
    # Remove None nodes (deleted statements)
    new_tree.body = [node for node in new_tree.body if node is not None]
    ast.fix_missing_locations(new_tree)
    return ast.unparse(new_tree)


def extract_imports(code: str) -> list[str]:
    """Extract import statements from user code, excluding flowfile imports.

    Args:
        code: The raw Python source code.

    Returns:
        List of import statement strings (each is a full import line).
    """
    tree = ast.parse(code)
    imports: list[str] = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Import):
            # Filter out "import flowfile"
            non_flowfile_aliases = [alias for alias in node.names if alias.name != "flowfile"]
            if non_flowfile_aliases:
                # Reconstruct import with only non-flowfile names
                filtered = ast.Import(names=non_flowfile_aliases)
                ast.fix_missing_locations(filtered)
                imports.append(ast.unparse(filtered))
        elif isinstance(node, ast.ImportFrom):
            if node.module and "flowfile" not in node.module:
                imports.append(ast.unparse(node))
            elif node.module is None:
                imports.append(ast.unparse(node))
    return imports


def _strip_imports_and_flowfile(code: str) -> str:
    """Remove import statements and flowfile import from code body.

    Returns the code with all top-level import/from-import statements removed.
    """
    tree = ast.parse(code)
    new_body = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            # Keep non-flowfile imports? No — imports are extracted separately
            continue
        elif isinstance(node, ast.ImportFrom):
            continue
        else:
            new_body.append(node)
    tree.body = new_body
    if not tree.body:
        return ""
    ast.fix_missing_locations(tree)
    return ast.unparse(tree)


def build_function_code(
    node_id: int,
    rewritten_code: str,
    analysis: FlowfileUsageAnalysis,
    input_vars: dict[str, str],
    kernel_id: str | None = None,
) -> tuple[str, str]:
    """Assemble rewritten code into a function definition and call.

    Args:
        node_id: The node ID for naming.
        rewritten_code: The AST-rewritten code (from rewrite_flowfile_calls,
            with imports already stripped).
        analysis: The flowfile usage analysis.
        input_vars: Mapping of input names to variable names from upstream nodes.
        kernel_id: The kernel ID (used to scope return expressions).

    Returns:
        Tuple of (function_definition, call_code).
        E.g.:
            ("def _node_5(input_df: pl.LazyFrame) -> pl.LazyFrame:\\n    ...",
             "df_5 = _node_5(df_3)")
    """
    func_name = f"_node_{node_id}"
    var_name = f"df_{node_id}"

    # Build parameter list and arguments
    params: list[str] = []
    args: list[str] = []

    if analysis.input_mode == "single":
        params.append("input_df: pl.LazyFrame")
        main_var = input_vars.get("main")
        if main_var is None:
            # Multiple main inputs — pick first
            for k in sorted(input_vars.keys()):
                if k.startswith("main"):
                    main_var = input_vars[k]
                    break
        args.append(main_var or "pl.LazyFrame()")
    elif analysis.input_mode == "multi":
        # Runtime returns dict[str, list[pl.LazyFrame]] — each input name
        # maps to a *list* of LazyFrames (multiple connections can share a name).
        params.append("inputs: dict[str, list[pl.LazyFrame]]")
        # Group input_vars by their base name (strip _0, _1 suffixes).
        grouped = _group_input_vars(input_vars)
        dict_entries = ", ".join(
            f'"{k}": [{", ".join(vs)}]' for k, vs in sorted(grouped.items())
        )
        args.append("{" + dict_entries + "}")

    param_str = ", ".join(params)
    return_type = "pl.LazyFrame" if params else "pl.LazyFrame | None"

    # Build function body
    body_lines: list[str] = []

    # Add warnings for unsupported calls / dynamic artifact names
    if analysis.unsupported_calls:
        methods = sorted({m for m, _ in analysis.unsupported_calls})
        body_lines.append(f"# WARNING: The following flowfile API calls are not supported in code generation")
        body_lines.append(f"# and will not work outside the kernel runtime: {', '.join(methods)}")
    if analysis.dynamic_artifact_names:
        body_lines.append("# WARNING: Dynamic artifact names detected — these may not resolve correctly")

    # Strip imports from rewritten code (they go to top-level)
    body_code = _strip_imports_and_flowfile(rewritten_code)

    if body_code:
        for line in body_code.split("\n"):
            body_lines.append(line)

    # Add return statement
    if analysis.has_output and analysis.output_exprs:
        last_expr = analysis.output_exprs[-1]
        if analysis.passthrough_output and analysis.input_mode == "single":
            # publish_output(read_input()) → return input_df
            body_lines.append("return input_df")
        else:
            output_return = _build_return_for_output(last_expr, analysis, kernel_id=kernel_id)
            body_lines.append(output_return)
    elif analysis.input_mode == "single":
        # No explicit output — pass through input
        body_lines.append("return input_df")
    elif analysis.input_mode == "multi":
        # Pass through first input list
        first_key = sorted(input_vars.keys())[0] if input_vars else "main"
        # Strip _0 suffix to get the base name
        base_key = _base_input_name(first_key)
        body_lines.append(f'return inputs["{base_key}"][0]')
    elif not params:
        body_lines.append("return None")

    if not body_lines:
        body_lines.append("pass")

    # Assemble function definition
    indented_body = textwrap.indent("\n".join(body_lines), "    ")
    func_def = f"def {func_name}({param_str}) -> {return_type}:\n{indented_body}"

    # Build call
    arg_str = ", ".join(args)
    call_code = f"{var_name} = {func_name}({arg_str})"

    return func_def, call_code


def _base_input_name(key: str) -> str:
    """Strip numeric suffix from input var keys: 'main_0' → 'main'."""
    parts = key.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0]
    return key


def _group_input_vars(input_vars: dict[str, str]) -> dict[str, list[str]]:
    """Group input variable names by their base name.

    E.g. {"main_0": "df_1", "main_1": "df_3"} → {"main": ["df_1", "df_3"]}
         {"main": "df_1"} → {"main": ["df_1"]}
    """
    grouped: dict[str, list[str]] = {}
    for key, var in sorted(input_vars.items()):
        base = _base_input_name(key)
        grouped.setdefault(base, []).append(var)
    return grouped


def _build_return_for_output(
    output_expr: ast.expr,
    analysis: FlowfileUsageAnalysis,
    kernel_id: str | None = None,
) -> str:
    """Build a return statement from a publish_output expression.

    The expression is the original AST node from publish_output(expr).
    We need to rewrite any flowfile calls in it and then produce the return.
    """
    # Create a temporary module to transform the expression
    temp_code = ast.unparse(output_expr)

    # Check if it's just a variable name — common pattern like publish_output(result)
    # In that case, ensure .lazy() is called for DataFrame returns
    # We add .lazy() wrapper as a safety measure for DataFrames
    rewriter = _FlowfileCallRewriter(analysis, kernel_id=kernel_id)
    expr_tree = ast.parse(temp_code, mode="eval")
    new_expr = rewriter.visit(expr_tree)
    ast.fix_missing_locations(new_expr)
    rewritten = ast.unparse(new_expr)

    # Check if the expression already has .lazy() call
    if rewritten.endswith(".lazy()"):
        return f"return {rewritten}"

    # If the expression is just a variable that likely holds a DataFrame,
    # wrap with .lazy() to ensure LazyFrame return
    return f"return {rewritten}.lazy()"


def get_required_packages(
    user_imports: list[str],
    kernel_packages: list[str],
) -> list[str]:
    """Cross-reference user imports with kernel packages.

    Args:
        user_imports: Import statement strings from user code.
        kernel_packages: Package names from kernel configuration.

    Returns:
        Sorted list of kernel packages that are actually used.
    """
    # Build reverse map: import_name → package_name
    import_to_package: dict[str, str] = {}
    for pkg in kernel_packages:
        for imp_name in get_import_names(pkg):
            import_to_package[imp_name] = pkg

    # Parse user imports to get root module names
    used_packages: set[str] = set()
    for imp_str in user_imports:
        try:
            tree = ast.parse(imp_str)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root_module = alias.name.split(".")[0]
                    if root_module in import_to_package:
                        used_packages.add(import_to_package[root_module])
            elif isinstance(node, ast.ImportFrom) and node.module:
                root_module = node.module.split(".")[0]
                if root_module in import_to_package:
                    used_packages.add(import_to_package[root_module])

    return sorted(used_packages)
