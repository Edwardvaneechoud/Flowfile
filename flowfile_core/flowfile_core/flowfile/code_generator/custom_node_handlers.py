import ast
import inspect
import re
import textwrap

from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.flowfile.flow_node.flow_node import FlowNode

# Import spellings that resolve the node-designer SDK. The node's own designer
# import is dropped from the inlined source and re-added canonically (below) so
# both bare-symbol and ``nd.``-style node bodies resolve after inlining.
_SDK_IMPORT_MARKERS = ("node_designer", "shared.node_designer")

# Symbols an inlined bare-import node body may reference.
_CANONICAL_SDK_SYMBOLS = (
    "CustomNodeBase, Section, NodeSettings, SingleSelect, MultiSelect, "
    "IncomingColumns, ColumnSelector, NumericInput, TextInput, "
    "ColumnActionInput, SliderInput, ToggleSwitch, SecretSelector"
)
_CANONICAL_SDK_SYMBOL_SET = frozenset(name.strip() for name in _CANONICAL_SDK_SYMBOLS.split(","))


class CustomNodeHandlersMixin(ConverterMixinBase):
    """User-defined custom-node source registration and call emission.

    Contract (matches the runtime/kernel/worker paths): ``process`` receives the
    pipeline LazyFrames directly (no hint-sniffing, no ``.collect()``); the
    return is normalized to lazy via ``isinstance`` at the call site. Multi-input
    order follows the node's connected inputs; multi-output dict returns are
    unpacked into one variable per output handle.
    """

    def _read_custom_node_source_file(self, custom_node_class: type) -> str | None:
        """Read the entire source file where a custom node class is defined.

        Returns the complete source (settings schema + node class + helpers), or
        None if not readable.
        """
        try:
            source_file = inspect.getfile(custom_node_class)
            with open(source_file) as f:
                return f.read()
        except (OSError, TypeError):
            return None

    def _handle_user_defined(self, node: FlowNode, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle user-defined custom nodes by including their class definition and calling process()."""
        custom_node_class = self._lookup_custom_node_class(node)
        if custom_node_class is None:
            return
        if not self._register_custom_node_source(node, custom_node_class):
            return
        self._emit_user_defined_call(node, custom_node_class, var_name, input_vars)

    def _lookup_custom_node_class(self, node: FlowNode) -> type | None:
        """Resolve a user-defined node's class from the registry, recording unsupported on miss."""
        from flowfile_core.configs.node_store import CUSTOM_NODE_STORE

        node_type = node.node_type
        custom_node_class = CUSTOM_NODE_STORE.get(node_type)
        if custom_node_class is None:
            self.unsupported_nodes.append(
                (node.node_id, node_type, f"User-defined node type '{node_type}' not found in the custom node registry")
            )
            self._add_comment(f"# Node {node.node_id}: User-defined node '{node_type}' - Not found in registry")
        return custom_node_class

    def _register_node_import(self, statement: str) -> None:
        """Carry a custom node's own import into the generated script.

        The node-designer import (any spelling) is skipped here because it is
        re-added in a canonical form by the caller. A ``flowfile_ctx`` import is
        also skipped: the flat script ships no sibling module, so the inline shim
        (project export ships flowfile_ctx.py) binds the name instead.
        """
        if any(marker in statement for marker in _SDK_IMPORT_MARKERS):
            return
        if re.match(r"\s*(import\s+flowfile_ctx\b|from\s+flowfile_ctx\b)", statement):
            self._needs_flowfile_ctx = True
            return
        self.imports.add(statement.strip())

    @staticmethod
    def _body_uses_bare_sdk_symbols(source: str) -> bool:
        """True if the inlined node source references an SDK symbol unqualified.

        ``nd.CustomNodeBase`` reads as an attribute (the bare name is ``nd``); only a
        bare ``CustomNodeBase`` shows up as an ``ast.Name`` in the canonical set.
        Unparseable source falls back to True so the import is emitted defensively.
        """
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return True
        return any(isinstance(node, ast.Name) and node.id in _CANONICAL_SDK_SYMBOL_SET for node in ast.walk(tree))

    def _register_custom_node_source(self, node: FlowNode, custom_node_class: type) -> bool:
        """Capture the custom node's class source for inlining into the generated script.

        Returns False (after recording the node as unsupported) when the
        source cannot be retrieved.
        """
        node_type = node.node_type
        class_name = custom_node_class.__name__
        if class_name not in self.custom_node_classes:
            # A class defined inside a function (e.g. a test fixture) shares its file
            # with unrelated code, so whole-file inlining would embed all of it —
            # fall through to the class-scoped getsource route instead.
            is_nested = custom_node_class.__qualname__ != custom_node_class.__name__
            file_source = None if is_nested else self._read_custom_node_source_file(custom_node_class)
            if file_source:
                # Lift import lines out of the inlined source (imports are emitted
                # separately at the top) but preserve the node's own runtime imports
                # so its process() body still resolves. The designer import is
                # re-added canonically below, so it is dropped here.
                lines = file_source.split("\n")
                non_import_lines = []
                import_buffer: list[str] = []
                in_multiline_import = False
                for line in lines:
                    stripped = line.strip()
                    if in_multiline_import:
                        import_buffer.append(line)
                        if ")" in stripped:
                            in_multiline_import = False
                            self._register_node_import("\n".join(import_buffer))
                            import_buffer = []
                        continue
                    if stripped.startswith("import ") or stripped.startswith("from "):
                        if "(" in stripped and ")" not in stripped:
                            in_multiline_import = True
                            import_buffer = [line]
                            continue
                        self._register_node_import(line)
                        continue
                    # Skip comments at the very start (like "# Auto-generated custom node")
                    if stripped.startswith("#") and not non_import_lines:
                        continue
                    non_import_lines.append(line)
                while non_import_lines and not non_import_lines[0].strip():
                    non_import_lines.pop(0)
                self.custom_node_classes[class_name] = "\n".join(non_import_lines)
            else:
                try:
                    source = textwrap.dedent(inspect.getsource(custom_node_class))
                except (OSError, TypeError) as e:
                    self.unsupported_nodes.append(
                        (node.node_id, node_type, f"Could not retrieve source code for user-defined node: {e}")
                    )
                    self._add_comment(
                        f"# Node {node.node_id}: User-defined node '{node_type}' - Source code unavailable"
                    )
                    return False
                settings_source = self._companion_settings_source(custom_node_class)
                self.custom_node_classes[class_name] = (
                    f"{settings_source}\n\n{source}" if settings_source else source
                )

            # The canonical alias resolves nd.-style bodies (what the designer emits).
            # The bare-symbol import is added only when the inlined body actually uses
            # unqualified SDK names (hand-authored nodes) — designer nodes never do, so
            # emitting it there would just be a dead import.
            self.imports.add("from flowfile import node_designer as nd")
            if self._body_uses_bare_sdk_symbols(self.custom_node_classes[class_name]):
                self.imports.add(f"from flowfile.node_designer import ({_CANONICAL_SDK_SYMBOLS})")
            if re.search(r"\bflowfile_ctx\b", self.custom_node_classes[class_name]):
                self._needs_flowfile_ctx = True
        # The inlined node body references pl. (and the Polars converter's return
        # normalization uses isinstance(..., pl.DataFrame)), so polars is needed
        # in both converters.
        self.imports.add("import polars as pl")
        return True

    def _companion_settings_source(self, custom_node_class: type) -> str | None:
        """Source of the node's settings-schema class when it lives in the same module —
        the class-scoped route can't pick it up from the file, and the inlined node
        class references it at class-creation time."""
        settings_schema = self._class_default(custom_node_class, "settings_schema", None)
        if settings_schema is None:
            return None
        settings_cls = type(settings_schema)
        if settings_cls.__module__ != custom_node_class.__module__:
            return None
        try:
            return textwrap.dedent(inspect.getsource(settings_cls))
        except (OSError, TypeError):
            return None

    def _custom_node_input_expr(self, input_var: str) -> str:
        """Render one input argument for ``process()``.

        Base (Polars): upstream vars are already polars LazyFrames — pass through.
        The FlowFrame converter overrides this to bridge FlowFrame → polars LazyFrame.
        """
        return input_var

    def _normalize_custom_output(self, out_expr: str) -> str:
        """Normalize ``process()``'s return to the converter's frame type.

        Base (Polars): a lazy polars frame (eager returns coerced via ``isinstance``).
        The FlowFrame converter overrides this to re-wrap the return as a FlowFrame.
        """
        return f"{out_expr}.lazy() if isinstance({out_expr}, pl.DataFrame) else {out_expr}"

    @staticmethod
    def _ordered_input_args(input_vars: dict[str, str]) -> list[str]:
        """Positional args for process(), in the node's connected-input order.

        ``_get_input_vars`` inserts keys in port order (main inputs in edge
        order, then left, then right), so insertion order is the argument order.
        The old code dropped every non-"main" key and sorted alphabetically —
        this preserves all inputs and their order.
        """
        return list(input_vars.values())

    @staticmethod
    def _class_default(custom_node_class: type, name: str, fallback):
        """Resolve a pydantic field default, honoring subclass overrides.

        Subclass attribute assignments (``environment: str = "kernel"``) become
        the field default in pydantic v2, so the field default is the source of
        truth. Fields with only a default_factory (e.g. ``dependencies``) fall
        back to the factory value.
        """
        from pydantic_core import PydanticUndefined

        field = custom_node_class.model_fields.get(name)
        if field is None:
            return fallback
        if field.default is not PydanticUndefined and field.default is not None:
            return field.default
        if field.default_factory is not None:
            return field.default_factory()
        return fallback

    def _emit_isolated_env_header(self, node: FlowNode, custom_node_class: type) -> None:
        """Emit a ``# Requires: pip install ...`` header for kernel-env nodes."""
        environment = self._class_default(custom_node_class, "environment", "local")
        requires_kernel = self._class_default(custom_node_class, "requires_kernel", False)
        if environment != "kernel" and not requires_kernel:
            return
        deps = self._class_default(custom_node_class, "dependencies", [])
        deps_list = deps if isinstance(deps, list) else []
        if deps_list:
            self._add_comment(f"# Requires: pip install {' '.join(deps_list)}")
        else:
            self._add_comment("# Requires: an isolated kernel environment (runs inline in exported code)")

    def _emit_user_defined_call(
        self, node: FlowNode, custom_node_class: type, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Emit the instantiation, settings population, and process() call for a custom node."""
        settings = node.setting_input
        class_name = custom_node_class.__name__
        settings_dict = getattr(settings, "settings", {}) or {}
        output_names = list(getattr(settings, "output_names", None) or ["main"])

        _node_name_field = custom_node_class.model_fields.get("node_name", type("", (), {"default": node.node_type}))
        self._add_code(f"# User-defined node: {_node_name_field.default}")
        self._emit_isolated_env_header(node, custom_node_class)
        self._add_code(f"_custom_node_{node.node_id} = {class_name}()")

        if settings_dict:
            self._add_code(f"_custom_node_{node.node_id}_settings = {settings_dict!r}")
            node_var = f"_custom_node_{node.node_id}"
            self._add_code(f"if {node_var}.settings_schema:")
            self._add_code(f"    {node_var}.settings_schema.populate_values({node_var}_settings)")

        input_args = ", ".join(self._custom_node_input_expr(arg) for arg in self._ordered_input_args(input_vars))

        if len(output_names) > 1:
            self._emit_multi_output_call(node, var_name, input_args, output_names)
        else:
            out_var = f"_out_{node.node_id}"
            self._add_code(f"{out_var} = _custom_node_{node.node_id}.process({input_args})")
            self._add_code(f"{var_name} = {self._normalize_custom_output(out_var)}")
        self._add_code("")

    def _emit_multi_output_call(self, node: FlowNode, var_name: str, input_args: str, output_names: list[str]) -> None:
        """Emit a multi-output custom-node call: dict return unpacked per handle.

        Output handle 0 reuses ``var_name`` (the node's primary var); each named
        output registers in ``node_handle_var_mapping`` so downstream nodes wire
        to the right handle.
        """
        outs_var = f"_outs_{node.node_id}"
        self._add_code(f"{outs_var} = _custom_node_{node.node_id}.process({input_args})")
        for index, name in enumerate(output_names):
            out_var = var_name if index == 0 else f"{var_name}_{name}"
            self._add_code(f"{out_var} = {self._normalize_custom_output(f'{outs_var}[{name!r}]')}")
            self.node_handle_var_mapping[(node.node_id, f"output-{index}")] = out_var
