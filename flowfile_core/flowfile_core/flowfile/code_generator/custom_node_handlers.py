import inspect
import typing

from flowfile_core.configs import logger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.flowfile.flow_node.flow_node import FlowNode


class CustomNodeHandlersMixin(ConverterMixinBase):
    """User-defined custom-node source registration and call emission."""

    def _check_process_method_signature(self, custom_node_class: type) -> tuple[bool, bool]:
        """
        Check the process method signature to determine if collect/lazy is needed.

        Returns:
            Tuple of (needs_collect, needs_lazy):
            - needs_collect: True if inputs need to be collected to DataFrame before passing to process()
            - needs_lazy: True if output needs to be converted to LazyFrame after process()
        """
        needs_collect = True
        needs_lazy = True

        process_method = getattr(custom_node_class, "process", None)
        if process_method is None:
            return needs_collect, needs_lazy

        try:
            type_hints = typing.get_type_hints(process_method)

            return_type = type_hints.get("return")
            if return_type is not None:
                return_type_str = str(return_type)
                if "LazyFrame" in return_type_str:
                    needs_lazy = False

            sig = inspect.signature(process_method)
            params = list(sig.parameters.values())
            for param in params[1:]:
                if param.annotation != inspect.Parameter.empty:
                    param_type_str = str(param.annotation)
                    if "LazyFrame" in param_type_str:
                        needs_collect = False
                        break
                if param.name in type_hints:
                    hint_str = str(type_hints[param.name])
                    if "LazyFrame" in hint_str:
                        needs_collect = False
                        break
        except (NameError, AttributeError, ValueError, TypeError) as e:
            # If we can't determine types, use defaults (collect + lazy)
            logger.debug(f"Could not determine process method signature: {e}")

        return needs_collect, needs_lazy

    def _read_custom_node_source_file(self, custom_node_class: type) -> str | None:
        """
        Read the entire source file where a custom node class is defined.
        This includes all class definitions in that file (settings schemas, etc.).

        Returns:
            The complete source code from the file, or None if not readable.
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

        The node_designer import is skipped here because it is re-added in a
        canonical multi-symbol form by the caller.
        """
        if "node_designer" in statement:
            return
        self.imports.add(statement.strip())

    def _register_custom_node_source(self, node: FlowNode, custom_node_class: type) -> bool:
        """Capture the custom node's class source for inlining into the generated script.

        Returns False (after recording the node as unsupported) when the
        source cannot be retrieved.
        """
        node_type = node.node_type
        class_name = custom_node_class.__name__
        if class_name not in self.custom_node_classes:
            file_source = self._read_custom_node_source_file(custom_node_class)
            if file_source:
                # Lift import lines out of the inlined source (imports are emitted
                # separately at the top) but preserve the node's own runtime imports
                # so its process() body still resolves. The node_designer import is
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
                    self.custom_node_classes[class_name] = inspect.getsource(custom_node_class)
                except (OSError, TypeError) as e:
                    self.unsupported_nodes.append(
                        (node.node_id, node_type, f"Could not retrieve source code for user-defined node: {e}")
                    )
                    self._add_comment(
                        f"# Node {node.node_id}: User-defined node '{node_type}' - Source code unavailable"
                    )
                    return False

            self.imports.add(
                "from flowfile_core.flowfile.node_designer import ("
                "CustomNodeBase, Section, NodeSettings, SingleSelect, MultiSelect, "
                "IncomingColumns, ColumnSelector, NumericInput, TextInput, "
                "ColumnActionInput, SliderInput, ToggleSwitch)"
            )
        return True

    def _emit_user_defined_call(
        self, node: FlowNode, custom_node_class: type, var_name: str, input_vars: dict[str, str]
    ) -> None:
        """Emit the instantiation, settings population, and process() call for a custom node."""
        settings = node.setting_input
        class_name = custom_node_class.__name__
        settings_dict = getattr(settings, "settings", {}) or {}

        needs_collect, needs_lazy = self._check_process_method_signature(custom_node_class)

        _node_name_field = custom_node_class.model_fields.get("node_name", type("", (), {"default": node.node_type}))
        self._add_code(f"# User-defined node: {_node_name_field.default}")
        self._add_code(f"_custom_node_{node.node_id} = {class_name}()")

        if settings_dict:
            self._add_code(f"_custom_node_{node.node_id}_settings = {repr(settings_dict)}")
            self._add_code(f"if _custom_node_{node.node_id}.settings_schema:")
            node_var = f"_custom_node_{node.node_id}"
            self._add_code(f"    {node_var}.settings_schema.populate_values({node_var}_settings)")

        if len(input_vars) == 0:
            input_args = ""
        elif len(input_vars) == 1:
            input_df = next(iter(input_vars.values()))
            input_args = f"{input_df}.collect()" if needs_collect else input_df
        else:
            arg_list = []
            for key in sorted(input_vars.keys()):
                if key.startswith("main"):
                    if needs_collect:
                        arg_list.append(f"{input_vars[key]}.collect()")
                    else:
                        arg_list.append(input_vars[key])
            input_args = ", ".join(arg_list)

        if needs_lazy:
            self._add_code(f"{var_name} = _custom_node_{node.node_id}.process({input_args}).lazy()")
        else:
            self._add_code(f"{var_name} = _custom_node_{node.node_id}.process({input_args})")
        self._add_code("")
