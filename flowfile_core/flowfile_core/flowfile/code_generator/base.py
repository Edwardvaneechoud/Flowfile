"""Shared converter surface for the code-generator handler mixins.

The node-type handlers are split across mixins (joins, transforms, connectors,
custom nodes, expressions) that all run as part of the composed
``FlowGraphCodeConverter``. Each handler reads converter state and calls a few
shared primitives that the composed class provides. This base only *declares*
that surface (under ``TYPE_CHECKING``) so cross-class ``self.*`` references in the
mixins resolve for static analysis; the real state and methods live on
``FlowGraphCodeConverter`` and win at runtime via the MRO.
"""

import json
import re
import typing

# Stdlib modules a polars_expr_transformer-generated expression may reference bare.
_EXPR_STDLIB_MODULES = ("datetime", "hashlib")


class ConverterMixinBase:
    """Type-only declaration of the converter surface shared by the handler mixins."""

    @staticmethod
    def _py_str(value: str) -> str:
        """Render ``value`` as a valid double-quoted Python string literal.

        Uses ``json.dumps`` so normal names stay byte-identical (``"name"``) while
        embedded quotes, backslashes, and newlines are escaped, keeping the
        emitted source valid Python.
        """
        return json.dumps(value, ensure_ascii=False)

    def _register_expr_stdlib_imports(self, code: str) -> None:
        """Import the stdlib modules a generated expression references.

        polars_expr_transformer emits bare module references — ``hashlib.md5(...)``
        inside a ``map_elements`` lambda for the hashing functions, and
        ``datetime.datetime.now()`` for ``now()``/``today()``. Neither is bound in
        the exported script unless it imports the module, so detect on the
        generated string rather than tracking which formula functions emit what.
        """
        for module in _EXPR_STDLIB_MODULES:
            if re.search(rf"\b{module}\.", code):
                self.imports.add(f"import {module}")

    @staticmethod
    def _py_path(value) -> str:
        # str() first: abs_file_path is str | None, and json.dumps(None) would emit `null`.
        return json.dumps(str(value), ensure_ascii=False)

    if typing.TYPE_CHECKING:
        framework: str
        imports: set[str]
        custom_node_classes: dict[str, str]
        unsupported_nodes: list[tuple[int, str, str]]

        def _add_code(self, line: str) -> None: ...

        def _add_comment(self, comment: str) -> None: ...

        def _get_agg_function(self, agg: str) -> str: ...
