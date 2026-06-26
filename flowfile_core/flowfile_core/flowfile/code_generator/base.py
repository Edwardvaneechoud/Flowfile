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
import typing


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

    if typing.TYPE_CHECKING:
        framework: str
        imports: set[str]
        custom_node_classes: dict[str, str]
        unsupported_nodes: list[tuple[int, str, str]]

        def _add_code(self, line: str) -> None: ...

        def _add_comment(self, comment: str) -> None: ...

        def _get_agg_function(self, agg: str) -> str: ...
