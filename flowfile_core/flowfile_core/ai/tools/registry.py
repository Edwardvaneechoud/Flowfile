"""Tool catalog generation — owned by W30.

Per D002, the catalog is *not* surfaced flat. Two surfaces:

* ``build_tool_catalog(surface: str)`` — per-surface preset (e.g.
  ``transformations`` returns ~5 tools for ``Cmd+K``); used wherever the
  context narrows the LLM's options.
* ``pick_category(intent: str)`` then ``build_tool_catalog(category)`` —
  two-stage path used by the full Level 3 agent so the model sees only the
  relevant bucket on the second call.

Per D004, names are MCP-compatible from day one
(``flowfile.graph.add_filter`` rather than ``add_filter_node``) and tool
specs use a JSON-Schema dialect compatible with an MCP server shim.

Source of truth for node settings: ``get_settings_class_for_node_type()``
at ``flowfile_core/flowfile_core/schemas/schemas.py`` — this resolves
user-defined nodes too, so the catalog inherits UDF support for free.

Stub until W30.
"""
