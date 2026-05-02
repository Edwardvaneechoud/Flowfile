"""Tool executor with prospective schema validation — owned by W31.

Per plan §6.3, ``execute_tool_call`` will:

1. resolve the target node-type / op;
2. validate args via the Pydantic settings class;
3. resolve the upstream schema and reject column references not in it;
4. for code-bearing nodes, dry-run on one sample row via ``kernel_runtime``
   to obtain the prospective schema (D003);
5. stage in the pending ``GraphDiff`` for Level 3, or apply immediately for
   Levels 1 / 2;
6. return success + predicted output schema so the next tool call can
   chain.

D011 (open) covers the degraded-mode rule when the upstream node's
``predicted_schema`` is ``None``. W31 cannot start until D011 is decided.

Stub until W31.
"""
