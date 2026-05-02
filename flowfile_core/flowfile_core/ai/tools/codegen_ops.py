"""Codegen tool surface — owned by W30.

Tools for generating ``polars_code`` / ``python_script`` / ``sql_query``
node bodies. Per D003, code-bearing proposals run a 1-row dry-run via
``kernel_runtime`` so the prospective output schema is known before the
GraphDiff stages — that work lives in ``executor.py`` (W31).

Reverse path (graph → Python) reuses the existing
``get_generated_flowframe_code()`` (``routes.py:746``,
``code_generator.py:2368``) and is **not** owned here — it's a UI surface.

Stub until W30.
"""
