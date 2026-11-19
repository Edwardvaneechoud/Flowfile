"""Measure how well a model builds charts through ``POST /ai/generate_chart`` on a live core.

A developer tool, never part of a test suite or CI: it seeds three tables of made-up data
into the target core's catalog, sends the prompts of ``cases.json`` with the given provider
and model, scores each answer against the case's partial expectations and writes
``report.md`` and ``report.json``. Run it from the repo root::

    poetry run python -m tools.chart_ai_eval --api http://localhost:63598 \\
        --provider openai --model <model> --out <dir>

``scoring`` is pure (no I/O), ``tables`` builds the data, ``runner`` talks to the core.
"""
