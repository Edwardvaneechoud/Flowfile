"""MkDocs build hooks for the Flowfile docs.

Registered via ``hooks:`` in mkdocs.yml. Currently regenerates the formula
function reference from the live ``polars_expr_transformer`` engine before each
build so it never drifts from the package.

Defensive by design: if the engine can't be imported (e.g. a docs-only CI image
without the package), it logs a warning and falls back to the committed copy of
``docs/users/python-api/reference/formula-functions.md`` instead of failing.
"""

import logging
import pathlib
import sys

log = logging.getLogger("mkdocs.hooks.formula_reference")


def on_pre_build(config, **kwargs):
    try:
        sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
        import gen_formula_reference

        gen_formula_reference.main()
        log.info("Regenerated formula function reference from polars_expr_transformer.")
    except Exception as exc:  # noqa: BLE001 - never fail the docs build over this
        log.warning(
            "Could not regenerate formula function reference (%s); "
            "using the committed copy.",
            exc,
        )
