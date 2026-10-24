"""The Polars Code sandbox, which lives in ``shared.polars_code_parser`` so the worker's child can run it too.

Re-exported here so the import path core has always used keeps working; ``polars_code_parser`` is the one shared
instance.
"""

from shared.polars_code_parser import (
    PolarsCodeParser,
    function_form,
    function_form_error,
    polars_code_parser,
    remove_comments_and_docstrings,
    unrunnable_def_error,
)

__all__ = [
    "PolarsCodeParser",
    "function_form",
    "function_form_error",
    "polars_code_parser",
    "remove_comments_and_docstrings",
    "unrunnable_def_error",
]
