"""Worker-friendly data-science utilities (estimator dispatch, predict, IO).

This package is shared between the core service (for schema callbacks and
add-time validation) and the worker service (for the actual fit/predict
execution). It must NOT depend on FastAPI, Pydantic models from `core`, or
any service that is not part of the worker runtime.
"""
