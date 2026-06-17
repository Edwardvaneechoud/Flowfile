"""HTTP protocol constants shared by the catalog visualization endpoints.

These are the deadlines the core's HTTP client (when calling the worker) and
the worker's per-request budget (waiting on a spawned child) agree on. The
request budget intentionally sits below the HTTP timeout so the worker
returns a typed timeout error before the client connection times out.
"""

HTTP_TIMEOUT_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = HTTP_TIMEOUT_SECONDS - 5
