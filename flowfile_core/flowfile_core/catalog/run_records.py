"""Where a run's record points when the run happened elsewhere: the log and the flow version behind it.

The catalog that owns a flow owns the record of each of its runs (a ``flow_runs`` row). A run executed by another
machine, a server a hub sent it to, gives its record ``executed_on`` (the backend's name), ``remote_run_id`` (the
run id the executor reported under), ``log_ref`` (``backend:<name>:<id>``, the executor's own run row whose log
route holds the log) and ``flow_version`` (the published version that ran, in place of a snapshot). A plain core
knows no other machine: ``backend_core_url`` answers ``None`` and ``version_document`` ``None``, so a record with
a ``log_ref`` answers that its log is kept elsewhere and a record with a version has no snapshot. A core that keeps
other machines' records (a hub) sets both at start-up, and ``backend_ui_url`` for the executors that serve an editor:
the log route takes the executor's own sign-in, so a person is sent to the run's page in that editor (``log_page``),
never to the route.
"""

from collections.abc import Callable

LOG_REF_PREFIX = "backend:"
LOG_ELSEWHERE = "The log of this run is kept by the server {backend}, which this core cannot reach"


def log_ref(backend: str, run_id: int) -> str:
    """The reference a record carries for a log kept by ``backend`` under its own run row ``run_id``."""
    return f"{LOG_REF_PREFIX}{backend}:{run_id}"


def parse_log_ref(ref: str) -> tuple[str, int] | None:
    """``(backend, run id)`` of a ``log_ref``; ``None`` for anything else."""
    if not ref.startswith(LOG_REF_PREFIX):
        return None
    backend, _, run_id = ref[len(LOG_REF_PREFIX) :].rpartition(":")
    return (backend, int(run_id)) if backend and run_id.isdigit() else None


def _no_core_url(backend: str) -> str | None:
    return None


def _no_ui_url(backend: str) -> str | None:
    return None


def _no_document(flow_uuid: str, version: int) -> str | None:
    return None


backend_core_url: Callable[[str], str | None] = _no_core_url
"""The base URL of the core that executes as ``backend``; set by an app that knows other machines."""
backend_ui_url: Callable[[str], str | None] = _no_ui_url
"""The editor ``backend`` serves, as a browser reaches it; set by an app that knows other machines."""
version_document: Callable[[str, int], str | None] = _no_document
"""The document of a flow's published version, as text; set by an app that keeps flow versions."""


def log_location(ref: str) -> str | None:
    """The log route of the executor a ``log_ref`` names, or ``None`` when this core cannot reach it."""
    parsed = parse_log_ref(ref)
    if parsed is None:
        return None
    backend, run_id = parsed
    url = backend_core_url(backend)
    return f"{url.rstrip('/')}/catalog/runs/{run_id}/log" if url else None


def log_page(ref: str) -> str | None:
    """The page of the executor's editor that shows the run a ``log_ref`` names, its log included, for a person to
    open; ``None`` when the executor serves no editor this core knows."""
    parsed = parse_log_ref(ref)
    if parsed is None:
        return None
    backend, run_id = parsed
    url = backend_ui_url(backend)
    return f"{url.rstrip('/')}/#/main/catalog?tab=runs&runId={run_id}" if url else None


def snapshot_of(run) -> str | None:
    """The document a run's record stands for: its own snapshot, else the published version it names."""
    if run.flow_snapshot is not None:
        return run.flow_snapshot
    if run.flow_version is not None and run.flow_uuid:
        return version_document(run.flow_uuid, run.flow_version)
    return None
