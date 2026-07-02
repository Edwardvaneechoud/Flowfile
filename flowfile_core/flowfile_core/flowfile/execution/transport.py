"""HTTP/WebSocket transport to the flowfile_worker service.

Single owner of worker URLs and request plumbing. Fetchers and trigger
functions talk to a ``WorkerTransport`` instead of formatting ``WORKER_URL``
themselves, so an alternative worker (or a future pool) can be injected.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import requests

from flowfile_core.flowfile.execution.exceptions import WorkerConnectionError, WorkerTaskError

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import Status


class WorkerTransport:
    """Client for one worker service, addressed by base URL.

    ``base_url`` may be a string, a zero-arg callable (resolved per request),
    or ``None`` to follow ``settings.WORKER_URL`` (which already accounts for
    single-file mode's ``/worker`` prefix).
    """

    def __init__(self, base_url: str | Callable[[], str] | None = None):
        self._base_url = base_url

    @property
    def base_url(self) -> str:
        if callable(self._base_url):
            return self._base_url()
        if self._base_url is not None:
            return self._base_url
        from flowfile_core.configs.settings import WORKER_URL

        return WORKER_URL

    @property
    def ws_base_url(self) -> str:
        return self.base_url.replace("http://", "ws://").replace("https://", "wss://")

    # -- raw verbs -----------------------------------------------------------

    def post(self, path: str, **kwargs: Any) -> requests.Response:
        return self._request("post", path, **kwargs)

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        return self._request("get", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> requests.Response:
        return self._request("delete", path, **kwargs)

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        url = f"{self.base_url}{path}"
        try:
            return requests.request(method, url, **kwargs)
        except requests.exceptions.ConnectionError as e:
            raise WorkerConnectionError(f"Could not connect to the worker at {url}: {e}") from e

    # -- task lifecycle --------------------------------------------------------

    def get_status(self, task_id: str, timeout: float | None = None) -> Status:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import Status

        response = self.get(f"/status/{task_id}", timeout=timeout)
        if response.status_code != 200:
            raise WorkerTaskError(f"Could not fetch the status, {response.text}")
        return Status(**response.json())

    def results_exist(self, task_id: str) -> bool:
        try:
            response = self.get(f"/status/{task_id}")
            return response.status_code == 200 and response.json()["status"] == "Completed"
        except requests.RequestException:
            return False

    def cancel_task(self, task_id: str) -> bool:
        try:
            return self.post(f"/cancel_task/{task_id}").ok
        except requests.RequestException as e:
            raise WorkerTaskError(f"Failed to cancel task: {e}") from e

    def clear_task(self, task_id: str) -> bool:
        try:
            return self.delete(f"/clear_task/{task_id}").status_code == 200
        except requests.RequestException:
            return False

    # -- WebSocket streaming ---------------------------------------------------

    def streaming_submit(
        self,
        task_id: str,
        operation_type: str,
        flow_id: int,
        node_id: int | str,
        lf_bytes: bytes,
        kwargs: dict | None = None,
    ) -> tuple[Any, Status]:
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations.streaming import streaming_submit

        return streaming_submit(
            task_id=task_id,
            operation_type=operation_type,
            flow_id=flow_id,
            node_id=node_id,
            lf_bytes=lf_bytes,
            kwargs=kwargs,
            ws_base_url=self.ws_base_url,
        )

    def streaming_start(
        self,
        task_id: str,
        operation_type: str,
        flow_id: int,
        node_id: int | str,
        lf_bytes: bytes,
        kwargs: dict | None = None,
    ):
        from flowfile_core.flowfile.flow_data_engine.subprocess_operations.streaming import streaming_start

        return streaming_start(
            task_id=task_id,
            operation_type=operation_type,
            flow_id=flow_id,
            node_id=node_id,
            lf_bytes=lf_bytes,
            kwargs=kwargs,
            ws_base_url=self.ws_base_url,
        )


_default_transport: WorkerTransport | None = None


def get_default_transport() -> WorkerTransport:
    """The process-wide transport pointing at ``settings.WORKER_URL``."""
    global _default_transport
    if _default_transport is None:
        _default_transport = WorkerTransport()
    return _default_transport
