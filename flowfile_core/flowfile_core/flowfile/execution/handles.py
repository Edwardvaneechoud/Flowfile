"""Task handles: a uniform view over in-process and worker-backed results."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import Status


@runtime_checkable
class TaskHandle(Protocol):
    """What node execution needs from a submitted compute task.

    ``BaseFetcher`` (worker-backed) satisfies this protocol;
    ``LocalResultHandle`` is the in-process equivalent.
    """

    file_ref: str
    status: Status | None

    def get_result(self) -> Any: ...

    def cancel(self) -> None: ...

    @property
    def error_code(self) -> int: ...

    @property
    def error_description(self) -> str | None: ...


class LocalResultHandle:
    """Immediately-available in-process result satisfying ``TaskHandle``."""

    def __init__(self, result: Any, file_ref: str = "", status: Status | None = None):
        self._result = result
        self.file_ref = file_ref
        self.status = status

    def get_result(self) -> Any:
        return self._result

    @property
    def result(self) -> Any:
        return self._result

    def cancel(self) -> None:
        return None

    @property
    def error_code(self) -> int:
        return 0

    @property
    def error_description(self) -> str | None:
        return None
