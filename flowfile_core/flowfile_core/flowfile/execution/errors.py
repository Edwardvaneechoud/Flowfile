import logging

logger = logging.getLogger(__name__)

_ADVICE = "Start it, or check the Backends page."
_HUB_ADVICE = "Start it, or check its address on the hub's Compute page."


class ComputeError(Exception):
    """Base class for every failure while talking to compute."""


class ComputeUnavailable(ComputeError):
    """Compute cannot be reached.

    ``backend_id`` names the unreachable backend when the code raising the error knows it, else ``None``; ``url`` the
    address that did not answer, when the error is a transport's failure to reach it and is raised from that failure.
    """

    def __init__(self, message: str, *, backend_id: str | None = None, url: str | None = None) -> None:
        super().__init__(message)
        self.backend_id = backend_id
        self.url = url
        if url is not None and _hub_server(backend_id) is not None:
            logger.warning(message)

    def __str__(self) -> str:
        """The message, plus a hint to start the backend when the connection was refused (POSIX or Windows wording).

        A server filled from a hub (``registry.hub_server``) is named by its name on the hub, and the hint, given for
        any failure to reach it, points at its address on the hub's Compute page, where the hub keeps it. When such a
        server did not answer at its ``url``, the sentence says that and why in a few words (``_reason``) in place of
        the transport's text, which is logged as the error is raised. The hint is added once, so an error rebuilt from
        a run report row's text keeps the one it has.
        """
        message = super().__str__()
        if message.endswith((_ADVICE, _HUB_ADVICE)):
            return message
        server = _hub_server(self.backend_id)
        if server is not None:
            if self.url is not None and self.__cause__ is not None:
                reason = _reason(self.__cause__)
                return f'The hub\'s server "{server}" did not answer at {self.url} ({reason}). {_HUB_ADVICE}'
            return f'{message}. Is the hub\'s server "{server}" running? {_HUB_ADVICE}'
        if "refused" not in message.lower():
            return message
        if self.backend_id is None:
            return f"{message}. Is the compute backend running? {_ADVICE}"
        return f'{message}. Is backend "{self.backend_id}" running? {_ADVICE}'


def _hub_server(backend_id: str | None) -> str | None:
    from flowfile_core.flowfile.execution import registry  # imported here: the registry imports this module

    return registry.hub_server(backend_id)


def _reason(failure: BaseException) -> str:
    """Why a transport failed to reach a worker, in a few words: "connection refused" (POSIX or Windows wording),
    "timed out", else the failure's class name."""
    text = str(failure).lower()
    if "refused" in text:
        return "connection refused"
    if "timed out" in text or "timeout" in text:
        return "timed out"
    return type(failure).__name__


class VersionMismatch(ComputeError):
    """The compute runs other versions than this process."""


class ResultMissing(ComputeError):
    """A result the compute no longer has."""


class PlanRejected(ComputeError):
    """A plan the compute could not load."""


class OperationUnsupported(ComputeError):
    """An operation the compute does not implement."""


class ForeignRef(ComputeError):
    """A result reference minted by another compute."""


class TaskFailed(ComputeError):
    """A job that ran and failed."""


def worker_failure(error_kind: str | None, description: str) -> ComputeError:
    """Typed error for a worker task the fetcher reports with error code -1, by the worker's ``error_kind``.

    - ``"environment"``: :class:`PlanRejected`. The worker could not load the plan or write the result, or killed
      the task at its time limit.
    - ``"connection"``: :class:`ComputeUnavailable`. The socket to the worker closed mid-task with no cancel in flight.
    - otherwise: :class:`TaskFailed`. The child was killed, or core failed to read the result.

    Codes 1, 2 and -2 do not come here. The message is kept verbatim.
    """
    if error_kind == "connection":
        return ComputeUnavailable(description)
    if error_kind == "environment":
        return PlanRejected(description)
    return TaskFailed(description)
