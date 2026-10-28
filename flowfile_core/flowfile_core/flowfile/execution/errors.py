_ADVICE = "Start it, or check the Backends page."


class ComputeError(Exception):
    """Base class for every failure while talking to compute."""


class ComputeUnavailable(ComputeError):
    """Compute cannot be reached.

    ``backend_id`` names the unreachable backend when the code raising the error knows it, else ``None``.
    """

    def __init__(self, message: str, *, backend_id: str | None = None) -> None:
        super().__init__(message)
        self.backend_id = backend_id

    def __str__(self) -> str:
        """The message, plus a hint to start the backend when the connection was refused (POSIX or Windows wording).

        The hint is added once, so an error rebuilt from a run report row's text keeps the one it has.
        """
        message = super().__str__()
        if "refused" not in message.lower() or message.endswith(_ADVICE):
            return message
        if self.backend_id is None:
            return f"{message}. Is the compute backend running? {_ADVICE}"
        return f'{message}. Is backend "{self.backend_id}" running? {_ADVICE}'


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
