_ADVICE = "Start it, or check the Backends page."


class ComputeError(Exception):
    """Base class for every failure while talking to compute."""


class ComputeUnavailable(ComputeError):
    """Compute cannot be reached.

    ``backend_id`` is the id of the backend that could not be reached, when the code that made the error knows it
    (``WorkerBackend`` and the handshake gate it hands its id to), else ``None``.
    """

    def __init__(self, message: str, *, backend_id: str | None = None) -> None:
        super().__init__(message)
        self.backend_id = backend_id

    def __str__(self) -> str:
        """The message, plus what to do about it when the connection was refused.

        A refused connection reads ``Connection refused`` on POSIX and ``the target machine actively refused it``
        on Windows (``WinError 10061``); both get the hint: ``Is backend "<backend_id>" running? Start it, or check
        the Backends page.``, or ``Is the compute backend running? …`` when the error has no ``backend_id``. The hint
        is added once: a message that already ends with the advice, as an error rebuilt from its own text (a run
        report row's) does, is read as it is, whichever backend it names.
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
    """Typed error for a worker task the fetcher reports with error code -1.

    The fetcher folds several failures into code -1; the worker's ``error_kind`` tells them apart:

    - ``"environment"``: the worker could not load the plan (bytes that do not deserialise, a UDF
      module or plugin it cannot import), could not write the result, or terminated the task at
      its time limit. The worker labels its own time-limit kill "environment" too, so a timeout
      surfaces as :class:`PlanRejected` whose message says the task exceeded the time limit.
    - ``"connection"``: the socket to the worker closed mid-task with no cancel in flight.
    - no kind: the child was killed ("Unknown Error" over REST, "Process ended unexpectedly" over
      the WebSocket), or core failed to read the result the worker produced.

    Codes 1, 2 and -2 are not routed through here. The message is kept verbatim.
    """
    if error_kind == "connection":
        return ComputeUnavailable(description)
    if error_kind == "environment":
        return PlanRejected(description)
    return TaskFailed(description)
