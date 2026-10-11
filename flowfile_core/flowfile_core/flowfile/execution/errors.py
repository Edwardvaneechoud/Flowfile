class ComputeError(Exception):
    """Base class for every failure while talking to compute."""


class ComputeUnavailable(ComputeError):
    """Compute cannot be reached."""

    def __str__(self) -> str:
        """The message, plus what to do about it when the connection was refused."""
        message = super().__str__()
        if "connection refused" in message.lower():
            return f"{message}. Is the worker running? Start it, or switch the flow to local execution."
        return message


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
