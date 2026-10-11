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
