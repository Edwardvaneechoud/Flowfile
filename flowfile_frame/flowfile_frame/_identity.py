"""The user id the frame acts as: the active notebook mode's, else 1 (a single-user script)."""

from flowfile_frame.notebook import current


def current_user_id() -> int:
    """The ``user_id`` of the notebook mode active in this context, else 1 (single-user mode).

    Nothing else sets it: the mode is context-local, so a thread or request that did not enter
    the mode acts as a script, and the process environment plays no part.
    """
    mode = current()
    if mode is not None and mode.user_id is not None:
        return mode.user_id
    return 1
