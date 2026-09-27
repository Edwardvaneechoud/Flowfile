"""The user id the frame acts as: the notebook session's, else ``FLOWFILE_SESSION_USER_ID``, else 1."""

import os

from flowfile_frame.notebook import current


def current_user_id() -> int:
    """The notebook mode's ``user_id``, else ``int(FLOWFILE_SESSION_USER_ID)``, else 1 (single-user mode)."""
    mode = current()
    if mode is not None and mode.user_id is not None:
        return mode.user_id
    env_user = os.environ.get("FLOWFILE_SESSION_USER_ID")
    if env_user:
        return int(env_user)
    return 1
