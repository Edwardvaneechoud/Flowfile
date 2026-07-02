"""Input-handle helpers for nodes with per-instance dynamic input handles.

Mirrors ``multi_output.py`` on the input side. Handle ``input-0`` is reserved
for the fixed parameter-data connection of dynamic-input nodes (run_flow);
``input-1``..``input-N`` map positionally onto the node's ``input_slots``.
"""

PARAM_INPUT_HANDLE = "input-0"
_INPUT_HANDLE_PREFIX = "input-"


def input_handle(index: int) -> str:
    """Return the handle id for input *index* (0 = the reserved parameter handle)."""
    return f"{_INPUT_HANDLE_PREFIX}{index}"


def input_handle_index(handle: str) -> int:
    """Return the numeric index of an ``input-N`` handle id.

    Raises ValueError for anything that is not a well-formed input handle.
    """
    if not handle.startswith(_INPUT_HANDLE_PREFIX):
        raise ValueError(f"Not an input handle: {handle!r}")
    try:
        index = int(handle[len(_INPUT_HANDLE_PREFIX) :])
    except ValueError as exc:
        raise ValueError(f"Not an input handle: {handle!r}") from exc
    if index < 0:
        raise ValueError(f"Not an input handle: {handle!r}")
    return index
