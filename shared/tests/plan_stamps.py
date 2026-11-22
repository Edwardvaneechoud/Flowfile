"""The Python version Polars stamps on each Python object in a serialised plan, read and rewritten for tests.

Polars serialises a Python object in a plan as one flag byte (``1`` cloudpickle, ``0`` plain pickle), the Python minor
and micro version, then the pickle, whose first byte is the protocol opcode ``0x80``. The binary plan writes those bytes
either raw or as MessagePack integers, where ``0x80`` reads ``0xcc 0x80``. Polars refuses a cloudpickled object stamped
with another version than its own, and reads a plain pickle under any; rewriting the stamp is how a test sends a plan
as a process on another Python patch version would.
"""

import sys

CLOUDPICKLE, PICKLE = 1, 0
_OPCODES = (b"\x80", b"\xcc\x80")


def _stamp(flag: int, micro: int) -> list[bytes]:
    return [bytes([flag, sys.version_info.minor, micro]) + opcode for opcode in _OPCODES]


def python_objects(plan: bytes, micro: int | None = None) -> list[int]:
    """The flag of each Python object in ``plan`` stamped with Python ``3.<minor>.<micro>``, this process's by default,
    in no particular order."""
    micro = sys.version_info.micro if micro is None else micro
    return [flag for flag in (CLOUDPICKLE, PICKLE) for stamp in _stamp(flag, micro) for _ in range(plan.count(stamp))]


def restamped(plan: bytes, micro: int) -> bytes:
    """``plan`` as a process on Python ``3.<minor>.<micro>`` would have serialised it."""
    for flag in (CLOUDPICKLE, PICKLE):
        for ours, theirs in zip(_stamp(flag, sys.version_info.micro), _stamp(flag, micro), strict=True):
            plan = plan.replace(ours, theirs)
    return plan


def other_micro() -> int:
    """A Python patch version that is not this process's."""
    return sys.version_info.micro + 5
