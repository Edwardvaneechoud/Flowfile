"""Whether this machine can reach each of the hub's compute nodes, checked once after a sync and kept in memory.

A node's worker URL is the hub's address for it, which may be a hostname only the team's network resolves; without a
check the user learns that from a failed run. The check is the one request the handshake makes first, the node's
capabilities signed with the node's token (``WorkerBackend.capabilities``). The handshake proper, with its probe plans
and its verdict, stays with the Handshake button and the first run: these two fields say what this computer can
reach, the verdict what the worker offers. A node is reachable when its worker answered at all, so a refusal (another
version, a token it does not accept) is reachable with the refusal's first line as the reason.

The outcomes are one dict in this process; nothing is written to the row or to disk, and the previous outcome stands
until a new check answers. A sync queues ``check_all`` in the background for the rows it wrote; ``POST
/hub_client/compute_nodes/check`` runs it in the request.
"""

import threading
from collections.abc import Iterable

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.errors import ComputeError, ComputeUnavailable

UNREACHABLE = "can't reach {url} from this computer: {reason}"
"""The reason of an unreachable node, as an administrator or a desktop user reads it."""
UNREACHABLE_WITHOUT_ADDRESS = "can't reach this node from this computer"
"""The reason of an unreachable node as a non-administrator reads it: the listing hides a worker's URL from them."""

_outcomes: dict[str, tuple[bool | None, str | None]] = {}
_lock = threading.Lock()


def _first_line(error: BaseException) -> str:
    text = str(error).strip()
    return text.splitlines()[0] if text else type(error).__name__


def check(backend_id: str) -> tuple[bool | None, str | None]:
    """Ask the worker of ``backend_id`` for its capabilities now and keep the outcome; ``(reachable, reason)``.

    ``True`` with no reason when the worker answered its capabilities, ``True`` with the refusal's first line when it
    answered a refusal that is not ``ComputeUnavailable``, ``False`` with ``UNREACHABLE`` filled when it could not be
    reached, and ``None`` with the ``LookupError``'s text when no backend could be made of the row, which for a
    hub-filled row means no token could be minted (no hub session, or the hub refused one); no worker is called then.
    """
    try:
        backend = registry.backend(backend_id)
    except LookupError as e:
        outcome: tuple[bool | None, str | None] = (None, str(e))
    else:
        try:
            backend.capabilities()
        except ComputeUnavailable as e:
            url = getattr(backend, "url", backend_id)
            outcome = (False, UNREACHABLE.format(url=url, reason=_first_line(e)))
        except ComputeError as e:
            outcome = (True, _first_line(e))
        else:
            outcome = (True, None)
    with _lock:
        _outcomes[backend_id] = outcome
    return outcome


def check_all(backend_ids: Iterable[str]) -> None:
    """``check`` each of ``backend_ids``, one after the other."""
    for backend_id in backend_ids:
        check(backend_id)


def of(backend_id: str) -> tuple[bool | None, str | None]:
    """The outcome kept for ``backend_id``; ``(None, None)`` until it was checked."""
    with _lock:
        return _outcomes.get(backend_id, (None, None))


def forget(backend_id: str) -> None:
    """Drop the outcome kept for ``backend_id``, as its row goes."""
    with _lock:
        _outcomes.pop(backend_id, None)


def clear() -> None:
    """Drop every outcome, for tests."""
    with _lock:
        _outcomes.clear()
