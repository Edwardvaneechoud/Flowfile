"""Route-level forwarding to the hub, for a server that joined it with keys of its own.

There every secret and every stored connection lives on the hub: the routes that keep them forward to the hub's own
with the person's delegated token (``hub_client.secrets``, ``hub_client.connections``) and answer what the hub
answers, its refusal with the hub's own sentence.
"""

from fastapi import HTTPException

from flowfile_core.auth import issuer_accounts
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused

PASSED_ON = (400, 404, 409, 422)


def on_hub() -> bool:
    """Whether this server joined its hub with keys of its own."""
    return issuer_accounts.joined_with_keys() is not None


def through_hub(call, *args, **kwargs):
    """``call(*args, **kwargs)`` against the hub; its refusal as this route's answer, with the hub's own sentence."""
    try:
        return call(*args, **kwargs)
    except LookupError as e:
        raise HTTPException(status_code=401, detail=str(e)) from None
    except HubRefused as e:
        raise HTTPException(status_code=e.status if e.status in PASSED_ON else 502, detail=e.detail) from None
    except (HubUnreachable, HubVersionRefused) as e:
        raise HTTPException(status_code=502, detail=str(e)) from None
