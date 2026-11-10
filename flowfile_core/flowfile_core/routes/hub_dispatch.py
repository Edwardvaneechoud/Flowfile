"""``POST /hub/dispatch``: the hub has this server run one version of a hub flow as a person.

Only a server joined to its hub with keys of its own takes it (404 elsewhere), and only with the hub's ``dispatch``
token for this server (``shared.hub_tokens.verify_dispatch``: signature, issuer, audience, expiry, scope), 401 with the
reason otherwise. A run id this process took already is 409. The route answers 202 with the run id at once; the run
itself is ``hub_client.dispatch.run``, in a thread of its own, reported to the hub when it ends.

``POST /hub/dispatch/cancel`` stops such a run: the same token check, a ``dispatch`` token of the hub's naming the run
id, and ``hub_client.dispatch.cancel``. It answers 200 with the run id and ``state``: ``cancelled``, ``finished`` for a
run that had ended already, ``unknown`` for a run this server neither runs nor has a record of.
"""

from fastapi import APIRouter, Body, HTTPException, status
from fastapi.responses import JSONResponse

from flowfile_core.auth import issuer_accounts
from flowfile_core.hub_client import dispatch
from shared import hub_tokens

router = APIRouter()


@router.post("/dispatch", status_code=status.HTTP_202_ACCEPTED, summary="Run a hub flow here, sent by the hub")
def take_dispatch(token: str = Body(..., embed=True)) -> JSONResponse:
    claims = _claims(token)
    if dispatch.taken(claims):
        raise HTTPException(status.HTTP_409_CONFLICT, f"Run {claims.run_id} was dispatched here already.")
    dispatch.start(claims, token)
    return JSONResponse({"run_id": claims.run_id}, status_code=status.HTTP_202_ACCEPTED)


@router.post("/dispatch/cancel", summary="Cancel a run the hub sent here")
def cancel_dispatch(token: str = Body(..., embed=True)) -> dict:
    claims = _claims(token)
    return {"run_id": claims.run_id, "state": dispatch.cancel(claims)}


def _claims(token: str) -> hub_tokens.DispatchClaims:
    """The claims of the hub's ``dispatch`` token for this server, or the refusal: 404 off a joined server, 401."""
    if issuer_accounts.joined_with_keys() is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "This server is not joined to a hub with keys of its own.")
    try:
        claims = hub_tokens.verify_dispatch(token)
    except hub_tokens.HubTokenRefused as e:
        detail = f"The hub's dispatch token was refused: {e.reason}."
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail) from None
    if claims is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "This is not a dispatch token from the hub.")
    return claims
