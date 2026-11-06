"""``POST /hub/dispatch``: the hub has this server run one version of a hub flow as a person.

Only a server joined to its hub with keys of its own takes it (404 elsewhere), and only with the hub's ``dispatch``
token for this server (``shared.hub_tokens.verify_dispatch``: signature, issuer, audience, expiry, scope), 401 with the
reason otherwise. A run id this process took already is 409. The route answers 202 with the run id at once; the run
itself is ``hub_client.dispatch.run``, in a thread of its own, reported to the hub when it ends.
"""

from fastapi import APIRouter, Body, HTTPException, status
from fastapi.responses import JSONResponse

from flowfile_core.auth import issuer_accounts
from flowfile_core.hub_client import dispatch
from shared import hub_tokens

router = APIRouter()


@router.post("/dispatch", status_code=status.HTTP_202_ACCEPTED, summary="Run a hub flow here, sent by the hub")
def take_dispatch(token: str = Body(..., embed=True)) -> JSONResponse:
    if issuer_accounts.joined_with_keys() is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "This server is not joined to a hub with keys of its own.")
    try:
        claims = hub_tokens.verify_dispatch(token)
    except hub_tokens.HubTokenRefused as e:
        detail = f"The hub's dispatch token was refused: {e.reason}."
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail) from None
    if claims is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "This is not a dispatch token from the hub.")
    if dispatch.taken(claims):
        raise HTTPException(status.HTTP_409_CONFLICT, f"Run {claims.run_id} was dispatched here already.")
    dispatch.start(claims, token)
    return JSONResponse({"run_id": claims.run_id}, status_code=status.HTTP_202_ACCEPTED)
