"""``POST /hub/dispatch`` and ``/hub/dispatch/cancel``: the hub runs or cancels one version of a hub flow here, with its
``dispatch`` token; only on a server joined with keys of its own (404 elsewhere)."""

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
