"""Open node-request issues, proxied from GitHub for the in-app "Request a node" dialog."""

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.flowfile import node_requests

router = APIRouter(dependencies=[Depends(get_current_active_user)])


class OpenNodeRequests(BaseModel):
    requests: list[node_requests.NodeRequest]


@router.get("", response_model=OpenNodeRequests)
def open_node_requests() -> OpenNodeRequests:
    """Every open request, oldest first; empty (never an error) when GitHub cannot be reached."""
    return OpenNodeRequests(requests=node_requests.open_requests())
