"""A person's sign-in on a server joined with keys, from a ``signin`` or ``dispatch`` token the hub signed for them,
traded at the hub for a delegation; their local account is the one mapped to the hub's issuer and their hub id."""

from dataclasses import dataclass

from sqlalchemy.orm import Session

from flowfile_core.auth import issuer_accounts, issuer_signin
from flowfile_core.auth.issuer_signin import SignInRefused
from flowfile_core.configs import logger
from flowfile_core.database import models as db_models
from flowfile_core.hub_client import delegation, node_addresses
from flowfile_core.hub_client import session as hub_session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable
from flowfile_core.hub_client.delegation import Delegation
from shared import hub_node


@dataclass(frozen=True)
class SignedIn:
    """A sign-in made; ``expires_in`` is the hub access token's lifetime in seconds."""

    user: db_models.User
    sid: str
    expires_in: int


def _sentence(text: str) -> str:
    return text.rstrip(".") + "."


def account(db: Session, user_id: int) -> db_models.User | None:
    return issuer_accounts.issuer_account(db, hub_node.resolved().issuer, user_id)


def live(user_id: int) -> bool:
    stored = hub_session.load(user_id)
    if stored is None or stored.get("kind") != "delegation":
        return False
    try:
        return hub_session.access_token(user_id) is not None
    except (HubRefused, HubUnreachable, LookupError):
        return False


def renew(user_id: int, sid: str | None) -> Delegation | None:
    """The hub's refresh of the sign-in ``sid`` now; ``None`` when it is not taken or not the one kept here."""
    try:
        return hub_session.refresh_delegation(user_id, sid)
    except HubRefused:
        return None


def sign_out(user_id: int) -> None:
    """Revoke the sign-in through the hub as far as the hub answers, and forget it here."""
    stored = hub_session.load(user_id)
    if stored is None or stored.get("kind") != "delegation":
        return
    try:
        delegation.revoke(stored["hub_url"], hub_node.resolved().issuer, stored["refresh_token"])
    except (HubRefused, HubUnreachable, LookupError) as e:
        logger.warning("The hub was not told of a sign-out on this server: %s", e)
    hub_session.clear(user_id)


def sign_in(db: Session, token: str, user_id: int) -> SignedIn:
    """Trade ``token``, naming the hub's person ``user_id``, for their sign-in here; the delegation is kept as their
    hub session, never in the database. ``SignInRefused`` when the hub refuses or answers for another person."""
    settings = hub_node.resolved()
    issuer_signin.refuse_disabled(db, settings.issuer, user_id)
    try:
        granted = delegation.exchange(settings.hub_url, settings.issuer, token)
    except HubRefused as e:
        raise SignInRefused(_sentence(f"The hub refused this sign-in: {e.detail}")) from None
    except LookupError as e:
        logger.warning("A sign-in through the hub could not be traded: %s", e)
        raise SignInRefused("This server cannot sign for itself at its hub.") from None
    if granted.user["uid"] != user_id:
        raise SignInRefused("The hub answered this sign-in for another person.")
    user = issuer_signin.admit(db, settings.issuer, settings.hub_url, granted.user)
    sid = hub_session.save_delegation(user.id, settings.hub_url, granted)
    node_addresses.sync_for(user.id)
    return SignedIn(user=user, sid=sid, expires_in=granted.expires_in)
