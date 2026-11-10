"""A person's sign-in on a server joined with keys, from a token the hub signed for them.

Two tokens put a person on such a server: the one-time ``signin`` token the hub's Open page gives their browser
(``POST /auth/hub/signin``), and a ``dispatch`` token the hub sends with a run of theirs (``POST /hub/dispatch``). Both
are traded at the hub for a delegation signed with this node's own key, and the person's local account is the one mapped
to the hub's issuer and their id there, made at their first sign-in. ``sign_in`` is that trade; what each route answers
afterwards is its own.
"""

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
    """A sign-in made: the person's account here, the ``sid`` core's own tokens for it carry, and how long the hub's
    access token lives (seconds)."""

    user: db_models.User
    sid: str
    expires_in: int


def _sentence(text: str) -> str:
    return text.rstrip(".") + "."


def account(db: Session, user_id: int) -> db_models.User | None:
    """The local account of the person ``user_id`` is on the hub, ``None`` before their first sign-in here."""
    return issuer_accounts.issuer_account(db, hub_node.resolved().issuer, user_id)


def live(user_id: int) -> bool:
    """Whether ``user_id`` has a sign-in through the hub on this server that still gives an access token."""
    stored = hub_session.load(user_id)
    if stored is None or stored.get("kind") != "delegation":
        return False
    try:
        return hub_session.access_token(user_id) is not None
    except (HubRefused, HubUnreachable, LookupError):
        return False


def renew(user_id: int, sid: str | None) -> Delegation | None:
    """What the hub answers when the sign-in ``sid`` of ``user_id`` is refreshed there now; ``None`` when the hub no
    longer takes it or it is not the one kept here. ``HubUnreachable`` for a hub that does not answer."""
    try:
        return hub_session.refresh_delegation(user_id, sid)
    except HubRefused:
        return None


def sign_out(user_id: int) -> None:
    """End the sign-in through the hub of ``user_id``: revoked at the hub as far as it answers, and forgotten here.
    Any other hub connection of the account is left as it is."""
    stored = hub_session.load(user_id)
    if stored is None or stored.get("kind") != "delegation":
        return
    try:
        delegation.revoke(stored["hub_url"], hub_node.resolved().issuer, stored["refresh_token"])
    except (HubRefused, HubUnreachable, LookupError) as e:
        logger.warning("The hub was not told of a sign-out on this server: %s", e)
    hub_session.clear(user_id)


def sign_in(db: Session, token: str, user_id: int) -> SignedIn:
    """Trade ``token``, a hub token naming the person ``user_id`` on the hub, for their sign-in here.

    The hub's exchange signs the delegation; the account is the one ``issuer_signin.admit`` answers for the hub's issuer
    and the person the hub described; the delegation is kept as the person's hub session, never in the database; and the
    hub's servers this person may run on become backend rows with it (``node_addresses.sync_for``), a hub whose list
    cannot be read leaving one warning. ``SignInRefused`` for a disabled account, a trade the hub refuses or a hub that
    answers for another person; ``HubUnreachable`` for a hub that does not answer.
    """
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
