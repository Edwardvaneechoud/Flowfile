"""A person's sign-in on a server joined with keys, from a token the hub signed for them.

Two tokens put a person on such a server: the one-time ``signin`` token the hub's Open page gives their browser
(``POST /auth/hub/signin``), and a ``dispatch`` token the hub sends with a run of theirs (``POST /hub/dispatch``). Both
are traded at the hub for a delegation signed with this node's own key, and the person's local account is the one mapped
to the hub's issuer and their id there, made at their first sign-in. ``sign_in`` is that trade; what each route answers
afterwards is its own.
"""

from dataclasses import dataclass

from sqlalchemy.orm import Session

from flowfile_core.auth import issuer_accounts
from flowfile_core.auth.jwt import invalidate_user_cache
from flowfile_core.configs import logger
from flowfile_core.database import models as db_models
from flowfile_core.hub_client import delegation, node_addresses
from flowfile_core.hub_client import session as hub_session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable
from shared import hub_node


class SignInRefused(Exception):
    """A sign-in this server will not make; the message is one sentence fit to answer."""


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


def sign_in(db: Session, token: str, user_id: int) -> SignedIn:
    """Trade ``token``, a hub token naming the person ``user_id`` on the hub, for their sign-in here.

    The hub's exchange signs the delegation; the account is made at a first sign-in and administers this server as
    ``node_admin`` says at every one; the delegation is kept as the person's hub session, never in the database; and the
    hub's servers this person may run on become backend rows with it (``node_addresses.sync_for``), a hub whose list
    cannot be read leaving one warning. ``SignInRefused`` for a disabled account, a trade the hub refuses or a hub that
    answers for another person; ``HubUnreachable`` for a hub that does not answer.
    """
    settings = hub_node.resolved()
    user = account(db, user_id)
    if user is not None and user.disabled:
        raise SignInRefused("This account is disabled on this server.")
    try:
        granted = delegation.exchange(settings.hub_url, settings.issuer, token)
    except HubRefused as e:
        raise SignInRefused(_sentence(f"The hub refused this sign-in: {e.detail}")) from None
    except LookupError as e:
        logger.warning("A sign-in through the hub could not be traded: %s", e)
        raise SignInRefused("This server cannot sign for itself at its hub.") from None
    if granted.user["uid"] != user_id:
        raise SignInRefused("The hub answered this sign-in for another person.")
    if user is None:
        user = issuer_accounts.new_issuer_account(db, settings.issuer, settings.hub_url, granted.user)
    user.is_admin = granted.user["node_admin"]
    db.commit()
    invalidate_user_cache(user.username)
    sid = hub_session.save_delegation(user.id, settings.hub_url, granted)
    node_addresses.sync_for(user.id)
    return SignedIn(user=user, sid=sid, expires_in=granted.expires_in)
