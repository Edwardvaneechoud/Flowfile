"""Flow favourites and follows."""

from __future__ import annotations

import logging

from flowfile_core.catalog.exceptions import (
    FavoriteNotFoundError,
    FlowNotFoundError,
    FollowNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services.flows import FlowRegistrationService
from flowfile_core.database.models import FlowFavorite, FlowFollow, FlowRegistration
from flowfile_core.schemas.catalog_schema import FlowRegistrationOut

logger = logging.getLogger(__name__)


class FlowEngagementService:
    """Owns favourites and follows for flow registrations."""

    def __init__(self, repo: CatalogRepository, flows: FlowRegistrationService) -> None:
        self.repo = repo
        self._flows = flows

    def add_favorite(self, user_id: int, registration_id: int) -> FlowFavorite:
        """Add a flow to user's favourites (idempotent)."""
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        existing = self.repo.get_favorite(user_id, registration_id)
        if existing is not None:
            return existing
        favorite = FlowFavorite(user_id=user_id, registration_id=registration_id)
        return self.repo.add_favorite(favorite)

    def remove_favorite(self, user_id: int, registration_id: int) -> None:
        """Remove a flow from user's favourites."""
        existing = self.repo.get_favorite(user_id, registration_id)
        if existing is None:
            raise FavoriteNotFoundError(user_id=user_id, registration_id=registration_id)
        self.repo.remove_favorite(user_id, registration_id)

    def list_favorites(self, user_id: int) -> list[FlowRegistrationOut]:
        """List all flows the user has favourited, enriched (bulk)."""
        favorites = self.repo.list_favorites(user_id)
        flows: list[FlowRegistration] = []
        for favorite in favorites:
            flow = self.repo.get_flow(favorite.registration_id)
            if flow is not None:
                flows.append(flow)
        return self._flows.bulk_enrich_flows(flows, user_id)

    def add_follow(self, user_id: int, registration_id: int) -> FlowFollow:
        """Follow a flow (idempotent)."""
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        existing = self.repo.get_follow(user_id, registration_id)
        if existing is not None:
            return existing
        follow = FlowFollow(user_id=user_id, registration_id=registration_id)
        return self.repo.add_follow(follow)

    def remove_follow(self, user_id: int, registration_id: int) -> None:
        """Unfollow a flow."""
        existing = self.repo.get_follow(user_id, registration_id)
        if existing is None:
            raise FollowNotFoundError(user_id=user_id, registration_id=registration_id)
        self.repo.remove_follow(user_id, registration_id)

    def list_following(self, user_id: int) -> list[FlowRegistrationOut]:
        """List all flows the user is following, enriched (bulk)."""
        follows = self.repo.list_follows(user_id)
        flows: list[FlowRegistration] = []
        for follow in follows:
            flow = self.repo.get_flow(follow.registration_id)
            if flow is not None:
                flows.append(flow)
        return self._flows.bulk_enrich_flows(flows, user_id)
