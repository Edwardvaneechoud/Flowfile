"""Pydantic schemas for the "host flows as HTTP APIs" feature.

Covers publishing a registered flow as an endpoint, the typed-parameter
specification exposed by an endpoint, and per-endpoint API keys.
"""

import re
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

ApiParamType = Literal["string", "integer", "float", "boolean", "enum"]

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


class ApiParamSpec(BaseModel):
    """A single query parameter exposed by a published endpoint.

    The ``name`` must match a flow-level ``${name}`` parameter. ``type`` drives
    validation/coercion of the incoming query value before it is substituted.
    """

    name: str
    type: ApiParamType = "string"
    required: bool = False
    default: str | None = None
    enum_values: list[str] | None = None

    @model_validator(mode="after")
    def _validate_enum(self) -> "ApiParamSpec":
        if self.type == "enum" and not self.enum_values:
            raise ValueError(f"parameter '{self.name}' is type 'enum' but has no enum_values")
        return self


class ApiEndpointCreate(BaseModel):
    """Request body to publish a registered flow as an API endpoint."""

    registration_id: int
    slug: str
    enabled: bool = True
    parameters: list[ApiParamSpec] = Field(default_factory=list)

    @field_validator("slug")
    @classmethod
    def _validate_slug(cls, v: str) -> str:
        v = v.strip().lower()
        if not _SLUG_RE.match(v):
            raise ValueError(
                "slug must start with a letter or digit and contain only lowercase "
                "letters, digits, hyphens, and underscores"
            )
        return v


class ApiEndpointUpdate(BaseModel):
    """Request body to update a published endpoint. All fields optional."""

    slug: str | None = None
    enabled: bool | None = None
    parameters: list[ApiParamSpec] | None = None

    @field_validator("slug")
    @classmethod
    def _validate_slug(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip().lower()
        if not _SLUG_RE.match(v):
            raise ValueError(
                "slug must start with a letter or digit and contain only lowercase "
                "letters, digits, hyphens, and underscores"
            )
        return v


class ApiEndpointOut(BaseModel):
    """Public view of a published endpoint.

    ``parameters`` are *inherited* from the flow's own ``${name}`` parameters and
    only refined by the endpoint's stored type overrides (see
    ``api_runner._effective_specs``). The single-endpoint GET resolves them against
    the live flow so the advertised set matches what the runner enforces; the list
    endpoint returns the stored overrides as-is for performance. The two can briefly
    differ if the flow's parameters changed after publishing.
    """

    id: int
    registration_id: int
    owner_id: int
    slug: str
    enabled: bool
    parameters: list[ApiParamSpec] = Field(default_factory=list)
    path: str = ""  # convenience: "/api/data/{slug}"
    flow_name: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ApiTestRequest(BaseModel):
    """Body for the owner-only 'try it' test run of a published endpoint."""

    params: dict[str, str] = Field(default_factory=dict)


class FlowParamInfo(BaseModel):
    """A flow-level ${name} parameter, surfaced so the UI can pre-fill query params."""

    name: str
    default: str = ""


class PublishableFlow(BaseModel):
    """A flow that is API-ready (has an api_response node) but not yet published.

    Surfaced by the APIs tab's "Create API" picker.
    """

    registration_id: int
    name: str
    file_exists: bool = True


class ApiKeyCreate(BaseModel):
    """Request body to mint a new API key for an endpoint."""

    name: str
    expires_at: datetime | None = None


class ApiKeyUpdate(BaseModel):
    """Request body to update an API key in place (rename / enable / disable). All fields optional."""

    name: str | None = None
    enabled: bool | None = None


class ApiKeyOut(BaseModel):
    """Public view of an API key. Never includes the raw token."""

    id: int
    consumer_id: int | None = None
    endpoint_id: int | None = None
    name: str
    key_prefix: str
    enabled: bool
    last_used_at: datetime | None = None
    expires_at: datetime | None = None
    created_at: datetime | None = None


class ApiKeyCreated(ApiKeyOut):
    """Returned once, at key creation, carrying the raw token."""

    api_key: str


# ---------------------------------------------------------------------------
# API consumers (service accounts) — reusable key holders granted access to
# one or more published endpoints. See routes/api_consumers.py.
# ---------------------------------------------------------------------------


class ApiConsumerCreate(BaseModel):
    """Request body to create an API consumer."""

    name: str
    description: str | None = None
    enabled: bool = True


class ApiConsumerUpdate(BaseModel):
    """Request body to update an API consumer. All fields optional."""

    name: str | None = None
    description: str | None = None
    enabled: bool | None = None


class ApiConsumerOut(BaseModel):
    """Public view of an API consumer."""

    id: int
    name: str
    description: str | None = None
    owner_id: int
    enabled: bool
    is_implicit: bool = False
    endpoint_count: int = 0
    key_count: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ApiConsumerGrant(BaseModel):
    """Request body to grant a consumer access to a published endpoint."""

    endpoint_id: int
