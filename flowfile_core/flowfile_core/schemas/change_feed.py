"""Delta change-feed read settings shared by the catalog reader and the cloud Delta reader.

Pydantic and stdlib only, so ``cloud_storage_schemas`` and ``input_schema`` can both import it.
"""

import re
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, model_validator

_PARAM_REF_RE = re.compile(r"^\$\{[a-zA-Z_][a-zA-Z0-9_]*\}$")  # a whole-field flow-parameter reference


class ChangeFeedReadSettings(BaseModel):
    """The "Read" selector of a Delta change-feed reader.

    Holds only the rules every change-feed reader shares: the since-version / since-time start
    values must be present and well-formed (or a whole-field ``${param}`` reference). Each reader
    adds its own rules on top: the catalog reader its cursor fields and source conflicts, the cloud
    reader its Delta-only check and a narrower ``cdc_mode`` (no cursor store, so no
    ``since_last_run``).
    """

    cdc_mode: Literal["off", "since_last_run", "since_version", "since_timestamp"] = "off"
    cdc_from_version: int | str | None = None  # a commit version, or a whole-field ${param} reference
    cdc_from_timestamp: str | None = None  # ISO-8601 instant or ${param}, required when cdc_mode == "since_timestamp"
    cdc_include_preimage: bool = False

    @model_validator(mode="after")
    def _validate_change_window(self) -> "ChangeFeedReadSettings":
        if self.cdc_mode == "since_version":
            if self.cdc_from_version is None:
                raise ValueError("cdc_from_version is required when cdc_mode is 'since_version'")
            if isinstance(self.cdc_from_version, str) and not _PARAM_REF_RE.match(self.cdc_from_version):
                raise ValueError("cdc_from_version must be a commit version or a ${parameter} reference")
        if self.cdc_mode == "since_timestamp":
            if not self.cdc_from_timestamp:
                raise ValueError("cdc_from_timestamp is required when cdc_mode is 'since_timestamp'")
            if _PARAM_REF_RE.match(self.cdc_from_timestamp):
                return self
            try:
                # Python 3.10's fromisoformat rejects the trailing "Z" the UI's DateTimePicker emits.
                datetime.fromisoformat(self.cdc_from_timestamp.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(
                    f"cdc_from_timestamp must be an ISO-8601 datetime: {self.cdc_from_timestamp!r}"
                ) from exc
        return self
