from typing import TypeAlias, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field


config_options: TypeAlias = Literal["in_line", "key_vault"]


class AirbyteConfig(BaseModel):
    source_name: str
    selected_stream: Optional[str] = None
    config_mode: config_options = "in_line"
    mapped_config_spec: Optional[Dict[str, Any]] = Field(default_factory=dict)
    parsed_config: Optional[Any] = None
    connection_name: Optional[str] = None
    version: Optional[str] = None

    @property
    def full_source_name(self) -> str:
        return f"source-{self.source_name}"

