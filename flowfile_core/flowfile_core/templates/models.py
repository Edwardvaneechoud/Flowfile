from typing import Literal

from pydantic import BaseModel


class FlowTemplateMeta(BaseModel):
    """Metadata for a flow template shown in the gallery."""

    template_id: str
    name: str
    description: str
    category: Literal["Beginner", "Intermediate", "Advanced"]
    tags: list[str]
    node_count: int
    icon: str  # material icon name
