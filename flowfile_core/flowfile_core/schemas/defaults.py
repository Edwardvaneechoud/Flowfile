from schemas import transform_schema
from pydantic import Field, BaseModel


default_union_input = transform_schema.UnionInput


class F(BaseModel):
    f: transform_schema.UnionInput = Field(default_factory=default_union_input)