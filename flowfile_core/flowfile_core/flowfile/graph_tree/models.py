from pydantic import BaseModel


class InputInfo(BaseModel):
    main: list[int]
    right: int | None = None
    left: int | None = None


class BranchInfo(BaseModel):
    label: str
    short_label: str
    inputs: InputInfo
    outputs: list[int]
    depth: int
