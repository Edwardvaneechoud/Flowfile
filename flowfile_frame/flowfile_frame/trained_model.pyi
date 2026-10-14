# Auto-generated stub for flowfile_frame.trained_model — do not edit.
# Run `make stubs` to regenerate from the Python source.
from __future__ import annotations

from typing import Any

class TrainedModel:
    model_type: str
    task_type: str
    target: str
    features: list[str]
    params: dict[str, Any]
    output_dtype: str
    fitted: dict[str, Any]
    @property
    def coefficients(self) -> dict[str, float] | None: ...
    @property
    def intercept(self) -> float | None: ...
    def to_dict(self) -> dict[str, Any]: ...

