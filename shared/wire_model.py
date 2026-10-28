"""The base of the core-worker wire models that refuse an unknown field: every request body and a few reports.

The answers core reads (``ResultRef``, ``SubmitResult``, ``Capabilities``, ``Status``) stay plain ``BaseModel``.
"""

from pydantic import BaseModel, ConfigDict


class WireModel(BaseModel):
    """A model that refuses an unknown field rather than dropping it, so a field the sender adds fails loudly.

    A subclass that sets its own ``model_config`` keeps this one's ``extra="forbid"``: pydantic merges the two.
    """

    model_config = ConfigDict(extra="forbid")
