"""Add image_flavour and custom_image to kernels.

Per-kernel image selection: lets a user pick the ``base`` image (Polars +
PyArrow + NumPy) or the ``ml`` image (sklearn / xgboost / lightgbm /
statsmodels pre-baked) without re-pulling, or specify a fully custom image
URI for advanced setups.

Existing kernel rows backfill to ``image_flavour='base'``, matching the
historic behaviour where every kernel ran the base ``flowfile-kernel`` image.

Revision ID: 012
Revises: 011
Create Date: 2026-05-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "012"
down_revision: str | None = "011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _existing_columns(table: str) -> set[str]:
    return {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    existing = _existing_columns("kernels")
    to_add = [c for c in ("image_flavour", "custom_image") if c not in existing]
    if to_add:
        with op.batch_alter_table("kernels") as batch:
            if "image_flavour" in to_add:
                batch.add_column(sa.Column("image_flavour", sa.String(), nullable=True))
            if "custom_image" in to_add:
                batch.add_column(sa.Column("custom_image", sa.String(), nullable=True))

        bind = op.get_bind()
        bind.execute(sa.text("UPDATE kernels SET image_flavour = 'base' WHERE image_flavour IS NULL"))

        with op.batch_alter_table("kernels") as batch:
            batch.alter_column("image_flavour", existing_type=sa.String(), nullable=False)


def downgrade() -> None:
    with op.batch_alter_table("kernels") as batch:
        batch.drop_column("custom_image")
        batch.drop_column("image_flavour")
