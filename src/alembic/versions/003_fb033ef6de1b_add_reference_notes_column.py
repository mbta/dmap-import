"""add reference_notes column

Revision ID: fb033ef6de1b
Revises: 0d3bf1348c58
Create Date: 2024-07-09 09:03:11.190561

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "fb033ef6de1b"
down_revision: Union[str, None] = "0d3bf1348c58"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "use_transaction_longitudinal",
        sa.Column("reference_notes", sa.String()),
    )
    op.add_column(
        "use_transaction_location", sa.Column("reference_notes", sa.String())
    )
    op.add_column("sale_transaction", sa.Column("reference_notes", sa.String()))


def downgrade() -> None:
    op.drop_column("use_transaction_longitudinal", "reference_notes")
    op.drop_column("use_transaction_location", "reference_notes")
    op.drop_column("sale_transaction", "reference_notes")
