"""drop 'reference' column

Revision ID: 214f62b149e9
Revises: fb033ef6de1b
Create Date: 2024-07-11 12:34:27.185019

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "214f62b149e9"
down_revision: Union[str, None] = "fb033ef6de1b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("use_transaction_longitudinal", "reference")
    op.drop_column("use_transaction_location", "reference")
    op.drop_column("sale_transaction", "reference")


def downgrade() -> None:
    op.add_column(
        "use_transaction_longitudinal",
        sa.Column("reference", sa.String()),
    )
    op.add_column("use_transaction_location", sa.Column("reference", sa.String()))
    op.add_column("sale_transaction", sa.Column("reference", sa.String()))
