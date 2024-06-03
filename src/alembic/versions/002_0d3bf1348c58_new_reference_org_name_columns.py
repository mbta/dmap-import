"""new reference org name columns

Revision ID: 0d3bf1348c58
Revises: d630193252db
Create Date: 2024-06-01 22:10:22.189820

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0d3bf1348c58"
down_revision: Union[str, None] = "d630193252db"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "use_transaction_longitudinal", sa.Column("reference", sa.String())
    )
    op.add_column(
        "use_transaction_location", sa.Column("reference", sa.String())
    )
    op.add_column("sale_transaction", sa.Column("reference", sa.String()))


def downgrade() -> None:
    op.drop_column("use_transaction_longitudinal", "reference")
    op.drop_column("use_transaction_location", "reference")
    op.drop_column("sale_transaction", "reference")
