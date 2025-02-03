"""pk_id to bigint

Revision ID: bf94c2890bc9
Revises: ab250e2a0b0d
Create Date: 2025-02-03 10:14:44.209868

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "bf94c2890bc9"
down_revision: Union[str, None] = "ab250e2a0b0d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "device_event",
        "pk_id",
        existing_type=sa.INTEGER(),
        type_=sa.BigInteger(),
        existing_nullable=False,
    )
    op.alter_column(
        "sale_transaction",
        "pk_id",
        existing_type=sa.INTEGER(),
        type_=sa.BigInteger(),
        existing_nullable=False,
    )
    op.alter_column(
        "use_transaction_location",
        "pk_id",
        existing_type=sa.INTEGER(),
        type_=sa.BigInteger(),
        existing_nullable=False,
    )
    op.alter_column(
        "use_transaction_longitudinal",
        "pk_id",
        existing_type=sa.INTEGER(),
        type_=sa.BigInteger(),
        existing_nullable=False,
    )


def downgrade() -> None:
    # Can not migrate from INT to BIGINT without losing data.
    pass
