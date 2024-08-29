"""add multi_ride_id, token_id columns to use_transaction

Revision ID: 775b2cf7ab94
Revises: 214f62b149e9
Create Date: 2024-07-30 15:32:28.457690

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "775b2cf7ab94"
down_revision: Union[str, None] = "214f62b149e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("use_transaction_location", sa.Column("multi_ride_id", sa.BigInteger()))
    op.add_column("use_transaction_location", sa.Column("token_id", sa.BigInteger()))
    op.add_column(
        "use_transaction_longitudinal",
        sa.Column("multi_ride_id", sa.BigInteger()),
    )
    op.add_column("use_transaction_longitudinal", sa.Column("token_id", sa.BigInteger()))


def downgrade() -> None:
    op.drop_column("use_transaction_location", "multi_ride_id")
    op.drop_column("use_transaction_location", "token_id")
    op.drop_column("use_transaction_longitudinal", "multi_ride_id")
    op.drop_column("use_transaction_longitudinal", "token_id")
