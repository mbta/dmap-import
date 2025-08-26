"""feat: add validator_interface to use_transaction_*

Revision ID: 92ab6c167c5b
Revises: 76a64efdbdd9
Create Date: 2025-08-26 13:35:20.236120

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "92ab6c167c5b"
down_revision: Union[str, None] = "76a64efdbdd9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("use_transaction_longitudinal", sa.Column("validator_interface", sa.String()))
    op.add_column("use_transaction_location", sa.Column("validator_interface", sa.String()))


def downgrade() -> None:
    op.drop_column("use_transaction_longitudinal", "validator_interface")
    op.drop_column("use_transaction_location", "validator_interface")
