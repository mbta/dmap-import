"""alter fare_prod_id to varchar

Revision ID: 76a64efdbdd9
Revises: b6ecb4180ef0
Create Date: 2025-08-06 10:39:30.287594

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "76a64efdbdd9"
down_revision: Union[str, None] = "b6ecb4180ef0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("sale_transaction", "fare_prod_id", type_=sa.String())
    op.alter_column("use_transaction_location", "fare_prod_id", type_=sa.String())
    op.alter_column("use_transaction_longitudinal", "fare_prod_id", type_=sa.String())


def downgrade() -> None:
    # NO downgrade
    pass
