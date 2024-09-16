"""alter customer_postal_code to varchar

Revision ID: 8638e949eea1
Revises: 775b2cf7ab94
Create Date: 2024-09-16 15:12:30.287594

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8638e949eea1"
down_revision: Union[str, None] = "775b2cf7ab94"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("sale_transaction", "customer_postal_code", type_=sa.String())
    op.alter_column("use_transaction_location", "customer_postal_code", type_=sa.String())
    op.alter_column("use_transaction_longitudinal", "customer_postal_code", type_=sa.String())


def downgrade() -> None:
    op.alter_column(
        "sale_transaction",
        "customer_postal_code",
        type_=sa.BigInteger(),
        postgresql_using="customer_postal_code::bigint",
    )
    op.alter_column(
        "use_transaction_location",
        "customer_postal_code",
        type_=sa.BigInteger(),
        postgresql_using="customer_postal_code::bigint",
    )
    op.alter_column(
        "use_transaction_longitudinal",
        "customer_postal_code",
        type_=sa.BigInteger(),
        postgresql_using="customer_postal_code::bigint",
    )
