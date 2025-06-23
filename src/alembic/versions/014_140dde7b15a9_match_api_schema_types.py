"""match api scehma types

Revision ID: 140dde7b15a9
Revises: 1dc0843c9d69
Create Date: 2025-06-23 13:52:30.287594

This change updates dmap-import table schemas to matching types returned by the API Schema endpoint.
This change brings dmap-import types into alignment with the api as of June 23, 2025.

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "140dde7b15a9"
down_revision: Union[str, None] = "1dc0843c9d69"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("device_event", "updated_flag", type_=sa.String())
    op.alter_column("sale_transaction", "updated_flag", type_=sa.String())
    op.alter_column("sale_transaction", "external_account_id", type_=sa.String())
    op.alter_column("sale_transaction", "merchant_id", type_=sa.String())
    op.alter_column("sale_transaction", "serial_nbr", type_=sa.String())
    op.alter_column("use_transaction_location", "updated_flag", type_=sa.String())
    op.alter_column("use_transaction_location", "external_account_id", type_=sa.String())
    op.alter_column("use_transaction_location", "reversed_dtm", type_=sa.String())
    op.alter_column("use_transaction_location", "serial_nbr", type_=sa.String())
    op.alter_column("use_transaction_location", "voided_dtm", type_=sa.String())
    op.alter_column("use_transaction_longitudinal", "updated_flag", type_=sa.String())
    op.alter_column("use_transaction_longitudinal", "external_account_id", type_=sa.String())
    op.alter_column("use_transaction_longitudinal", "serial_nbr", type_=sa.String())


def downgrade() -> None:
    # No downgrade
    pass
