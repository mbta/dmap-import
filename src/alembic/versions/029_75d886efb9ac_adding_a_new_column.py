"""Updating COMP_B_TXN_A, COMP_B_TXN_C

Revision ID: 75d886efb9ac
Revises: 8b430ccf2ddb
Create Date: 2026-04-06 10:31:37.522827

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.comp_views import COMP_B_TXN_A, COMP_B_TXN_C

# revision identifiers, used by Alembic.
revision: str = "75d886efb9ac"
down_revision: Union[str, None] = "8b430ccf2ddb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(COMP_B_TXN_A)
    op.execute(COMP_B_TXN_C)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.farerev_payg_trip_txn_a;")
    op.execute("DROP VIEW IF EXISTS ods.farerev_payg_trip_txn_c;")
