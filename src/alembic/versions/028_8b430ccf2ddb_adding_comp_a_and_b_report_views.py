"""Adding COMP A and B report views

Revision ID: 8b430ccf2ddb
Revises: d4f8a1e23c90
Create Date: 2026-03-27 13:50:18.416489

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import COMP_A_TXN_A, COMP_A_TXN_C, COMP_B_TXN_A, COMP_B_TXN_C
from cubic_loader.qlik.sql_strings.views import WA160_VIEW


# revision identifiers, used by Alembic.
revision: str = "8b430ccf2ddb"
down_revision: Union[str, None] = "d4f8a1e23c90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(COMP_A_TXN_A)
    op.execute(COMP_A_TXN_C)
    op.execute(COMP_B_TXN_A)
    op.execute(COMP_B_TXN_C)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.farerev_payg_trip_txn_a;")
    op.execute("DROP VIEW IF EXISTS ods.farerev_payg_trip_txn_c;")
    op.execute("DROP VIEW IF EXISTS ods.farerev_prod_sales_txn_a;")
    op.execute("DROP VIEW IF EXISTS ods.farerev_prod_sales_txn_c;")
