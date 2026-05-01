"""Adding FAREREV_RECOVERY_TXN views

Revision ID: 36f89551fe75
Revises: f5ded62bd362
Create Date: 2026-05-01 11:07:38.070903

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import FAREREV_RECOVERY_TXN_C, FAREREV_RECOVERY_TXN_A

# revision identifiers, used by Alembic.
revision: str = "36f89551fe75"
down_revision: Union[str, None] = "f5ded62bd362"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(FAREREV_RECOVERY_TXN_C)
    op.execute(FAREREV_RECOVERY_TXN_A)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS cubic_reports.farerev_recovery_txn_c;")
    op.execute("DROP VIEW IF EXISTS cubic_reports.farerev_recovery_txn_a;")
