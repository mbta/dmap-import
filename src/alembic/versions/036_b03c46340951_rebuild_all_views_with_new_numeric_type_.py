"""Rebuild all views with new 'numeric' type casting

Revision ID: b03c46340951
Revises: 1a962fdb6931
Create Date: 2026-06-01 16:58:43.261402

"""

from typing import Sequence, Union

from cubic_loader.qlik.sql_strings.views import (
    FAREREV_RECOVERY_TXN_A,
    FAREREV_RECOVERY_TXN_C,
    WC231_CLEARING_HOUSE,
    WC231_PASS_ID_ADHOC,
    WC232_ACQ_CONF,
    WC232_DEVICE_CASH_STC,
    WC232_MISC,
    WC232_PAYMENT_REJECTION,
    WC232_SALE,
    WC232_SYS_CONF,
    WC321_CLEARING_HOUSE,
    WO110,
    WO150,
)

from alembic import op
import sqlalchemy as sa

from cubic_loader.utils.postgres import DatabaseManager

# revision identifiers, used by Alembic.
revision: str = "b03c46340951"
down_revision: Union[str, None] = "1a962fdb6931"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    for view in [
        FAREREV_RECOVERY_TXN_A,
        FAREREV_RECOVERY_TXN_C,
        WC231_CLEARING_HOUSE,
        WC231_PASS_ID_ADHOC,
        WC232_ACQ_CONF,
        WC232_DEVICE_CASH_STC,
        WC232_MISC,
        WC232_PAYMENT_REJECTION,
        WC232_SALE,
        WC232_SYS_CONF,
        WC321_CLEARING_HOUSE,
        WO110,
        WO150,
    ]:
        op.execute(view)


def downgrade() -> None:
    pass
