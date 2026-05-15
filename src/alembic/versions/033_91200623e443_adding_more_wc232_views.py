"""adding more wc232 views

Revision ID: 91200623e443
Revises: 36f89551fe75
Create Date: 2026-05-15 14:54:12.092615

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import (
    WC232_SYS_CONF,
    WC232_SALE,
    WC232_PAYMENT_REJECTION,
    WC232_MISC,
    WC232_DEVICE_CASH_STC,
    WC232_ACQ_CONF,
)

# revision identifiers, used by Alembic.
revision: str = "91200623e443"
down_revision: Union[str, None] = "36f89551fe75"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WC232_SYS_CONF)
    op.execute(WC232_SALE)
    op.execute(WC232_PAYMENT_REJECTION)
    op.execute(WC232_MISC)
    op.execute(WC232_DEVICE_CASH_STC)
    op.execute(WC232_ACQ_CONF)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS cubic_reports.wc232_sys_conf;")
    op.execute("DROP VIEW IF EXISTS cubic_reports.wc232_sale;")
    op.execute("DROP VIEW IF EXISTS cubic_reports.wc232_payment_rejection;")
    op.execute("DROP VIEW IF EXISTS cubic_reports.wc232_misc;")
    op.execute("DROP VIEW IF EXISTS cubic_reports.wc232_device_cash_stc;")
    op.execute("DROP VIEW IF EXISTS cubic_reports.wc232_acq_conf;")
