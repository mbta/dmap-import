"""Adding views 042826

Revision ID: f5ded62bd362
Revises: 960c57e8e70b
Create Date: 2026-04-28 12:56:14.621995

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WC232_PATRON_ORDER_VIEW, WC232_REFUND_SALE_VIEW, WC232_USE_VIEW

# revision identifiers, used by Alembic.
revision: str = "f5ded62bd362"
down_revision: Union[str, None] = "960c57e8e70b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WC232_PATRON_ORDER_VIEW)
    op.execute(WC232_REFUND_SALE_VIEW)
    op.execute(WC232_USE_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wc232_patron_order;")
    op.execute("DROP VIEW IF EXISTS ods.wc232_refund_sale;")
    op.execute("DROP VIEW IF EXISTS ods.wc232_use;")
