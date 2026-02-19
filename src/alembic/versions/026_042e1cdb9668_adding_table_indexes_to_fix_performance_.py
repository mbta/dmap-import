"""Adding table indexes to fix performance of wc320

Revision ID: 042e1cdb9668
Revises: 0fd250676519
Create Date: 2026-02-18 16:56:08.411713

"""
from typing import Sequence, Union

from alembic import op
from cubic_loader.utils.postgres import DatabaseManager

# revision identifiers, used by Alembic.
revision: str = '042e1cdb9668'
down_revision: Union[str, None] = '0fd250676519'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(
        "CREATE INDEX edw_sale_transaction_sale_type_key_26 "
        "ON ods.edw_sale_transaction (sale_type_key) "
        "WHERE sale_type_key = 26"
    )
    op.execute(
        "CREATE INDEX edw_use_transaction_patron_trip_price "
        "ON ods.edw_use_transaction (patron_trip_id, trip_price_count) "
        "WHERE value_changed <> 0"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ods.edw_use_transaction_patron_trip_price")
    op.execute("DROP INDEX IF EXISTS ods.edw_sale_transaction_sale_type_key_26")
