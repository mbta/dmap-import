"""add WC231 and WC700_COMP_C views

Revision ID: 6ce4db2752fb
Revises: 4b8117e23c8e
Create Date: 2025-07-03 10:02:44.209868

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WC321_CLEARING_HOUSE
from cubic_loader.qlik.sql_strings.views import WC700_COMP_C_VIEW

# revision identifiers, used by Alembic.
revision: str = "6ce4db2752fb"
down_revision: Union[str, None] = "4b8117e23c8e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WC321_CLEARING_HOUSE)
    op.execute(WC700_COMP_C_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wc700_comp_c;")
    op.execute("DROP VIEW IF EXISTS ods.wc_321_clearing_house;")
