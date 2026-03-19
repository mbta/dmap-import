"""updating WC231 view

Revision ID: f3a1c9d85b72
Revises: 042e1cdb9668
Create Date: 2026-03-19 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WC231_CLEARING_HOUSE


# revision identifiers, used by Alembic.
revision: str = "f3a1c9d85b72"
down_revision: Union[str, None] = "042e1cdb9668"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WC231_CLEARING_HOUSE)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wc231_clearing_house;")
