"""Adding WO110 and WO150 views

Revision ID: 960c57e8e70b
Revises: 75d886efb9ac
Create Date: 2026-04-22 13:36:24.723402

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WO110, WO150

# revision identifiers, used by Alembic.
revision: str = "960c57e8e70b"
down_revision: Union[str, None] = "75d886efb9ac"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WO110)
    op.execute(WO150)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wo110;")
    op.execute("DROP VIEW IF EXISTS ods.wo150;")
