"""Adjustment to wa160

Revision ID: 4b9e25ccbea7
Revises: ff97f2739274
Create Date: 2026-08-13 16:35:26.446652

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WA160_VIEW, WO110


# revision identifiers, used by Alembic.
revision: str = "4b9e25ccbea7"
down_revision: Union[str, None] = "ff97f2739274"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WA160_VIEW)
    op.execute(WO110)


def downgrade() -> None:
    pass
