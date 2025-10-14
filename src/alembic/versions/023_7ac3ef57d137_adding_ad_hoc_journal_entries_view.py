"""adding ad-hoc journal entries view

Revision ID: 7ac3ef57d137
Revises: 12c471022b21
Create Date: 2025-10-14 13:56:38.029070

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import AD_HOC_JOURNAL_ENTRIES


# revision identifiers, used by Alembic.
revision: str = '7ac3ef57d137'
down_revision: Union[str, None] = '12c471022b21'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(AD_HOC_JOURNAL_ENTRIES)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.ad_hoc_journal_entries;")
