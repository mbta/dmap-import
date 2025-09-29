"""adding a new column

Revision ID: 12c471022b21
Revises: 07db86066661
Create Date: 2025-09-29 15:10:22.247607

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WA160_VIEW

# revision identifiers, used by Alembic.
revision: str = '12c471022b21'
down_revision: Union[str, None] = '07db86066661'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WA160_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wa160;")
