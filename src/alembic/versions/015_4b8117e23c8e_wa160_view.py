"""wa160 view

Revision ID: 4b8117e23c8e
Revises: 140dde7b15a9
Create Date: 2025-06-26 14:35:44.209868

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WA160_VIEW

# revision identifiers, used by Alembic.
revision: str = "4b8117e23c8e"
down_revision: Union[str, None] = "140dde7b15a9"
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
