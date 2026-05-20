"""Adding WO118, and new columns to WA160

Revision ID: 873bd2afd66f
Revises: 91200623e443
Create Date: 2026-05-20 16:45:36.210820

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WO118, WA160_VIEW

# revision identifiers, used by Alembic.
revision: str = "873bd2afd66f"
down_revision: Union[str, None] = "91200623e443"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WO118)
    op.execute(WA160_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wo118;")
    op.execute("DROP VIEW IF EXISTS ods.wa160;")
