"""Fixing wa160

Revision ID: 1018cd5a6a5e
Revises: d84dfdee27dc
Create Date: 2026-05-26 15:58:08.446107

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WO118, WA160_VIEW

# revision identifiers, used by Alembic.
revision: str = '1018cd5a6a5e'
down_revision: Union[str, None] = 'd84dfdee27dc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WA160_VIEW)


def downgrade() -> None:
    pass
