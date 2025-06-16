"""compb addendum matview

Revision ID: 1dc0843c9d69
Revises: be5284c3563e
Create Date: 2025-02-18 112:49:44.209868

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.mat_views import COMP_B_ADDENDUM

# revision identifiers, used by Alembic.
revision: str = "1dc0843c9d69"
down_revision: Union[str, None] = "be5284c3563e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(COMP_B_ADDENDUM)


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS ods.wc700_comp_b_addendum;")
