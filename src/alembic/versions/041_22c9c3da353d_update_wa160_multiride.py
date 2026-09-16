"""Update WA160 multiride

Revision ID: 22c9c3da353d
Revises: ea1d951b55e1
Create Date: 2026-09-16 15:38:29.246746

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WA160_VIEW
from cubic_loader.qlik.sql_strings.mat_views import MATERIALIZED_USE_TXNS_WA160


# revision identifiers, used by Alembic.
revision: str = "22c9c3da353d"
down_revision: Union[str, None] = "ea1d951b55e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WA160_VIEW)
    op.execute(MATERIALIZED_USE_TXNS_WA160)


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS ods.materialized_use_txns_wa160;")
