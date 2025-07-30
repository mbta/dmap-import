from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.mat_views import WC320_LATE_TAP_ADJUSTMENT


# revision identifiers, used by Alembic.
revision: str = "b6ecb4180ef0"
down_revision: Union[str, None] = "6ce4db2752fb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WC320_LATE_TAP_ADJUSTMENT)


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS ods.wc320_late_tap_adjustment;")
