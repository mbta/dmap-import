"""adding WC231 view

Revision ID: 328f5dd53ae9
Revises: 7ac3ef57d137
Create Date: 2025-10-22 13:48:11.214181

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WC231_PASS_ID_ADHOC
from cubic_loader.qlik.sql_strings.views import WC231_CLEARING_HOUSE


# revision identifiers, used by Alembic.
revision: str = 'd09f890f2828'
down_revision: Union[str, None] = '7ac3ef57d137'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute(WC231_PASS_ID_ADHOC)
    op.execute(WC231_CLEARING_HOUSE)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.WC231_PASS_ID_ADHOC;")
    op.execute("DROP VIEW IF EXISTS ods.WC231_CLEARING_HOUSE;")

