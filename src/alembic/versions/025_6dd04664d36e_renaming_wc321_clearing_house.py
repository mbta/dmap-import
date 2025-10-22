"""renaming WC321_CLEARING_HOUSE

Revision ID: 6dd04664d36e
Revises: d09f890f2828
Create Date: 2025-10-22 14:01:33.562790

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WC231_CLEARING_HOUSE


# revision identifiers, used by Alembic.
revision: str = '6dd04664d36e'
down_revision: Union[str, None] = 'd09f890f2828'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    op.execute("DROP VIEW IF EXISTS ods.WC321_CLEARING_HOUSE;")
    op.execute(WC231_CLEARING_HOUSE)


def downgrade() -> None:
    # Asymmetric downgrade, no need to rename back
    op.execute("DROP VIEW IF EXISTS ods.WC231_CLEARING_HOUSE;")
