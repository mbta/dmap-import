"""Update WC700 views

Revision ID: 028408b18bad
Revises: 4b9e25ccbea7
Create Date: 2026-09-15 11:00:28.057719

"""
from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import WC700_COMP_A_VIEW
from cubic_loader.qlik.sql_strings.views import WC700_COMP_B_VIEW
from cubic_loader.qlik.sql_strings.views import WC700_COMP_C_VIEW
from cubic_loader.qlik.sql_strings.views import WC700_COMP_D_VIEW


# revision identifiers, used by Alembic.
revision: str = '028408b18bad'
down_revision: Union[str, None] = '4b9e25ccbea7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    for view_def in [WC700_COMP_A_VIEW,
                     WC700_COMP_B_VIEW,
                     WC700_COMP_C_VIEW,
                     WC700_COMP_D_VIEW]:
        op.execute(view_def)


def downgrade() -> None:
    pass
