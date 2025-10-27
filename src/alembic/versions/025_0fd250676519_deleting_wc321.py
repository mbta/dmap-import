"""deleting WC321

Revision ID: 0fd250676519
Revises: 7ac3ef57d137
Create Date: 2025-10-24 15:16:48.396353

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager

# revision identifiers, used by Alembic.
revision: str = "0fd250676519"
down_revision: Union[str, None] = "d09f890f2828"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wc_321_clearing_house;")


def downgrade() -> None:
    pass
