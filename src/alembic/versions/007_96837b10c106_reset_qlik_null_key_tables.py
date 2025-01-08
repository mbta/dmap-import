"""reset qlik tables with possible null primary key values

Revision ID: 96837b10c106
Revises: 8638e949eea1
Create Date: 2021-01-07 19:42:12.287594

"""

import os
from typing import Sequence, Union

from cubic_loader.utils.aws import s3_delete_object
from cubic_loader.utils.remote_locations import ODS_STATUS

# revision identifiers, used by Alembic.
revision: str = "96837b10c106"
down_revision: Union[str, None] = "8638e949eea1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Delete status files for all tables loaded since the NOT NULL Primary Key requirement was dropped.
    # This will force a reset/re-load for these tables in dmap-import DB
    s3_delete_object(os.path.join(ODS_STATUS, "EDW.PAYMENT_SUMMARY.json"))
    s3_delete_object(os.path.join(ODS_STATUS, "EDW.MEMBER_DIMENSION.json"))


def downgrade() -> None:
    # Nothing to downgrade
    pass
