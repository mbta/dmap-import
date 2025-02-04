"""reset qlik tables with possible null primary key values

Revision ID: 96837b10c106
Revises: 8638e949eea1
Create Date: 2021-01-07 19:42:12.287594

"""

import os
from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.aws import s3_delete_object
from cubic_loader.utils.remote_locations import ODS_STATUS

# revision identifiers, used by Alembic.
revision: str = "96837b10c106"
down_revision: Union[str, None] = "8638e949eea1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass
    # Delete status files for all tables loaded since the NOT NULL Primary Key requirement was dropped.
    # This will force a reset/re-load for these tables in dmap-import DB
    # This was one-time migration, does not need to remain active
    # s3_delete_object(os.path.join(ODS_STATUS, "EDW.PAYMENT_SUMMARY.json"))
    # s3_delete_object(os.path.join(ODS_STATUS, "EDW.MEMBER_DIMENSION.json"))
    # op.drop_table("edw_payment_summary", schema="ods")
    # op.drop_table("edw_payment_summary_history", schema="ods")
    # op.drop_table("edw_member_dimension", schema="ods")
    # op.drop_table("edw_member_dimension_history", schema="ods")


def downgrade() -> None:
    # Nothing to downgrade
    pass
