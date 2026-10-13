"""Multiple view updates ported from Odin

Revision ID: 6008e1f3cbdd
Revises: 22c9c3da353d
Create Date: 2026-10-01 16:36:15.537359

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.qlik.sql_strings.views import (
    UNPROCESSED_TAPS_VIEW,
    WC700_COMP_A_VIEW,
    WSP611_OPERATION_PERFORMANCE_VIEW,
    WSP611_SYSTEM_AVAILABILITY_VIEW,
    WSP620_AVAILABILITY_VIEW,
    WSP620_PERFORMANCE_VIEW,
    WSP630_VIEW,
)


# revision identifiers, used by Alembic.
revision: str = "6008e1f3cbdd"
down_revision: Union[str, None] = "22c9c3da353d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    for view_def in [
        UNPROCESSED_TAPS_VIEW,
        WC700_COMP_A_VIEW,
        WSP611_OPERATION_PERFORMANCE_VIEW,
        WSP611_SYSTEM_AVAILABILITY_VIEW,
        WSP620_AVAILABILITY_VIEW,
        WSP620_PERFORMANCE_VIEW,
        WSP630_VIEW,
    ]:
        op.execute(view_def)


def downgrade() -> None:
    for view_name in [
        "unprocessed_taps",
        "wsp611_operation_performance",
        "wsp611_system_availability",
        "wsp620_availability",
        "wsp620_performance",
        "wsp630",
    ]:
        op.execute(f"DROP VIEW IF EXISTS ods.{view_name};")
