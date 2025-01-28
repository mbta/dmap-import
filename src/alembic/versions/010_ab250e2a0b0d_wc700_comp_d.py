"""add views for wc700 comp D

Revision ID: ab250e2a0b0d
Revises: f2c5a52c7149
Create Date: 2025-01-28 12:23:12.274894

"""

import logging
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ab250e2a0b0d"
down_revision: Union[str, None] = "f2c5a52c7149"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    comp_d_view = """
        CREATE OR REPLACE VIEW ods.wc700_comp_d 
        AS
        SELECT 
            ps.settlement_day_key
            ,ps.operating_day_key
            ,ps.payment_type_key
            ,tcm.txn_channel_display
            ,tcm.sales_channel_display
            ,rd.reason_name
            ,SUM(ps.payment_value)/100 as refund_value
        FROM
            ods.edw_payment_summary ps
        JOIN 
            ods.edw_txn_channel_map tcm 
            ON
                tcm.txn_source = ps.txn_source
                AND tcm.sales_channel_key = ps.sales_channel_key
                AND tcm.payment_type_key = ps.payment_type_key
        LEFT JOIN 
            ods.edw_reason_dimension rd 
            ON
                rd.reason_key = ps.reason_key
        WHERE
            tcm.txn_group = 'Direct Refunds Applied'
        GROUP BY
            ps.settlement_day_key
            ,ps.operating_day_key
            ,ps.payment_type_key
            ,tcm.txn_channel_display
            ,tcm.sales_channel_display
            ,rd.reason_name
        ORDER BY
            ps.operating_day_key desc
            ,ps.settlement_day_key desc
        ;
    """
    try:
        op.execute(comp_d_view)
    except Exception as exception:
        logging.exception(exception)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wc700_comp_d;")
