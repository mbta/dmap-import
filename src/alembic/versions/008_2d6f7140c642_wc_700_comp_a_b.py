"""add views for wc700 comp a&b

Revision ID: 2d6f7140c642
Revises: 96837b10c106
Create Date: 2021-01-10 09:49:12.274894

"""

import logging
from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager

# revision identifiers, used by Alembic.
revision: str = "2d6f7140c642"
down_revision: Union[str, None] = "96837b10c106"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    comp_a_view = """
        CREATE OR REPLACE VIEW ods.wc700_comp_a 
        AS
        SELECT 
            ps.settlement_day_key
            ,ps.operating_day_key
            ,ps.payment_type_key
            ,tcm.txn_channel_display
            ,tcm.sales_channel_display
            ,SUM(COALESCE(transit_value,0) + COALESCE(benefit_value,0) + COALESCE(bankcard_payment_value,0) + COALESCE(one_account_value,0))/100 AS stored_value
            ,SUM(COALESCE(pass_cost,0))/100 AS pass_cost
            ,SUM(COALESCE(enablement_fee,0))/100 AS enablement_fee
            ,SUM(COALESCE(replacement_fee, 0))/100 AS replacement_fee
            ,SUM(COALESCE(transit_value,0) + COALESCE(benefit_value,0) + COALESCE(bankcard_payment_value,0) + COALESCE(one_account_value,0) + COALESCE(pass_cost,0) + COALESCE(enablement_fee,0) + COALESCE(replacement_fee, 0))/100 AS total_fare_revenue
        FROM 
            ods.edw_payment_summary ps
        JOIN 
            ods.edw_txn_channel_map tcm 
            ON 
                tcm.txn_source = ps.txn_source 
                AND tcm.sales_channel_key = ps.sales_channel_key 
                and tcm.payment_type_key = ps.payment_type_key
        WHERE 
            tcm.txn_group = 'Open Payment Trips'
        GROUP BY
            ps.settlement_day_key
            ,ps.operating_day_key
            ,ps.payment_type_key
            ,tcm.txn_channel_display
            ,tcm.sales_channel_display
        ORDER BY
            operating_day_key desc
            ,settlement_day_key desc
        ;
    """
    comp_b_view = """
        CREATE OR REPLACE VIEW ods.wc700_comp_b 
        AS
        SELECT 
            ps.settlement_day_key
            ,ps.operating_day_key
            ,ps.payment_type_key
            ,tcm.txn_channel_display
            ,tcm.sales_channel_display
            ,SUM(COALESCE(payment_value,0))/100 AS total_fare_revenue
        FROM 
            ods.edw_payment_summary ps
        JOIN 
            ods.edw_txn_channel_map tcm 
            ON 
                tcm.txn_source = ps.txn_source 
                AND tcm.sales_channel_key = ps.sales_channel_key 
                and tcm.payment_type_key = ps.payment_type_key
        WHERE 
            tcm.txn_group = 'Open Payment Trips'
        GROUP BY
            ps.settlement_day_key
            ,ps.operating_day_key
            ,ps.payment_type_key
            ,tcm.txn_channel_display
            ,tcm.sales_channel_display
        ORDER BY
            operating_day_key desc
            ,settlement_day_key desc
        ;
    """
    try:
        op.execute(comp_a_view)
        op.execute(comp_b_view)
    except Exception as exception:
        logging.exception(exception)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS ods.wc700_comp_a;")
    op.execute("DROP VIEW IF EXISTS ods.wc700_comp_b;")
