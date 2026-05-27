"""resetting table edw_payment_summary

Revision ID: 1a962fdb6931
Revises: 873bd2afd66f
Create Date: 2026-05-18 15:48:08.064966

"""

from typing import Sequence, Union

from cubic_loader.qlik.ods_qlik import CubicODSQlik
from cubic_loader.qlik.rds_utils import drop_table
from cubic_loader.utils.aws import s3_delete_object
from cubic_loader.utils.postgres import DatabaseManager


# revision identifiers, used by Alembic.
revision: str = "1a962fdb6931"
down_revision: Union[str, None] = "873bd2afd66f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    table = "EDW.PAYMENT_SUMMARY"
    qlik_table = CubicODSQlik(table)
    db = DatabaseManager()

    # Ensure status is reset so ETL starts from source snapshot again
    s3_delete_object(qlik_table.status_path)

    fact_schema, fact_table = qlik_table.db_fact_table.split(".", maxsplit=1)
    history_schema, history_table = qlik_table.db_history_table.split(".", maxsplit=1)

    if db.table_exists(history_schema, history_table):
        db.execute(drop_table(qlik_table.db_history_table))

    if db.table_exists(fact_schema, fact_table):
        db.truncate_table(qlik_table.db_fact_table, restart_identity=True, cascade=True)


def downgrade() -> None:
    return None
