"""Regenerate edw_frm_crdb_recon_sysconf_acqconf_history

Revision ID: ff97f2739274
Revises: b03c46340951
Create Date: 2026-06-09 15:58:46.258581

"""

from typing import Sequence, Union

import os
from cubic_loader.utils.remote_locations import ODS_SCHEMA, ODS_STATUS
from cubic_loader.qlik.rds_utils import drop_table
from cubic_loader.utils.aws import s3_delete_object
from cubic_loader.utils.postgres import DatabaseManager


# revision identifiers, used by Alembic.
revision: str = "ff97f2739274"
down_revision: Union[str, None] = "b03c46340951"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    table = "EDW.FRM_CRDB_RECON_SYSCONF_ACQCONF"
    db = DatabaseManager()

    status_path = os.path.join(ODS_STATUS, f"{table}.json")
    db_fact_table = f"{ODS_SCHEMA}.{table.replace('.', '_').lower()}"
    db_history_table = f"{db_fact_table}_history"

    fact_schema, fact_table = db_fact_table.split(".", maxsplit=1)
    history_schema, history_table = db_history_table.split(".", maxsplit=1)

    print(f"MIGRATION: status_path {status_path}")
    print(f"MIGRATION: db_fact_table {db_fact_table}")
    print(f"MIGRATION: db_history_table {db_history_table}")

    # Delete the S3 ETL status file. This is the actual trigger for a reload:
    # the pipeline decides what CDC to replay from `last_cdc_ts` in this file,
    # NOT from the contents of the postgres table. With the status gone, the
    # next ETL run rebuilds from the latest S3 snapshot and replays all CDC,
    # refetching the corrupted April/May window.
    s3_delete_object(status_path)

    if db.table_exists(history_schema, history_table):
        db.execute(drop_table(db_history_table))

    if db.table_exists(fact_schema, fact_table):
        db.truncate_table(db_fact_table, restart_identity=True, cascade=True)


def downgrade() -> None:
    return None
