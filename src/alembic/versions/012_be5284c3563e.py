"""abp_tap index

Revision ID: be5284c3563e
Revises: bf94c2890bc9
Create Date: 2025-02-03 10:14:44.209868

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from cubic_loader.qlik.sql_strings.views import AD_HOC_VIEW
from cubic_loader.qlik.sql_strings.views import COMP_A_VIEW
from cubic_loader.qlik.sql_strings.views import COMP_B_VIEW
from cubic_loader.qlik.sql_strings.views import COMP_D_VIEW

# revision identifiers, used by Alembic.
revision: str = "be5284c3563e"
down_revision: Union[str, None] = "bf94c2890bc9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER SEQUENCE device_event_pk_id_seq AS bigint;")
    op.execute("ALTER SEQUENCE sale_transaction_pk_id_seq AS bigint;")
    op.execute("ALTER SEQUENCE use_transaction_location_pk_id_seq AS bigint;")
    op.execute("ALTER SEQUENCE use_transaction_longitudinal_pk_id_seq AS bigint;")
    op.execute("CREATE INDEX idx_abp_tap_inserted ON ods.edw_abp_tap (source_inserted_dtm);")
    op.execute(AD_HOC_VIEW)
    op.execute(COMP_A_VIEW)
    op.execute(COMP_B_VIEW)
    op.execute(COMP_D_VIEW)


def downgrade() -> None:
    op.execute("DROP INDEX idx_abp_tap_inserted;")
    op.execute("DROP VIEW IF EXISTS ods.ad_hoc_processed_taps;")
    op.execute("DROP VIEW IF EXISTS ods.wc700_comp_a;")
    op.execute("DROP VIEW IF EXISTS ods.wc700_comp_b;")
    op.execute("DROP VIEW IF EXISTS ods.wc700_comp_d;")
