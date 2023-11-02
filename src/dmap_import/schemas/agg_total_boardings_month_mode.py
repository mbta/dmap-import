import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class AggTotalBoardingsMonthMode(SqlBase):
    """Table for A.2.6 Total Monthly Boardings /Entries by Month and Mode"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_total_boardings_month_mode"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    year = sa.Column(sa.Integer, nullable=True)
    month = sa.Column(sa.String(), nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    service = sa.Column(sa.String(), nullable=True)
    boardings_entries = sa.Column(sa.Integer, nullable=True)
