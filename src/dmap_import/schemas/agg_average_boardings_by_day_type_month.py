import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class AggAvgBoardingsByDayTypeMonth(SqlBase):
    """Table for A.2.1 Avg Daily Boardings / Entries by Day Type and Month"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_average_boardings_by_day_type_month"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    month = sa.Column(sa.String(), nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    boardings_entries = sa.Column(sa.Float, nullable=True)
    year = sa.Column(sa.Integer, nullable=True)
