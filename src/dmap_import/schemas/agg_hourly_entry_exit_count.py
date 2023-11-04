import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class AggHourlyEntryExitCount(SqlBase):
    """Table for A.2.2 Hourly Station Entry/Exit Counts (Minimum 10 Counts)"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_hourly_entry_exit_count"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    dataset_id = sa.Column(sa.String(), nullable=True, index=True)
    date = sa.Column(sa.Date, nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    timeperiod = sa.Column(sa.String(), nullable=True)
    hour = sa.Column(sa.Integer, nullable=True)
    station = sa.Column(sa.String(), nullable=True)
    entries = sa.Column(sa.Integer, nullable=True)
    exits = sa.Column(sa.Integer, nullable=True)
