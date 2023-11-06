import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class AggDailyTransfersStation(SqlBase):
    """Table for A.2.7 Daily Transfers by Station"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_daily_transfers_station"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    dataset_id = sa.Column(sa.String(), nullable=True, index=True)
    date = sa.Column(sa.Date, nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    station = sa.Column(sa.String(), nullable=True)
    transfer_taps_at_station = sa.Column(sa.Integer, nullable=True)
