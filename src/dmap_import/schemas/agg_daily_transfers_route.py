import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class AggDailyTransfersRoute(SqlBase):
    """Table for A.2.8 Daily Transfers by Route"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_daily_transfers_route"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    dataset_id = sa.Column(sa.String(), nullable=True, index=True)
    date = sa.Column(sa.Date, nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    route = sa.Column(sa.String(), nullable=True)
    transfer_taps_from_route = sa.Column(sa.Integer, nullable=True)
