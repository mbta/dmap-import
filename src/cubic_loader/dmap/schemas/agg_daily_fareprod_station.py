import sqlalchemy as sa
from cubic_loader.dmap.schemas import SqlBase


class AggDailyFareprodStation(SqlBase):
    """Table for A.2.3 Daily Fare Product Usage Counts by Type and Station"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_daily_fareprod_station"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    dataset_id = sa.Column(sa.String(), nullable=True, index=True)
    date = sa.Column(sa.Date, nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    service = sa.Column(sa.String(), nullable=True)
    station = sa.Column(sa.String(), nullable=True)
    fare_product_type = sa.Column(sa.String(), nullable=True)
    count = sa.Column(sa.Integer, nullable=True)
