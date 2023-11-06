import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class AggDailyFareprodRoute(SqlBase):
    """Table for A.2.4 Daily Fare Product Usage Counts by Type and Route"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_daily_fareprod_route"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    dataset_id = sa.Column(sa.String(), nullable=True, index=True)
    date = sa.Column(sa.Date, nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    service = sa.Column(sa.String(), nullable=True)
    route = sa.Column(sa.String(), nullable=True)
    fare_product_type = sa.Column(sa.String(), nullable=True)
    count = sa.Column(sa.Integer, nullable=True)
