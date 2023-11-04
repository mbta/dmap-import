import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class AggDailyFareprod(SqlBase):
    """Table for A.2.9 DAILY FARE PRODUCT USAGE COUNTS BY TYPE"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_daily_fareprod"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    dataset_id = sa.Column(sa.String(), nullable=True, index=True)
    date = sa.Column(sa.Date, nullable=True)
    service = sa.Column(sa.String(), nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    rider_class_name = sa.Column(sa.String(), nullable=True)
    fare_product_type = sa.Column(sa.String(), nullable=True)
    ride_type_name = sa.Column(sa.String(), nullable=True)
    count = sa.Column(sa.Integer, nullable=True)
    weekend = sa.Column(sa.Integer, nullable=True)
