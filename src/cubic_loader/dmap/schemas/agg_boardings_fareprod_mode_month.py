import sqlalchemy as sa
from cubic_loader.dmap.schemas import SqlBase


class AggBoardingsFareprodModeMonth(SqlBase):
    """Table for A.2.5 Taps by Fare Product, Service, and Month"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "agg_boardings_fareprod_mode_month"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    dataset_id = sa.Column(sa.String(), nullable=True, index=True)
    year = sa.Column(sa.Integer, nullable=True)
    month = sa.Column(sa.String(), nullable=True)
    day_of_week = sa.Column(sa.String(), nullable=True)
    service = sa.Column(sa.String(), nullable=True)
    fare_product_type = sa.Column(sa.String(), nullable=True)
    count = sa.Column(sa.Integer, nullable=True)
