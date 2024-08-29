import sqlalchemy as sa
from cubic_loader.dmap.schemas import SqlBase


class ApiMetadata(SqlBase):
    """Table for CUBIC API endpoint metadata"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "api_metadata"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    url = sa.Column(sa.String(), nullable=False, unique=True)
    last_updated = sa.Column(sa.DateTime, nullable=False)
