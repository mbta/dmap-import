import sqlalchemy as sa
from cubic_loader.dmap.schemas import SqlBase


class Citation(SqlBase):
    """Table for Citations"""

    # pylint: disable=too-few-public-methods

    __tablename__ = "citation"

    # required for pipeline
    pk_id = sa.Column(sa.BigInteger, primary_key=True)
    dataset_id = sa.Column(sa.String(), nullable=True, index=True)

    # dataset columns
    id = sa.Column(sa.String(), nullable=True)
    inserted_dtm = sa.Column(sa.DateTime, nullable=True)
    updated_dtm = sa.Column(sa.DateTime, nullable=True)
    depersonalized_dtm = sa.Column(sa.DateTime, nullable=True)
    updated_flag = sa.Column(sa.String(), nullable=True)
    citation_type_id = sa.Column(sa.String(), nullable=True)
    citation_type_name = sa.Column(sa.String(), nullable=True)
    citation_dtm = sa.Column(sa.DateTime, nullable=True)
    inspector_id = sa.Column(sa.String(), nullable=True)
    citation_reason_name = sa.Column(sa.String(), nullable=True)
    citation_amount = sa.Column(sa.String(), nullable=True)
    direction_id = sa.Column(sa.String(), nullable=True)
    block_number = sa.Column(sa.String(), nullable=True)
    recommendation = sa.Column(sa.String(), nullable=True)
    recommendation_override = sa.Column(sa.String(), nullable=True)
    manually_entered = sa.Column(sa.String(), nullable=True)
    inspection_result = sa.Column(sa.String(), nullable=True)
    citation_history_match_type = sa.Column(sa.String(), nullable=True)
    inspection_id = sa.Column(sa.String(), nullable=True)
    operating_day = sa.Column(sa.DateTime, nullable=True)
    citation_status_id = sa.Column(sa.String(), nullable=True)
    _exported_dtm = sa.Column(sa.DateTime, nullable=True)
