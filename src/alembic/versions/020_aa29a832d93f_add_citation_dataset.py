"""add citation dataset

Revision ID: aa29a832d93f
Revises: 92ab6c167c5b
Create Date: 2025-09-05 12:00:12.669250

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'aa29a832d93f'
down_revision: Union[str, None] = '92ab6c167c5b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    op.create_table(
        "citation",
        sa.Column("pk_id", sa.Integer(), nullable=False),
        sa.Column("dataset_id", sa.String(), nullable=True, index=True),
        sa.Column("id", sa.String(), nullable=True),
        sa.Column("inserted_dtm", sa.DateTime(), nullable=True),
        sa.Column("updated_dtm", sa.DateTime(), nullable=True),
        sa.Column("depersonalized_dtm", sa.DateTime(), nullable=True),
        sa.Column("updated_flag", sa.Integer(), nullable=True),
        sa.Column("citation_type_id", sa.String(), nullable=True),
        sa.Column("citation_type_name", sa.String(), nullable=True),
        sa.Column("citation_dtm", sa.DateTime(), nullable=True),
        sa.Column("inspector_id", sa.String(), nullable=True, index=True),
        sa.Column("citation_reason_name", sa.String(), nullable=True),
        sa.Column("citation_amount", sa.String(), nullable=True, index=True), # maybe: use sa.Float()?
        sa.Column("direction_id", sa.String(), nullable=True, index=True),
        sa.Column("block_number", sa.String(), nullable=True),
        sa.Column("recommendation", sa.String(), nullable=True),
        sa.Column("recommendation_override", sa.String(), nullable=True, index=True),
        sa.Column("manually_entered", sa.String(), nullable=True),
        sa.Column("inspection_result", sa.String(), nullable=True),
        sa.Column("citation_history_match_type", sa.String(), nullable=True),
        sa.Column("inspection_id", sa.String(), nullable=True, index=True),
        sa.Column("operating_day", sa.DateTime(), nullable=True),
        sa.Column("citation_status_id", sa.String(), nullable=True, index=True),
        sa.Column("_exported_dtm", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("pk_id"),
    )

def downgrade() -> None:
    op.drop_table("citation")
