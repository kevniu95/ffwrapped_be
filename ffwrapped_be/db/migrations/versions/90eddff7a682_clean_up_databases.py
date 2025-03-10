"""Clean up databases

Revision ID: 90eddff7a682
Revises: a91077650fab
Create Date: 2025-02-23 23:15:18.343424

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "90eddff7a682"
down_revision: Union[str, None] = "a91077650fab"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("player_week_metadata")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "player_week_metadata",
        sa.Column(
            "player_week_metadata_id", sa.INTEGER(), autoincrement=True, nullable=False
        ),
        sa.Column("season", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column(
            "chunk_start_value", sa.INTEGER(), autoincrement=False, nullable=True
        ),
        sa.Column("chunk_size", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column("completed", sa.BOOLEAN(), autoincrement=False, nullable=True),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=True,
        ),
        sa.PrimaryKeyConstraint(
            "player_week_metadata_id", name="player_week_metadata_pkey"
        ),
        sa.UniqueConstraint(
            "season",
            "chunk_start_value",
            "chunk_size",
            name="player_week_metadata_season_chunk_start_value_chunk_size_key",
        ),
    )
    # ### end Alembic commands ###
