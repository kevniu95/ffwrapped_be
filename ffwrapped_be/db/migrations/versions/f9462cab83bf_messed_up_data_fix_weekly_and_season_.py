"""Messed up data, fix weekly and season tables

Revision ID: f9462cab83bf
Revises: d08e2d2cf9a1
Create Date: 2025-02-23 00:09:03.904224

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f9462cab83bf"
down_revision: Union[str, None] = "d08e2d2cf9a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Rename the existing player_week table to player_week_reference_deprecated
    op.rename_table("player_week", "player_week_reference_deprecated")

    # Create the new player_week table
    op.create_table(
        "player_week",
        sa.Column(
            "player_season_id",
            sa.Integer,
            sa.ForeignKey("player_season.player_season_id"),
            primary_key=True,
        ),
        sa.Column("week", sa.Integer, primary_key=True),
        sa.Column("tm_id", sa.Integer, sa.ForeignKey("team.team_id")),
    )

    op.execute(
        """
        INSERT INTO player_week (player_season_id, week, tm_id)
        SELECT player_season_id, week, tm_id 
        FROM player_week_reference_deprecated
    """
    )


def downgrade() -> None:
    # Drop the new player_week table
    op.drop_table("player_week")

    # Rename the player_week_reference_deprecated table back to player_week
    op.rename_table("player_week_reference_deprecated", "player_week")
