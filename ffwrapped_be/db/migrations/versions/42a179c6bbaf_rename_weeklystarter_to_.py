"""-Rename weeklystarter to leagueweeklyteam and ref playerseason from playerweekspn

Revision ID: 42a179c6bbaf
Revises: e44c1e45bc71
Create Date: 2025-02-22 16:24:47.362681

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "42a179c6bbaf"
down_revision: Union[str, None] = "e44c1e45bc71"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.rename_table("weekly_starter", "league_weekly_team")
    op.add_column(
        "player_week_espn", sa.Column("player_season_id", sa.Integer(), nullable=True)
    )
    op.create_foreign_key(
        "player_week_espn_player_season_id_fkey",
        "player_week_espn",
        "player_season",
        ["player_season_id"],
        ["player_season_id"],
    )

    op.execute(
        """
    UPDATE player_week_espn pwe
    SET player_season_id = ps.player_season_id
    FROM player_season ps
    WHERE pwe.player_id = ps.player_id
    AND pwe.season = ps.season
    """
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute(
        """
    UPDATE player_week_espn pwe
    SET player_season_id = NULL"""
    )

    op.drop_constraint(
        "player_week_espn_player_season_id_fkey", "player_week_espn", type_="foreignkey"
    )
    op.drop_column("player_week_espn", "player_season_id")
    op.rename_table("league_weekly_team", "weekly_starter")
    # ### end Alembic commands ###
