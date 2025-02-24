"""Re-create player_week_espn and leauge_weekly_team tables

Revision ID: 3c33325a4dfb
Revises: 61c16241e9ed
Create Date: 2025-02-23 16:27:40.174580

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "3c33325a4dfb"
down_revision: Union[str, None] = "61c16241e9ed"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "player_week_espn",
        sa.Column("player_season_id", sa.Integer(), nullable=True),
        sa.Column("week", sa.Integer(), nullable=False),
        sa.Column("passing_attempts", sa.Integer(), nullable=True),
        sa.Column("passing_completions", sa.Integer(), nullable=True),
        sa.Column("passing_yards", sa.Integer(), nullable=True),
        sa.Column("passing_touchdowns", sa.Integer(), nullable=True),
        sa.Column("passing_40yard_tds", sa.Integer(), nullable=True),
        sa.Column("passing_50yard_tds", sa.Integer(), nullable=True),
        sa.Column("passing_2pt_conversions", sa.Integer(), nullable=True),
        sa.Column("passing_interceptions", sa.Integer(), nullable=True),
        sa.Column("rushing_attempts", sa.Integer(), nullable=True),
        sa.Column("rushing_yards", sa.Integer(), nullable=True),
        sa.Column("rushing_touchdowns", sa.Integer(), nullable=True),
        sa.Column("rushing_40yard_tds", sa.Integer(), nullable=True),
        sa.Column("rushing_50yard_tds", sa.Integer(), nullable=True),
        sa.Column("rushing_2pt_conversions", sa.Integer(), nullable=True),
        sa.Column("receiving_targets", sa.Integer(), nullable=True),
        sa.Column("receiving_receptions", sa.Integer(), nullable=True),
        sa.Column("receiving_yards", sa.Integer(), nullable=True),
        sa.Column("receiving_touchdowns", sa.Integer(), nullable=True),
        sa.Column("receiving_40yard_tds", sa.Integer(), nullable=True),
        sa.Column("receiving_50yard_tds", sa.Integer(), nullable=True),
        sa.Column("receiving_2pt_conversions", sa.Integer(), nullable=True),
        sa.Column("fumbles", sa.Integer(), nullable=True),
        sa.Column("fumbles_lost", sa.Integer(), nullable=True),
        sa.Column("fumbles_recovered_for_td", sa.Integer(), nullable=True),
        sa.Column("passing_sacks", sa.Integer(), nullable=True),
        sa.Column("kicking_xpm", sa.Integer(), nullable=True),
        sa.Column("kicking_xpa", sa.Integer(), nullable=True),
        sa.Column("kicking_fgm_0_39", sa.Integer(), nullable=True),
        sa.Column("kicking_fga_0_39", sa.Integer(), nullable=True),
        sa.Column("kicking_fgm_40_49", sa.Integer(), nullable=True),
        sa.Column("kicking_fga_40_49", sa.Integer(), nullable=True),
        sa.Column("kicking_fgm_50_59", sa.Integer(), nullable=True),
        sa.Column("kicking_fga_50_59", sa.Integer(), nullable=True),
        sa.Column("kicking_fgm_60_plus", sa.Integer(), nullable=True),
        sa.Column("kicking_fga_60_plus", sa.Integer(), nullable=True),
        sa.Column("defensive_blocked_kick_return_tds", sa.Integer(), nullable=True),
        sa.Column("defensive_interceptions", sa.Integer(), nullable=True),
        sa.Column("defensive_fumble_recoveries", sa.Integer(), nullable=True),
        sa.Column("defensive_blocked_kicks", sa.Integer(), nullable=True),
        sa.Column("defensive_safeties", sa.Integer(), nullable=True),
        sa.Column("defensive_sacks", sa.Integer(), nullable=True),
        sa.Column("kickoff_return_touchdowns", sa.Integer(), nullable=True),
        sa.Column("punt_return_touchdowns", sa.Integer(), nullable=True),
        sa.Column("interception_return_touchdowns", sa.Integer(), nullable=True),
        sa.Column("fumble_return_touchdowns", sa.Integer(), nullable=True),
        sa.Column("defensive_forced_fumbles", sa.Integer(), nullable=True),
        sa.Column("defensive_assisted_tackles", sa.Integer(), nullable=True),
        sa.Column("defensive_solo_tackles", sa.Integer(), nullable=True),
        sa.Column("defensive_passes_defended", sa.Integer(), nullable=True),
        sa.Column("kickoff_return_yards", sa.Integer(), nullable=True),
        sa.Column("punt_return_yards", sa.Integer(), nullable=True),
        sa.Column("punts_returned", sa.Integer(), nullable=True),
        sa.Column("defensive_points_allowed", sa.Integer(), nullable=True),
        sa.Column("defensive_yards_allowed", sa.Integer(), nullable=True),
        sa.Column("defensive_2pt_return", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["player_season_id"],
            ["player_season.player_season_id"],
        ),
        sa.PrimaryKeyConstraint("week", "player_season_id"),
    )
    op.create_table(
        "league_weekly_team",
        sa.Column("league_team_id", sa.Integer(), nullable=False),
        sa.Column("player_season_id", sa.Integer(), nullable=True),
        sa.Column("week", sa.Integer(), nullable=False),
        sa.Column("lineup_position", sa.String(length=50), nullable=False),
        sa.ForeignKeyConstraint(
            ["league_team_id"],
            ["league_team.league_team_id"],
        ),
        sa.ForeignKeyConstraint(
            ["player_season_id"],
            ["player_season.player_season_id"],
        ),
        sa.PrimaryKeyConstraint("league_team_id", "player_season_id", "week"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("league_weekly_team")
    op.drop_table("player_week_espn")
    # ### end Alembic commands ###
