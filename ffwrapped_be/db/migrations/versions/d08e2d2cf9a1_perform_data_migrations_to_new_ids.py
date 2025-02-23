"""Perform data migrations to new ids

Revision ID: d08e2d2cf9a1
Revises: f29e320a3a83
Create Date: 2025-02-22 23:05:00.718647

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d08e2d2cf9a1"
down_revision: Union[str, None] = "f29e320a3a83"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
    UPDATE player_week pw
    SET player_season_id = ps.player_season_id
    FROM player_season ps
    WHERE pw.player_id = ps.player_id
    AND pw.season = ps.season
    """
    )

    op.execute(
        """
    UPDATE player_week_espn pwe
    SET player_week_id = pw.player_week_id
    FROM player_week pw
    WHERE pwe.player_season_id = pw.player_season_id
	  AND pwe.week = pw.week
    """
    )

    op.execute(
        """
    UPDATE league_weekly_team lwt
    SET player_week_id = pw.player_week_id
    FROM player_week pw
    WHERE lwt.player_id = pw.player_id
	  AND lwt.week = pw.week
    """
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    op.execute(
        """
    UPDATE league_weekly_team
    SET player_week_id = NULL
    """
    )

    op.execute(
        """
    UPDATE player_week_espn
    SET player_week_id = NULL
    """
    )

    op.execute(
        """
    UPDATE player_week
    SET player_season_id = NULL
    """
    )
    # ### end Alembic commands ###
