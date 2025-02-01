"""Singularize table names

Revision ID: 9516771f1726
Revises: 29df909e2352
Create Date: 2025-02-01 00:30:34.872939

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9516771f1726"
down_revision: Union[str, None] = "29df909e2352"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("players", "player")
    op.rename_table("teams", "team")
    op.rename_table("games", "game")
    op.rename_table("team_names", "team_name")
    # Add more rename operations as needed


def downgrade() -> None:
    op.rename_table("player", "players")
    op.rename_table("team", "teams")
    op.rename_table("game", "games")
    op.rename_table("team_name", "team_names")
    # Add more rename operations as needed
