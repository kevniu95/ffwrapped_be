"""Insert ESPN into platform table

Revision ID: 823540d93a0c
Revises: 9516771f1726
Create Date: 2025-02-01 15:56:54.383603

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "823540d93a0c"
down_revision: Union[str, None] = "9516771f1726"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("INSERT INTO platform (platform_name) VALUES ('ESPN')")


def downgrade() -> None:
    op.execute("DELETE FROM platform WHERE platform_name = 'ESPN'")
