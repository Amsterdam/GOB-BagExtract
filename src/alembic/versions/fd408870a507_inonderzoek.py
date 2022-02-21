"""inonderzoek

Revision ID: fd408870a507
Revises: 4852c4ff9ead
Create Date: 2022-02-18 11:20:46.981861

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fd408870a507'
down_revision = '4852c4ff9ead'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "bag_inonderzoek",
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column("gemeente", sa.String(), nullable=False),
        sa.Column("object_id", sa.String(), nullable=False),
        sa.Column("last_update", sa.Date(), nullable=False),
        sa.Column("object", sa.JSON()),
        sa.PrimaryKeyConstraint('id')
    )
    # Assume object_id is globally unique (for all gemeentes)
    op.create_index(op.f(f'ix_{"bag_inonderzoek"}_object_id'), "bag_inonderzoek", ['object_id'], unique=True)
    op.create_index(op.f(f'ix_{"bag_inonderzoek"}_gemeente_last_update'), "bag_inonderzoek", ['gemeente', 'last_update'])


def downgrade():
    op.drop_table("bag_inonderzoek")
    op.drop_index(op.f('ix_{"bag_inonderzoek"}_object_id'), table_name='mutation_import')
