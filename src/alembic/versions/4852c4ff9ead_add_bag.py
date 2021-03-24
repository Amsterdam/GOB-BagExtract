"""Add ligplaatsen model

Revision ID: 4852c4ff9ead
Revises:
Create Date: 2021-02-04 14:10:49.026184

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4852c4ff9ead'
down_revision = '8a5295fa3641'
branch_labels = None
depends_on = None

TABLES = (
    'bag_ligplaatsen',
    'bag_nummeraanduidingen',
    'bag_openbareruimtes',
    'bag_panden',
    'bag_standplaatsen',
    'bag_verblijfsobjecten',
    'bag_woonplaatsen',
)


def upgrade():
    for table_name in TABLES:
        op.create_table(
            table_name,
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column("gemeente", sa.String(), nullable=False),
            sa.Column("object_id", sa.String(), nullable=False),
            sa.Column("last_update", sa.Date(), nullable=False),
            sa.Column("object", sa.JSON()),
            sa.PrimaryKeyConstraint('id')
        )
        # Assume object_id is globally unique (for all gemeentes)
        op.create_index(op.f(f'ix_{table_name}_object_id'), table_name, ['object_id'], unique=True)
        op.create_index(op.f(f'ix_{table_name}_gemeente_last_update'), table_name, ['gemeente', 'last_update'])


def downgrade():
    for table_name in TABLES:
        op.drop_table(table_name)
        op.drop_index(op.f('ix_{table_name}_object_id'), table_name='mutation_import')
