from __future__ import with_statement

import sys

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine.url import URL
from logging.config import fileConfig


sys.path.append('.')
from gobbagextract.database.model import Base  # noqa
from gobbagextract.config import DATABASE_CONFIG  # noqa

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def include_object(object, name, type_, reflected, compare_to):
    skip_objects = [
        'spatial_ref_sys'
    ]
    return name not in skip_objects


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Register database URL
    ini_section = config.get_section(config.config_ini_section)
    ini_section['sqlalchemy.url'] = URL(**DATABASE_CONFIG)

    # Connect to database
    connectable = engine_from_config(
        ini_section,
        prefix='sqlalchemy.',
        poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    raise NotImplementedError
else:
    run_migrations_online()
