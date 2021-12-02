import alembic.config
import alembic.script
from alembic.runtime import migration
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from gobbagextract.database.model import Base
from gobbagextract.config import DATABASE_CONFIG

session = None
engine = None


def connect(force_migrate=False):
    """Module initialisation

    The connection with the underlying storage is initialised.
    Meta information is available via the Base variable.
    Data retrieval is facilitated via the session object

    :return: True when the connection has been established
    """
    global session, engine

    try:
        print('Connecting to DB', DATABASE_CONFIG)
        engine = create_engine(URL(**DATABASE_CONFIG), connect_args={'sslmode': 'require'})

        migrate_storage(force_migrate)

        # Declarative base model to create database tables and classes
        Base.metadata.bind = engine

        session = Session(engine)
    except DBAPIError as e:
        # Catch any connection errors
        print(f"Connect failed: {str(e)}")
        disconnect()  # Cleanup

    return is_connected()


def disconnect():
    """Disconnect from the database

    Cancel any running transactions and close the session and engine

    :return: None
    """
    global engine, session

    try:
        if session is not None:
            session.rollback()
            session.close()
        if engine is not None:
            engine.dispose()
    except DBAPIError as e:
        # Catch any connection errors
        print(f"Disconnect failed: {str(e)}")
    finally:
        engine = None
        session = None


def is_connected():
    """Is connected

    Tells whether the database connection is alive

    A simple statement is executed to test if the database communication is OK

    :return: True when the database connection is OK
    """
    if engine is None or session is None:
        return False
    else:
        try:
            session.execute("SELECT 1")
            return True
        except Exception:
            return False


def migrate_storage(force_migrate):
    """
    Migrate storage to latest version.

    In order to prevent that multiple instances will migrate at the same time
    and to prevent migration locks, access to this method is normally acquired
    by a lock.

    This method will always unlock the lock, even if a lock has not been set.
    When using the force_migrate option the lock is passed and any open lock will be released

    The reason for setting the lock is to prevent multiple migrations that might lock each other
    :return:
    """
    MIGRATION_LOCK = 24804454110  # Just some random number

    if not force_migrate:
        # Don't force
        # Nicely wait for any migrations to finish before continuing
        engine.execute(f"SELECT pg_advisory_lock({MIGRATION_LOCK})")

    try:
        # Check if storage is up-to-date
        alembic_cfg = alembic.config.Config('alembic.ini')
        script = alembic.script.ScriptDirectory.from_config(alembic_cfg)
        with engine.begin() as conn:
            context = migration.MigrationContext.configure(conn)
            up_to_date = context.get_current_revision() == script.get_current_head()

        if not up_to_date:
            print('Migrating storage')
            alembicArgs = [
                '--raiseerr',
                'upgrade', 'head',
            ]
            alembic.config.main(argv=alembicArgs)
    except Exception as e:
        print(f'Storage migration failed: {str(e)}')
    else:  # No exception
        print('Storage is up-to-date')

    # Always unlock
    engine.execute(f"SELECT pg_advisory_unlock({MIGRATION_LOCK})")
