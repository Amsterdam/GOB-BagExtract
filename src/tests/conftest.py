import os
from pathlib import Path
from typing import Generator
from unittest import mock
from unittest.mock import MagicMock

import pytest
from alembic.command import upgrade as alembic_upgrade
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, Session

from gobbagextract.config import DATABASE_CONFIG
from gobbagextract.database import connection
from gobbagextract.database.model import Base
from gobbagextract.extract_config import extract_config


@pytest.fixture
def app_dir() -> Path:
    """Returns directory which contains the app source."""
    return Path(__file__).parent.parent


@pytest.fixture
def tests_dir() -> Path:
    """Returns directory which contains tests. Used to find files required for tests."""
    return Path(__file__).parent


@pytest.fixture(autouse=True)
def set_bag_data_config(tests_dir: Path) -> Generator[None, None, None]:
    """Invalidate cached location mapping cache, set fixture dir.

    Cache should be invalidated to avoid leaking state between tests.
    """
    extract_config.data_set_locations_mapping_cache = None
    os.environ["BAG_DATA_CONFIG"] = str(tests_dir / "fixtures" / "bag_data")
    yield
    extract_config.data_set_locations_mapping_cache = None


@pytest.fixture
def reset_db() -> None:
    """Drop test database and recreate it to ensure the database is empty."""
    test_db_name = f"test_{DATABASE_CONFIG['database']}"
    DATABASE_CONFIG['database'] = test_db_name
    tmp_config = DATABASE_CONFIG.copy()
    # Cannot drop the currently open database, so do not open it.
    tmp_config.pop("database")
    engine_tmp: Engine = create_engine(URL(**tmp_config), echo=True)
    with engine_tmp.connect() as conn:
        conn.execute("commit")
        conn.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        conn.execute("commit")
        conn.execute(f"CREATE DATABASE {test_db_name}")
        conn.execute("commit")


@pytest.fixture
def database(app_dir: Path, reset_db: None) -> Generator[Session, None, None]:
    """Fixture which sets up the database, returns a db session.

    :param app_dir: path to current application source.
    :param reset_db: fixture which resets the test database.
    :return: a generator which yields a db session.
    """
    engine: Engine = create_engine(URL(**DATABASE_CONFIG), echo=True)
    session_factory = sessionmaker(bind=engine)
    session: Session = session_factory()

    # Migrate the database
    alembic_config = AlembicConfig(app_dir / "alembic.ini")
    alembic_config.set_main_option('script_location', str(app_dir / "alembic"))
    alembic_upgrade(alembic_config, 'head')

    # Set global variables to make the app work
    Base.metadata.bind = engine
    connection.session = session
    connection.engine = engine
    yield session
    engine.dispose()


@pytest.fixture
def gob_logger_mock() -> Generator[MagicMock, None, None]:
    """Mock GOB logger.

    GOB logger cannot be overwritten with logging.setLoggerClass as it is not compatible with that.
    :return: A generator which yields the mocked logger.
    """
    with mock.patch("gobbagextract.__main__.logger") as p:
        yield p
