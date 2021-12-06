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


@pytest.fixture
def app_dir() -> Path:
    """Returns directory which contains tests. Used to find files required for tests."""
    return Path(__file__).parent.parent


@pytest.fixture
def database(app_dir: Path) -> Generator[Session, None, None]:
    """Fixture which sets up the database, returns a db session.

    :param app_dir: path to current application source.
    :return: a generator which yields a db session.
    """
    engine: Engine = create_engine(URL(**DATABASE_CONFIG), echo=True)
    session_factory = sessionmaker(bind=engine)

    # Migrate the database
    alembic_config = AlembicConfig(app_dir / "alembic.ini")
    alembic_config.set_main_option('script_location', str(app_dir / "alembic"))
    alembic_upgrade(alembic_config, 'head')

    # Set global variables to make the app work
    session: Session = session_factory()
    Base.metadata.bind = engine
    connection.session = session
    connection.engine = engine
    yield session
    engine.dispose()


@pytest.fixture
def disable_gob_logger() -> Generator[MagicMock, None, None]:
    """Mock GOB logger.

    GOB logger cannot be overwritten with logging.setLoggerClass as it is not compatible with that.
    :return: the mocked logger.
    """
    with mock.patch("gobbagextract.__main__.logger") as p:
        yield p
