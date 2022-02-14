import json
import os
from pathlib import Path
from typing import Generator

import pytest
from alembic.command import upgrade as alembic_upgrade
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker, Session

from gobbagextract.config import DATABASE_CONFIG, KADASTER_PRODUCTSTORE_AFGIFTE_URL
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
    try:
        yield
    finally:
        extract_config.data_set_locations_mapping_cache = None


@pytest.fixture
def mock_config() -> dict:
    return json.loads(Path(os.environ["BAG_DATA_CONFIG"], "bag.test.json").read_text())


@pytest.fixture
def recreate_database() -> str:
    """Drop test database and recreate it to ensure the database is empty.

    :returns: de test database name it created.
    """
    test_db_name = f"test_{DATABASE_CONFIG['database']}"
    tmp_config = DATABASE_CONFIG.copy()
    # Cannot drop the currently open database, so do not open it.
    tmp_config.pop("database")
    engine_tmp: Engine = create_engine(URL(**tmp_config), echo=True)
    try:
        print("TRYING")
        with engine_tmp.connect() as conn:
            conn.execute("commit")
            conn.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
            conn.execute("commit")
            conn.execute(f"CREATE DATABASE {test_db_name}")
            conn.execute("commit")
    except OperationalError as e:
        raise Exception(
            "Could not connect to test bag database. "
            "Is the bagextract_database container running?"
        ) from e
    return test_db_name


@pytest.fixture
def database(app_dir: Path, recreate_database) -> Generator[Session, None, None]:
    """Fixture which sets up the database, returns a db session.

    :param app_dir: path to current application source.
    :param recreate_database: fixture which resets the test database.
    :return: a generator which yields a db session.
    """
    test_db_name = recreate_database
    DATABASE_CONFIG["database"] = test_db_name
    engine: Engine = create_engine(URL(**DATABASE_CONFIG), echo=True)
    session_factory = sessionmaker(bind=engine)
    session: Session = session_factory()

    # Migrate the database
    alembic_config = AlembicConfig(app_dir / "alembic.ini")
    alembic_config.set_main_option("script_location", str(app_dir / "alembic"))
    alembic_upgrade(alembic_config, "head")

    # Set global variables to make the app work
    Base.metadata.bind = engine
    connection.session = session
    connection.engine = engine
    try:
        yield session
    finally:
        engine.dispose()


@pytest.fixture
def mock_kadaster_request(tests_dir) -> str:
    xml = tests_dir / "fixtures" / "xml" / "request.xml"
    return xml.read_text()


@pytest.fixture
def mock_response_full(tests_dir, requests_mock):
    xml = tests_dir / "fixtures" / "xml" / "response_full.xml"
    requests_mock.post(KADASTER_PRODUCTSTORE_AFGIFTE_URL, text=xml.read_text())


@pytest.fixture
def mock_response_mutaties(tests_dir, requests_mock):
    xml = tests_dir / "fixtures" / "xml" / "response_mutaties.xml"
    requests_mock.post(KADASTER_PRODUCTSTORE_AFGIFTE_URL, text=xml.read_text())


@pytest.fixture
def mock_response_empty(tests_dir, requests_mock):
    xml = tests_dir / "fixtures" / "xml" / "response_empty.xml"
    requests_mock.post(KADASTER_PRODUCTSTORE_AFGIFTE_URL, text=xml.read_text())


@pytest.fixture
def mock_response_error(tests_dir, requests_mock):
    xml = tests_dir / "fixtures" / "xml" / "response_error.xml"
    requests_mock.post(KADASTER_PRODUCTSTORE_AFGIFTE_URL, text=xml.read_text())
