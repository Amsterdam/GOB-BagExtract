import datetime
from pathlib import Path
from typing import Generator
from unittest import mock
from unittest.mock import MagicMock

import freezegun
import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from gobbagextract.__main__ import handle_bag_extract_message
from gobbagextract.database.model import MutationImport
from gobbagextract.database.repository import MutationImportRepository
from gobcore.enum import ImportMode


class TestHandleBagExtract:
    @pytest.fixture
    def gob_logger_mock(self) -> Generator[MagicMock, None, None]:
        """Mock GOB logger.

        GOB logger cannot be overwritten with logging.setLoggerClass as it is not compatible with that.
        :return: A generator which yields the mocked logger.
        """
        with mock.patch("gobbagextract.__main__.logger") as p:
            yield p

    @pytest.fixture
    def gob_logger_manager_mock(self) -> Generator[MagicMock, None, None]:
        with mock.patch("gobcore.logging.logger.LoggerManager") as p:
            yield p

    @pytest.fixture
    def mock_response_full_download(self, requests_mock, tests_dir):
        requests_mock.post(
            "https://kadaster.nl/productstore/download/09ec66c1-ca01-4b5a-a2e6-7d90d62cb2b2",
            content=Path(tests_dir, "fixtures", "bag_data", "BAGGEM0457L-15102021.zip").read_bytes()
        )

    @pytest.fixture
    def mock_response_mutations_download(self, requests_mock, tests_dir):
        requests_mock.post(
            "https://kadaster.nl/productstore/download/02b9b15c-3051-4af3-8714-9a3325bf69fa",
            content=Path(tests_dir, "fixtures", "bag_data", "BAGNLDM-13112021-14112021.zip").read_bytes()
        )

    @freezegun.freeze_time("2021-11-14")
    def test_handle_bag_extract_message_inserts_mutation(
            self, database: Session, gob_logger_mock: MagicMock, gob_logger_manager_mock,
            mock_response_mutaties: None, mock_response_mutations_download: None, mock_response_full_download: None
    ):
        message = {
            "header": {
                "catalogue": "bag",
                "collection": "ligplaatsen",
            }
        }
        repo = MutationImportRepository(database)
        obj = MutationImport(
            mode=ImportMode.MUTATIONS.value,
            filename="BAGNLDM-13112021-14112021.zip",
            application="BagExtract",
            catalogue="bag",
            collection="ligplaatsen",
            started_at=datetime.datetime.now() + datetime.timedelta(hours=1),
        )
        updated_obj = repo.save(obj)

        repo.session.commit()
        rows = database.execute(text("SELECT * FROM bag_inonderzoek"))
        assert rows.rowcount == 0
        handle_bag_extract_message(message)
        # Check if mutation is inserted in the database
        assert repo.get(updated_obj.id).filename == obj.filename
        # Check if no warnings or errors where logged.
        assert gob_logger_mock.get_warnings.call_count == 0
        assert gob_logger_mock.get_errrors.call_count == 0

        rows = database.execute(text("SELECT * FROM bag_inonderzoek"))
        assert rows.rowcount > 0

    @freezegun.freeze_time("2021-11-14")
    def test_handle_bag_extract_message_full_inserts_inonderzoek(
            self, database: Session, gob_logger_mock: MagicMock, gob_logger_manager_mock,
            mock_response_full: None, mock_response_full_download: None
    ):
        message = {
            "header": {
                "catalogue": "bag",
                "collection": "inonderzoek",
            }
        }
        repo = MutationImportRepository(database)
        obj = MutationImport(
            mode=ImportMode.FULL.value,
            filename="BAGGEM0457L-15102021.zip",
            application="BagExtract",
            catalogue="bag",
            collection="inonderzoek",
            started_at=datetime.datetime.now(),
            # ended_at=datetime.datetime.now() + datetime.timedelta(hours=1)
        )
        repo.save(obj)
        repo.session.commit()
        rows = database.execute(text("SELECT * FROM bag_inonderzoek"))
        assert rows.rowcount == 0
        handle_bag_extract_message(message)

        rows = database.execute(text("SELECT * FROM bag_inonderzoek"))
        assert rows.rowcount > 0

    @freezegun.freeze_time("2021-11-14")
    def test_handle_bag_extract_message_mutation_inserts_inonderzoek(
            self, database: Session, gob_logger_mock: MagicMock, gob_logger_manager_mock,
            mock_response_mutaties: None, mock_response_mutations_download: None
    ):
        message = {
            "header": {
                "catalogue": "bag",
                "collection": "inonderzoek",
            }
        }
        repo = MutationImportRepository(database)
        obj = MutationImport(
            mode=ImportMode.MUTATIONS.value,
            filename="BAGNLDM-13112021-14112021.zip",
            application="BagExtract",
            catalogue="bag",
            collection="inonderzoek",
            started_at=datetime.datetime.now() + datetime.timedelta(hours=1),
        )
        repo.save(obj)
        repo.session.commit()
        rows = database.execute(text("SELECT * FROM bag_inonderzoek"))
        assert rows.rowcount == 0
        handle_bag_extract_message(message)
        rows = database.execute(text("SELECT * FROM bag_inonderzoek"))
        assert rows.rowcount > 0
