from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import freezegun
from sqlalchemy.orm import Session

from gobbagextract.__main__ import handle_bag_extract_message
from gobbagextract.database.model import MutationImport
from gobbagextract.database.repository import MutationImportRepository
from gobcore.enum import ImportMode


class TestHandleBagExtract:

    def test_handle_bag_extract_message_inserts_mutation(self, database: Session, gob_logger_mock: MagicMock):
        message = {
            "header": {
                "catalogue": "bag",
                "collection": "ligplaatsen",
            }
        }
        repo = MutationImportRepository(database)
        obj = MutationImport(
            mode=ImportMode.MUTATIONS.value,
            filename="data/somefile.json",
        )
        updated_obj = repo.save(obj)
        handle_bag_extract_message(message)
        # Check if mutation is inserted in the database
        assert repo.get(updated_obj.id).filename == obj.filename
        # Check if no warnings or errors where logged.
        assert gob_logger_mock.get_warnings.call_count == 0
        assert gob_logger_mock.get_errrors.call_count == 0

    @freezegun.freeze_time("2021-09-16")
    def test_handle_import_msg_mutations(self, database: Session, gob_logger_mock: MagicMock, requests_mock, tests_dir):
        with mock.patch("gobcore.logging.logger.Logger") as p:
            dir_html = Path(tests_dir, "fixtures", "bag_data", "dir_index.html").read_text()
            requests_mock.get(
                "https://extracten.bag.kadaster.nl/lvbag/extracten/Gemeente%20LVC/0457/BAGGEM0457L-15092021.zip",
                content= Path(tests_dir, "fixtures", "bag_data", "BAGGEM0457L-15092021.zip").read_bytes()
            )
            requests_mock.get(
                "https://extracten.bag.kadaster.nl/lvbag/extracten/Gemeente%20LVC/0457/",
                text=dir_html
            )
            message = {
                "header": {
                    "catalogue": "bag",
                    "collection": "ligplaatsen",
                }
            }
            # hoe kan het dat onderstaande al een last_import heeft? Is die db niet leeg misschien?
            print(list(database.query(MutationImport).filter_by()))
            handle_bag_extract_message(message)

    #
    #     dataset = {
    #         'application': 'APP NAME',
    #         'catalogue': 'CAT',
    #         'dataset_file': 'data/fromheader.json',
    #         'entity': 'ENT',
    #         'source': {'application': 'APP NAME'},
    #     }
    #     mock_mutations_handler = Mock()
    #
    #     mocked_last_import = MutationImport()
    #     mocked_next_import = MutationImport()
    #     mocked_next_import.id = 42
    #     mocked_next_import.mode = ImportMode.MUTATIONS
    #
    #     mock_repo.return_value.get_last.return_value = mocked_last_import
    #
    #     updated_dataset = "UPDATED DATASET"
    #     date = datetime.now().date()
    #     mock_mutations_handler.get_next_import.return_value = (mocked_next_import, updated_dataset, date)
    #
    #     _handle_mutation_import(self.mock_msg, dataset, mock_mutations_handler)
    #
    #     mock_repo.return_value.get_last.assert_called_with('CAT', 'ENT', 'APP NAME')
    #     mock_repo.return_value.save.assert_called_with(mocked_next_import)
    #
    #     mock_client.assert_called_with(self.mock_msg, updated_dataset, ImportMode.MUTATIONS, date)
