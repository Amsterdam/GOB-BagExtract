from datetime import datetime
from unittest import TestCase
from unittest.mock import patch, Mock

from dateutil import relativedelta
from freezegun import freeze_time

from gobbagextract.__main__ import \
    SERVICEDEFINITION, handle_bag_extract_message, NothingToDo, _handle_mutation_import, \
    _log_no_more_left, _validate_message
from gobbagextract.config import BAGEXTRACT_NOT_AVAIL_DAYS_ERROR, BAGEXTRACT_NOT_AVAIL_DAYS_WARNING
from gobbagextract.database.model import MutationImport
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException


class TestMain(TestCase):

    def setUp(self):
        self.mock_msg = {
            "dataset_file": "data/somefile.json",
            "header": {
                "catalogue": "bag",
                "collection": "ligplaatsen",
            },
        }

    @patch("gobbagextract.__main__.sys")
    @patch("gobbagextract.__main__.connect")
    @patch("gobbagextract.__main__.messagedriven_service")
    def test_init___main__(self, mock_messagedriven_service, mock_connect, mock_sys):
        mock_sys.argv = ["arg0"]
        from gobbagextract import __main__ as module

        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_once_with(SERVICEDEFINITION, "BagExtract")

    @patch("gobbagextract.__main__.sys")
    @patch("gobbagextract.__main__.handle_bag_extract_message")
    def test_init_wth_args(self, mock_handle_bag_extract_message, mock_sys):
        collection = "COL"
        mock_sys.argv = ["arg0", collection]
        msg = {
            "header": {
                "catalogue": "bag",
                "collection": collection,
            }
        }
        from gobbagextract import __main__ as module

        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_handle_bag_extract_message.assert_called_once_with(msg)

    @patch("gobbagextract.__main__._handle_mutation_import")
    @patch("gobbagextract.__main__.MutationsHandler")
    @patch("gobbagextract.__main__.logger")
    def test_handle_bag_extract_message(
            self,  mock_logger, mock_mutations_handler, mock_handle_mutation_import):
        mock_handle_mutation_import.side_effect = (self.mock_msg, True), (self.mock_msg, False)
        mocked_next_import = MutationImport()
        mocked_next_import.id = 42
        mocked_next_import.mode = ImportMode.MUTATIONS

        updated_dataset = "UPDATED DATASET"
        date = datetime.now().date()
        mock_mutations_handler.return_value.get_next_import.return_value = (mocked_next_import, updated_dataset, date)
        msg = handle_bag_extract_message(self.mock_msg)

        result_msg = {
            "dataset_file": "data/somefile.json",
            "header": {
                "application": "BAGExtract",
                "catalogue": "bag",
                "collection": "ligplaatsen",
                "entity": "ligplaatsen",
                "source": "Kadaster",
            }
        }
        self.assertEqual(result_msg, self.mock_msg)
        self.assertEqual(msg, self.mock_msg)
        mock_logger.info.assert_called_with("This was the last file to be exctracted for now.")

    @patch("gobbagextract.__main__.PrepareClient")
    @patch("gobbagextract.__main__.DatabaseSession")
    @patch("gobbagextract.__main__.MutationImportRepository")
    @patch("gobbagextract.__main__.logger")
    def test_handle_import_msg_mutations(self, mock_logger, mock_repo, mock_session, mock_client):

        dataset = {
            "application": "APP NAME",
            "catalogue": "CAT",
            "dataset_file": "data/fromheader.json",
            "entity": "ENT",
            "source": {"application": "APP NAME"},
        }
        mock_mutations_handler = Mock()

        mocked_last_import = MutationImport()
        mocked_next_import = MutationImport()
        mocked_next_import.id = 42
        mocked_next_import.mode = ImportMode.MUTATIONS

        mock_repo.return_value.get_last.return_value = mocked_last_import

        updated_dataset = "UPDATED DATASET"
        date = datetime.now().date()
        mock_mutations_handler.get_next_import.return_value = (mocked_next_import, updated_dataset, date)

        _handle_mutation_import(self.mock_msg, dataset, mock_mutations_handler)

        mock_repo.return_value.get_last.assert_called_with("CAT", "ENT", "APP NAME")
        mock_repo.return_value.save.assert_called_with(mocked_next_import)

        mock_client.assert_called_with(self.mock_msg, updated_dataset, ImportMode.MUTATIONS, date)

    @patch("gobbagextract.__main__.DatabaseSession")
    @patch("gobbagextract.__main__.MutationImportRepository")
    @patch("gobbagextract.__main__.logger")
    @patch("gobbagextract.__main__._log_no_more_left")
    def test_handle_import_msg_mutations_nothing_to_do(self, _log_no_more_left, mock_logger, mock_repo, mock_session):
        mock_mutations_handler = Mock()
        mock_mutations_handler.get_next_import.side_effect = NothingToDo()
        dataset = {"header": "bello"}
        msg, last = _handle_mutation_import(self.mock_msg, dataset, mock_mutations_handler)
        summary = mock_logger.get_summary()
        self.assertEqual(mock_logger.info.call_count, 2)
        self.assertEqual(last, False)
        self.assertEqual(msg, {"header": self.mock_msg["header"], "summary": summary})
        _log_no_more_left.assert_called_once()

    def test_validate_message_required_keys(self):
        msg = {"header": {"catalogue": "bag", "collection": "panden"}}
        _validate_message(msg)

    def test_validate_message_missing_cataluge(self):
        msg = {"header": {"collection": "panden"}}
        self.assertRaises(GOBException, _validate_message, msg)

    def test_validate_message_missing_header(self):
        msg = {"not_header": {}}
        self.assertRaises(GOBException, _validate_message, msg)

    @patch("gobbagextract.__main__.logger")
    @freeze_time("2013-04-09")
    def test_log_no_more_left(self, mock_logger):
        mock_logger.error = Mock()
        mock_logger.info = Mock()
        mock_logger.warning = Mock()
        _log_no_more_left(None)
        mock_logger.error.assert_called_once()
        last_import = Mock()
        last_import.ended_at = datetime.now() - relativedelta.relativedelta(days=BAGEXTRACT_NOT_AVAIL_DAYS_ERROR + 1)
        mock_logger.error.reset_mock()
        _log_no_more_left(last_import)
        mock_logger.info.assert_called_once()
        mock_logger.error.assert_called_once()
        last_import.ended_at = datetime.now() - relativedelta.relativedelta(days=BAGEXTRACT_NOT_AVAIL_DAYS_WARNING + 1)
        mock_logger.error.reset_mock()
        _log_no_more_left(last_import)
        mock_logger.warning.assert_called_once()
        mock_logger.error.assert_not_called()
