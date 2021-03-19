import datetime
from unittest import TestCase
from unittest.mock import patch, Mock

from gobcore.enum import ImportMode

from gobbagextract.__main__ import \
    SERVICEDEFINITION, handle_bag_extract_message, NothingToDo, _handle_mutation_import, \
    BAG_EXTRACT, _extract_dataset_from_msg, GOBException, main

from gobbagextract.database.model import MutationImport


class TestMain(TestCase):

    def setUp(self):
        self.mock_msg = {
            'dataset_file': 'data/somefile.json',
            'header': {
                'dataset_file': 'data/fromheader.json',
            },
        }

    @patch("gobbagextract.__main__.sys")
    @patch("gobbagextract.__main__.connect")
    @patch("gobbagextract.__main__.messagedriven_service")
    def test_init___main__(self, mock_messagedriven_service, mock_connect, mock_sys):
        mock_sys.argv = ['arg0']
        from gobbagextract import __main__ as module

        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_once_with(SERVICEDEFINITION, "BagExtract")

    @patch("gobbagextract.__main__.sys")
    @patch("gobbagextract.__main__.main")
    def test_init_wth_args(self, mock_main, mock_sys):
        mock_sys.argv = ['arg0', 'COL']
        from gobbagextract import __main__ as module

        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_main.assert_called_once_with('COL')

    @patch("gobbagextract.__main__.logger")
    @patch("gobbagextract.__main__._handle_mutation_import")
    @patch("gobbagextract.__main__.MutationsHandler")
    @patch("gobbagextract.__main__._extract_dataset_from_msg")
    def test_main(
            self, mock_extract_dataset, mock_mutations_handler, mock_handle_mutation_import, logger):
        collection = 'COL'
        dataset = {
            'application': 'APP NAME',
            'catalogue': 'CAT',
            'dataset_file': 'data/fromheader.json',
            'entity': 'ENT',
            'source': {'application': 'APP NAME'},
        }
        exp_msg = {'header': {'catalogue': 'bag', 'collection': collection}}
        mock_mutations_handler.return_value = 'MUTATIONS_HANLDER'
        mock_extract_dataset.return_value = dataset
        mock_handle_mutation_import.side_effect = [(exp_msg, True), (exp_msg, False)]
        main(collection)
        mock_mutations_handler.assert_called_once_with(dataset)
        mock_handle_mutation_import.assert_called_with(exp_msg, dataset, mock_mutations_handler.return_value)
        self.assertEqual(mock_handle_mutation_import.call_count, 2)

    @patch("gobbagextract.__main__.start_workflow")
    @patch("gobbagextract.__main__._handle_mutation_import")
    @patch("gobbagextract.__main__.MutationsHandler")
    @patch("gobbagextract.__main__.logger")
    @patch("gobbagextract.__main__._extract_dataset_from_msg")
    def test_handle_bag_extract_message(
            self, mock_extract_dataset, mock_logger, mock_mutations_handler, mock_handle_mutation_import,
            mock_start_workflow):

        dataset = {
            "source": {
                "name": "Some name",
                "application": "APP NAME",
            },
            "catalogue": "CAT",
            "entity": "ENT"
        }

        mock_handle_mutation_import.return_value = self.mock_msg, True
        mock_extract_dataset.return_value = dataset

        mocked_next_import = MutationImport()
        mocked_next_import.id = 42
        mocked_next_import.mode = ImportMode.MUTATIONS

        updated_dataset = "UPDATED DATASET"
        date = datetime.datetime.now().date()
        mock_mutations_handler.return_value.get_next_import.return_value = (mocked_next_import, updated_dataset, date)
        msg = handle_bag_extract_message(self.mock_msg)

        result_msg = {
            'dataset_file': 'data/somefile.json',
            'header': {
                'application': 'APP NAME',
                'catalogue': 'CAT',
                'dataset_file': 'data/fromheader.json',
                'entity': 'ENT',
                'source': 'Some name',
            }
        }
        mock_handle_mutation_import.assert_called_with(
                result_msg, mock_extract_dataset.return_value, mock_mutations_handler())

        self.assertEqual(result_msg, self.mock_msg)
        self.assertEqual(msg, self.mock_msg)

        mock_start_workflow.assert_called_once()
        mock_start_workflow.assert_called_with(
            {'workflow_name': BAG_EXTRACT}, {'catalogue': 'CAT', 'application': 'APP NAME', 'entity': 'ENT'})

        # No mutations left
        mock_handle_mutation_import.return_value = self.mock_msg, False
        handle_bag_extract_message(self.mock_msg)
        mock_logger.info.assert_called_with("This was the last file to be exctracted for now.")

    @patch("gobbagextract.__main__.PrepareClient")
    @patch("gobbagextract.__main__.DatabaseSession")
    @patch("gobbagextract.__main__.MutationImportRepository")
    @patch("gobbagextract.__main__.logger")
    def test_handle_import_msg_mutations(self, mock_logger, mock_repo, mock_session, mock_client):

        dataset = {
            'application': 'APP NAME',
            'catalogue': 'CAT',
            'dataset_file': 'data/fromheader.json',
            'entity': 'ENT',
            'source': {'application': 'APP NAME'},
        }
        mock_mutations_handler = Mock()

        mocked_last_import = MutationImport()
        mocked_next_import = MutationImport()
        mocked_next_import.id = 42
        mocked_next_import.mode = ImportMode.MUTATIONS

        mock_repo.return_value.get_last.return_value = mocked_last_import

        updated_dataset = "UPDATED DATASET"
        date = datetime.datetime.now().date()
        mock_mutations_handler.get_next_import.return_value = (mocked_next_import, updated_dataset, date)

        _handle_mutation_import(self.mock_msg, dataset, mock_mutations_handler)

        mock_repo.return_value.get_last.assert_called_with('CAT', 'ENT', 'APP NAME')
        mock_repo.return_value.save.assert_called_with(mocked_next_import)

        mock_client.assert_called_with(self.mock_msg, updated_dataset, ImportMode.MUTATIONS, date)

    @patch("gobbagextract.__main__.DatabaseSession")
    @patch("gobbagextract.__main__.MutationImportRepository")
    @patch("gobbagextract.__main__.logger")
    def test_handle_import_msg_mutations_nothing_to_do(self, mock_logger, mock_repo, mock_session):
        mock_mutations_handler = Mock()
        mock_mutations_handler.get_next_import.side_effect = NothingToDo()
        dataset = {'header': 'bello'}
        ret = _handle_mutation_import(self.mock_msg, dataset, mock_mutations_handler)
        self.assertEqual(ret[1], False)

    @patch("gobbagextract.__main__.get_extract_definition")
    def test_extract_data_from_msg(self, mock_extract_definition):
        msg = {'header': {'catalogue': 'bag', 'collection': 'panden'}}
        mock_extract_definition.return_value = 'RET'
        ret = _extract_dataset_from_msg(msg)
        self.assertEqual(ret, 'RET')
        mock_extract_definition.assert_called_with('bag', 'panden')
        msg = {'header': {'collection': 'panden'}}
        self.assertRaises(GOBException, _extract_dataset_from_msg, msg)
