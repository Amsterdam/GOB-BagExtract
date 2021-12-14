import datetime
from unittest import TestCase
from unittest.mock import patch, Mock, call

from gobcore.enum import ImportMode
from gobconfig.datastore.config import TYPE_POSTGRES

from gobbagextract.config import DATABASE_CONFIG
from gobbagextract.prepare.prepare_client import PrepareClient


class TestPrepareClient(TestCase):

    @patch('gobbagextract.prepare.prepare_client.PostgresDatastoreExt')
    @patch('gobbagextract.prepare.prepare_client.BagExtractDatastore')
    def test_init(self, mock_bagextractdatastore, mock_postgres_ds):
        dataset = {
            'catalogue': 'bag',
            'entity': 'ENT',
            'source': {
                'read_config': {
                    'object_type': 'object_type',
                    'xml_object': 'xml_object',
                    'mode': 'modes',
                    'gemeentes': 'gemeentes',
                    'download_location': 'download_location'
                }
            }
        }
        msg = {'header': 'HEADER'}
        mode = ImportMode.FULL
        last_date = datetime.datetime.now().date()
        read_config = dataset['source']['read_config']
        PrepareClient(msg, dataset, mode, last_date)
        mock_bagextractdatastore.assert_called_with({}, read_config, last_date)
        mock_postgres_ds.assert_called_once()
        ds_config = DATABASE_CONFIG | {'type': TYPE_POSTGRES}
        ds_config.pop('drivername')
        mock_postgres_ds.assert_called_with(ds_config)

    @patch('gobbagextract.prepare.prepare_client.DatastoreToPostgresSelector')
    def test_import_data(self, mock_ds_to_postgres_selector):
        client = Mock()
        nr_rows = 10
        selector = Mock()
        selector.select = Mock(return_value=nr_rows)
        mock_ds_to_postgres_selector.return_value = selector

        PrepareClient.import_dataset(client)

        client.assert_has_calls(
            [
                call._data_src.connect(),
                call._data_dst.connect(),
                call.get_result_msg(nr_rows),
                call._data_src.disconnect(),
                call._data_dst.disconnect()
            ]
        )
        selector.select.assert_called_once()

    @patch('gobbagextract.prepare.prepare_client.logger')
    def test_get_result_message(self, mock_logger):
        client = Mock()
        client.header = {'cataloge': 'bag'}
        client.dataset = {'version': 1}
        nr_rows = 20
        mock_logger.get_summary.return_value = {'summary': 'a summary'}
        ret = PrepareClient.get_result_msg(client, nr_rows)
        mock_logger.info.assert_called_once()
        mock_logger.get_summary.assert_called_once()
        self.assertEqual(set(ret.keys()), {'header', 'summary'})
