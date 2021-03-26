from datetime import datetime

from gobcore.enum import ImportMode
from gobcore.logging.logger import logger
from gobconfig.datastore.config import TYPE_POSTGRES

from gobbagextract.config import DATABASE_CONFIG
from gobbagextract.datastore.postgres import PostgresDatastoreExt
from gobbagextract.selector.datastore_to_postgres import DatastoreToPostgresSelector
from gobbagextract.datastore.bag_extract import BagExtractDatastore


class PrepareClient:
    columns_def = [
            {'name': 'object_id', 'type': 'string'},
            {'name': 'gemeente', 'type': 'string'},
            {'name': 'last_update', 'type': 'datetime'},
            {'name': 'object', 'type': 'JSON'},
    ]

    def __init__(self, msg: dict, dataset, mode: ImportMode, last_date: datetime):
        self.dataset = dataset
        self.header = msg.get('header', {})
        self._laste_date = last_date
        self.entity = dataset['entity']
        read_config = dataset.get('source', {}).get('read_config', {})
        read_config['mode'] = mode
        self._data_src = BagExtractDatastore(dict(), read_config, last_date)
        data_store_config = DATABASE_CONFIG | {'type': TYPE_POSTGRES}
        data_store_config.pop('drivername')
        self._data_dst = PostgresDatastoreExt(data_store_config)
        self.source_app = self.dataset.get('source', {}).get('application')
        self._destination_table = '_'.join((dataset['catalogue'], dataset['entity']))
        self._config = {
            'destination_table': {
                'name': self._destination_table,
                'columns': self.columns_def,
            },
            'ignore_missing': False,
            'catalogue': dataset['catalogue'],
            'entity': dataset['entity'],
            'gemeente': read_config.get('gemeente'),
        }

    def connect(self):
        if self._data_src:
            self._data_src.connect()
        self._data_dst.connect()

    def disconnect(self):
        """Closes open database connections

        :return:
        """
        if self._data_src:
            self._data_src.disconnect()
        self._data_dst.disconnect()
        self._data_src = None
        self._data_dst = None

    def import_dataset(self) -> int:
        '''
           Return total number of imported elements
        '''

        self.connect()
        selector = DatastoreToPostgresSelector(self._data_src, self._data_dst, self._config)
        nr_rows = selector.select()
        ret = self.get_result_msg(nr_rows)
        self._data_dst.query(f'VACUUM FULL {self._destination_table};')
        self.disconnect()
        return ret

    def get_result_msg(self, nr_rows):
        """The result of the bag extract needs to be published.

        Publication includes a header, summary and results
        The header is for identification purposes
        The summary is for the interpretation of the results. Was the import successful, what er the metrics, etc
        The results is the imported data in GOB format

        :return:
        """
        header = {
            **self.header,
            "version": self.dataset['version'],
            "timestamp": datetime.utcnow().isoformat()
        }

        summary = {
            'num_records': nr_rows
        }

        # Log end of import process
        logger.info(f"Bag extract dataset {self.entity} from {self.source_app} completed. "
                    f"{summary['num_records']} records were read from the source.",
                    kwargs={"data": summary})

        summary.update(logger.get_summary())

        import_message = {
            "header": header,
            "summary": summary,
        }
        return import_message
