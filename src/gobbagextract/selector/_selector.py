import itertools
from typing import Iterator

from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.datastore.datastore import Datastore


class Selector:
    """
    Base Selector.

    Selector handles execution of queries on src_connection. The results of the queries are written to
    dst_database
    """
    WRITE_BATCH_SIZE = 50_000

    def __init__(self, src_datastore: Datastore, dst_datastore: Datastore, config: dict):
        """
        :param src_datastore:
        :param dst_datastore:
        :param config:
        """
        self._src_datastore = src_datastore
        self._dst_datastore = dst_datastore
        self._config = config
        self.destination_table = config['destination_table']
        self.ignore_missing = config.get('ignore_missing', False)
        self.query = self._config.get('query', '')

    def select(self) -> int:
        """Entry method. Saves result of select query in destination table."""
        total_cnt = 0
        table = self.destination_table['name']
        columns = self.destination_table['columns']

        while True:
            rows = self._read_rows(self.query)
            chunk = itertools.islice(rows, self.WRITE_BATCH_SIZE)
            values = self._process_values(chunk, columns)
            row_cnt = self._write_rows(table, values, columns)

            total_cnt += row_cnt
            if row_cnt < self.WRITE_BATCH_SIZE:
                logger.info(f"Written {total_cnt:,} rows to destination table {table}")
                return total_cnt

    def _process_values(self, rows: iter, columns: list) -> Iterator[list]:
        """
        Transforms the rows (dictionaries of column:value pairs) to lists of values in the order as specified by
        columns. If a column:value pair is missing for a column present in columns, a GOBException is raised when
        self.ignore_missing == False. If self.ignore_missing == True, the value for that column will be set to None.
        """
        def process_col(column, row_):
            if column['name'].lower() in row_:
                return row_[column['name'].lower()]
            elif not self.ignore_missing:
                raise GOBException(f"Missing column {column['name'].lower()} in query result")
            else:
                return None

        for row in rows:
            yield [process_col(col, row) for col in columns]
