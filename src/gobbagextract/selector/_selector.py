import itertools
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.datastore.datastore import Datastore


class Selector():
    """
    Base Selector.

    Selector handles execution of queries on src_connection. The results of the queries are written to
    dst_database
    """
    WRITE_BATCH_SIZE = 50000

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
        """Entry method. Saves result of select query in destination
        table.

        :param query:
        :param destination_table:
        :return:
        """
        rows = self._read_rows(self.query)
        total_cnt = 0

        while True:
            chunk = itertools.islice(rows, self.WRITE_BATCH_SIZE)
            columns = self.destination_table['columns']
            values = self._values_list(chunk, columns)
            column_names = [c['name'] for c in columns]
            self._write_rows(self.destination_table['name'], values, column_names)

            total_cnt += len(values)

            if len(values) < self.WRITE_BATCH_SIZE:
                logger.info(f"Written {total_cnt} rows to destination table {self.destination_table['name']}")
                return total_cnt

    def _values_list(self, rows: iter, columns: list):
        """Transforms the rows (dictionaries of column:value pairs) to lists of values in the order as specified by
        columns. If a column:value pair is missing for a column present in columns, a GOBException is raised when
        self.ignore_missing == False. If self.ignore_missing == True, the value for that column will be set to None.

        :param rows:
        :param columns:
        :return:
        """
        result = []
        for row in rows:
            rowvals = []
            for column in columns:
                if column['name'].lower() in row:
                    rowvals.append(row[column['name'].lower()])
                elif not self.ignore_missing:
                    raise GOBException(f"Missing column {column['name'].lower()} in query result")
                else:
                    rowvals.append(None)
            result.append(self._prepare_row(rowvals, columns))
        return result
