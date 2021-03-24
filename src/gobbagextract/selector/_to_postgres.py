from psycopg2.extras import Json
from typing import List, Optional


class ToPostgresSelector():

    def _prepare_row(self, row: list, columns: list):
        """Perform data transformations where necessary

        :param row:
        :param columns:
        :return:
        """
        for idx, val in enumerate(row):
            if columns[idx]['type'] in ["JSON", "JSONB"]:
                row[idx] = Json(val)
        return row

    def _write_rows(self, table: str, values, columns: Optional[List[str]] = None):
        self._dst_datastore.write_rows(table, values, columns)
