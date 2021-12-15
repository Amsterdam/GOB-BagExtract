from typing import Any

from psycopg2.extras import Json


class ToPostgresSelector:

    @staticmethod
    def _prepare_row(row: list, columns: list[dict[str, str]]) -> list[Any]:
        """Perform data transformations where necessary.
        Transforms JSON type values to correct database type

        :param row: values per row
        :param columns: dict of name
        :return: transformed row
        """
        for idx, val in enumerate(row):
            if columns[idx]['type'] in ["JSON", "JSONB"]:
                row[idx] = Json(val)
        return row

    def _write_rows(self, table: str, values, columns: list[dict[str, str]]) -> int:
        """Prepares and write values to a postgresql database and return number of rows."""
        values = [self._prepare_row(row, columns) for row in values]
        column_names = [c['name'] for c in columns]

        return self._dst_datastore.write_rows(table, values, columns=column_names)
