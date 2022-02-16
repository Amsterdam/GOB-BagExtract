from typing import List

from psycopg2 import Error
from psycopg2.extras import execute_values

from gobcore.datastore.postgres import PostgresDatastore
from gobcore.exceptions import GOBException


class PostgresDatastoreExt(PostgresDatastore):

    def write_rows(self, table: str, rows: List[list], columns: list) -> int:
        """
        Writes rows to Postgres table using the optimised execute_values function from psycopg2, which
        combines all inserts into one query.

        :param connection:
        :param table:
        :param rows:
        :param columns: columns in each row, first column item is the unique id
        :return:
        """
        id_name = columns[0]
        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES %s " \
            f"ON CONFLICT({id_name}) " \
            f"DO UPDATE SET " \
            f"{','.join([ col + '=EXCLUDED.' + col for col in columns[1:]])}"
        try:
            with self.connection.cursor() as cursor:
                execute_values(cursor, query, rows)
                self.connection.commit()
        except Error as e:
            # print(e.pgcode)
            # print(e.pgerror)
            # print(query)
            print(id_name)
            print(sorted([r[0] for r in rows]))
            # print(rows[0][0])
            raise GOBException(f'Error writing rows to table {table}. Error: {e}')

        return len(rows)
