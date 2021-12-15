from unittest import TestCase
from unittest.mock import Mock, patch

from psycopg2 import Error
from gobbagextract.datastore.postgres import PostgresDatastoreExt


class PotgresTest(TestCase):

    @patch("gobbagextract.datastore.postgres.execute_values")
    def test_write_rows(self, mock_execute_values):
        config = {}
        ds = PostgresDatastoreExt(config)
        ds.connection = Mock()
        ds.connection.cursor = Mock()
        cursor = Mock()
        ds.connection.cursor.return_value.__enter__ = cursor
        ds.connection.cursor.return_value.__exit__ = cursor
        rows = [("a1", "a2"), ("b1", "b2")]
        table = "my table"
        columns = ["col1", "col2"]
        ds.write_rows(table, rows, columns)
        mock_execute_values.assert_called_once()
        query = "INSERT INTO my table (col1,col2) VALUES %s ON CONFLICT(col1) DO UPDATE SET col2=EXCLUDED.col2"
        mock_execute_values.assert_called_once()
        self.assertEqual(mock_execute_values.call_args.args[1], query)
        self.assertEqual(mock_execute_values.call_args.args[2], rows)

        ds.connection.cursor.side_effect = Error("Error")
        self.assertRaises(Exception, ds.write_rows, table, rows, columns)
