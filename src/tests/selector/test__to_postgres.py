from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobbagextract.selector._to_postgres import ToPostgresSelector


class TestToPostgresSelector(TestCase):

    def setUp(self) -> None:
        self.config = {
            "type": "select",
            "queries": [
                {
                    "query": ["SELECT SOMETHING", "FROM SOMEWHERE", "ETC"],
                    "destination_table": {
                        "name": "dst.table",
                        "create": True,
                        "columns": [
                            {
                                "name": "col_a",
                                "type": "VARCHAR(20)",
                            },
                            {
                                "name": "col_b",
                                "type": "TIMESTAMP",
                            },
                        ],
                    },
                },
            ]
        }
        self.selector = ToPostgresSelector()
        self.selector._dst_datastore = MagicMock()

    @patch("gobbagextract.selector._to_postgres.Json")
    def test_prepare_row(self, mock_json):
        row = [
            {"key": "value"},
            {"key": "value"},
            {"key": "value"},
            {"key": "value"},
            {"key": "value"},
        ]
        columns = [
            {"type": "SOME_INNOCENT_TYPE"},
            {"type": "JSONB"},
            {"type": "SOME_INNOCENT_TYPE"},
            {"type": "SOME_INNOCENT_TYPE"},
            {"type": "JSON"},
        ]

        result = self.selector._prepare_row(row, columns)
        # Rows 1 and 4 should be replaced with return value
        row[1] = row[4] = mock_json.return_value
        self.assertEqual(row, result)

    def test_write_rows(self):
        table = "some_table"
        values = [[2, 4, 5], [2, 2, 0], [4, 4, 3]]
        columns = [
            {"type": "typ1", "name": "naam1"},
            {"type": "typ2", "name": "naam2"},
            {"type": "typ3", "name": "naam3"}
        ]
        self.selector._write_rows(table, values, columns)
        self.selector._dst_datastore.write_rows.assert_called_with(table, values, columns=["naam1", "naam2", "naam3"])
