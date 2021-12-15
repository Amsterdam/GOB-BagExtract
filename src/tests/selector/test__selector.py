from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.exceptions import GOBException
from gobbagextract.selector._selector import Selector


class TestSelector(TestCase):

    def setUp(self) -> None:
        self.config = {
            "type": "select",
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
        }
        self.selector = Selector("src_store", "dst_store", self.config)

    def test_init(self):
        self.assertEqual("src_store", self.selector._src_datastore)
        self.assertEqual("dst_store", self.selector._dst_datastore)
        self.assertEqual(self.config, self.selector._config)

    @patch("gobbagextract.selector._selector.logger")
    def test_select(self, mock_logger):
        result_cnt = 97

        self.selector.WRITE_BATCH_SIZE = 24
        self.selector._create_destination_table = MagicMock()
        self.selector._read_rows = MagicMock()
        self.selector._write_rows = MagicMock(side_effect=[24, 24, 24, 24, 1])

        # Mock values list. Important that returned length is the same as length of input generator x.
        self.selector._process_values = lambda x, y: [[] for _ in x]

        # Create bogus data, matching with table definition
        self.selector._read_rows.return_value = iter([{"col_a": str(i), "col_b": i} for i in range(result_cnt)])

        # Reset call count
        mock_logger.reset()

        result = self.selector.select()

        self.assertEqual(result_cnt, result)
        self.assertEqual(5, self.selector._write_rows.call_count)
        mock_logger.info.assert_called_once()

    @patch("gobbagextract.selector._selector.logger")
    def test_select_match_batch_size(self, mock_logger):
        result_cnt = 2
        self.selector.WRITE_BATCH_SIZE = result_cnt
        self.selector._create_destination_table = MagicMock()
        self.selector._read_rows = MagicMock()
        self.selector._write_rows = MagicMock(side_effect=[2, 0])

        # Mock values list. Important that returned length is the same as length of input generator x.
        self.selector._values_list = lambda x, y: [[] for _ in x]
        # Create bogus data, matching with table definition
        self.selector._read_rows.return_value = iter([{"col_a": str(i), "col_b": i} for i in range(result_cnt)])

        # Reset call count
        mock_logger.reset()

        result = self.selector.select()

        self.assertEqual(result_cnt, result)
        self.assertEqual(2, self.selector._write_rows.call_count)
        mock_logger.info.assert_called_once()

    def test_values_list(self):
        self.selector._prepare_row = lambda x, y: x  # return rowvals as is
        rows = [
            {"col_a": 8, "col_b": 2, "col_c": 7},
            {"col_b": 2, "col_c": 5, "col_a": 0},
            {"col_c": 2, "col_b": 4, "col_a": 6},
            {"col_a": 4, "col_c": 8, "col_b": 2},
        ]
        cols = [
            {"name": "col_a"},
            {"name": "col_b"},
            {"name": "col_c"}
        ]

        # Expect values of 'rows' in the order of 'cols'
        expected_result = [
            [8, 2, 7],
            [0, 2, 5],
            [6, 4, 2],
            [4, 2, 8]
        ]
        self.assertEqual(expected_result, list(self.selector._process_values(rows, cols)))

    def test_values_list_missing_column_exception(self):
        self.selector._prepare_row = lambda x, y: x  # return rowvals as is
        rows = [
            {"col_a": 8, "col_b": 2, "col_c": 7},
            {"col_b": 2, "col_c": 5, "col_a": 0},
            {"col_c": 2, "col_b": 4},
            {"col_a": 4, "col_c": 8, "col_b": 2},
        ]
        cols = [
            {"name": "col_a"},
            {"name": "col_b"},
            {"name": "col_c"}
        ]

        with self.assertRaisesRegex(GOBException, "Missing column"):
            list(self.selector._process_values(rows, cols))

    def test_values_list_missing_column_allowed(self):
        rows = [
            {"col_a": 8, "col_b": 2, "col_c": 7},
            {"col_b": 2, "col_c": 5, "col_a": 0},
            {"col_c": 2, "col_b": 4},
            {"col_a": 4, "col_b": 2},
        ]
        cols = [
            {"name": "col_a"},
            {"name": "col_b"},
            {"name": "col_c"}
        ]

        self.selector.ignore_missing = True

        # Expect values of 'rows' in the order of 'cols' with missing values set to None
        expected_result = [
            [8, 2, 7],
            [0, 2, 5],
            [None, 4, 2],
            [4, 2, None]
        ]
        self.assertEqual(expected_result, list(self.selector._process_values(rows, cols)))
