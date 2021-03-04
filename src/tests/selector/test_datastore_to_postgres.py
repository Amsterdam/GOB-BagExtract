from unittest import TestCase
from unittest.mock import MagicMock

from gobbagextract.selector.datastore_to_postgres import DatastoreToPostgresSelector


class TestDatastoreToPostgresSelector(TestCase):

    def test_init(self):
        src_conn = MagicMock()
        dst_conn = MagicMock()
        config = {
            "query": "some query",
            "query_src": "string",
            "destination_table": {},
        }

        selector = DatastoreToPostgresSelector(src_conn, dst_conn, config)
        self.assertEqual(src_conn, selector._src_datastore)
        self.assertEqual(dst_conn, selector._dst_datastore)
        self.assertEqual(config, selector._config)
