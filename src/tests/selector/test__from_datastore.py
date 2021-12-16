from unittest import TestCase
from unittest.mock import MagicMock

from gobbagextract.selector._from_datastore import FromDatastoreSelector


class TestFromDatastoreSelector(TestCase):

    def setUp(self) -> None:
        self.selector = FromDatastoreSelector()
        self.selector._src_connection = "postgres_connection"

    def test_read_rows(self):
        self.selector._src_datastore = MagicMock()
        self.selector._src_datastore.query.return_value = ["row_a", "row_b"]
        query = "some query"
        result = self.selector._read_rows(query)
        self.assertEqual(["row_a", "row_b"], result)
        self.selector._src_datastore.query.assert_called_with(query)
