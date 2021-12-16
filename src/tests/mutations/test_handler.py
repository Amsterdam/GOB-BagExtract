
from unittest import TestCase
from unittest.mock import MagicMock

from gobbagextract.mutations.handler import MutationsHandler, BagExtractMutationsHandler, GOBException, MutationImport


class TestMutationsHandler(TestCase):

    def test_init(self):
        dataset = {
            "source": {
                "application": "BAGExtract",
            }
        }

        handler = MutationsHandler(dataset)
        self.assertEqual(dataset, handler.dataset)
        self.assertEqual("BAGExtract", handler.application)

        self.assertIsInstance(handler.handler, BagExtractMutationsHandler)

        with self.assertRaises(GOBException):
            dataset["source"]["application"] = "SomeOther"
            MutationsHandler(dataset)

    def test_get_next_import(self):
        dataset = {
            "source": {
                "application": "BAGExtract",
            }
        }
        handler = MutationsHandler(dataset)
        handler.handler = MagicMock()
        last_import = MagicMock()

        self.assertEqual(handler.handler.handle_import.return_value, handler.get_next_import(last_import))
        handler.handler.handle_import.assert_called_with(last_import, handler.dataset)

    def test_have_next(self):
        dataset = {
            "source": {
                "application": "BAGExtract",
            }
        }
        handler = MutationsHandler(dataset)
        handler.handler = MagicMock()

        last_import = MutationImport()
        self.assertEqual(handler.handler.have_next.return_value, handler.have_next(last_import))
        handler.handler.have_next.assert_called_with(last_import, handler.dataset)
