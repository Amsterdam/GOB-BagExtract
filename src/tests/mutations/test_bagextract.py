import copy
import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from gobbagextract.mutations.bagextract import BagExtractMutationsHandler, GOBException, ImportMode, MutationImport, \
    NothingToDo


class TestBagExtractMutationsHandler(TestCase):

    def test_last_full_import_date(self):
        handler = BagExtractMutationsHandler()

        testcases = [
            # (now, result)
            ("2021-02-08", "2021-01-15"),
            ("2021-02-01", "2021-01-15"),
            ("2021-02-15", "2021-02-15"),
            ("2021-02-27", "2021-02-15"),
            ("2021-01-15", "2021-01-15"),
            ("2021-01-14", "2020-12-15"),
        ]

        for now, result in testcases:
           self.assertEqual(result, handler._last_full_import_date(datetime.date.fromisoformat(now)).strftime("%Y-%m-%d"))

    def test_date_gemeente_from_filename(self):
        testcases = [
            ("BAGGEM0457L-15102020.zip", (datetime.date(year=2020, month=10, day=15), '0457')),
            ("BAGGEM0000L-15102020.zip", (datetime.date(year=2020, month=10, day=15), '0000')),
            ("BAGNLDM-10112020-11112020.zip", (datetime.date(year=2020, month=11, day=11), None)),
            ("BAGNLDM-31012021-01022021.zip", (datetime.date(year=2021, month=2, day=1), None)),
        ]

        handler = BagExtractMutationsHandler()
        for filename, expected_date in testcases:
            self.assertEqual(expected_date, handler._date_gemeente_from_filename(filename))

        with self.assertRaises(GOBException):
            handler._date_gemeente_from_filename("Unparsable")

    def test_datestr(self):
        self.assertEqual("07022020", BagExtractMutationsHandler()._datestr(datetime.date(year=2020, month=2, day=7)))

    @patch("gobbagextract.mutations.bagextract.BAGEXTRACT_DOWNLOAD_URL", "https://example.com")
    def test_get_full_download_path(self):
        self.assertEqual("https://example.com/Gemeente LVC/1234/",
                         BagExtractMutationsHandler()._get_full_download_path("1234"))

    @patch("gobbagextract.mutations.bagextract.BAGEXTRACT_DOWNLOAD_URL", "https://example.com")
    def test_get_mutations_download_path(self):
        self.assertEqual("https://example.com/Nederland dagmutaties/",
                         BagExtractMutationsHandler()._get_mutations_download_path())

    @patch("gobbagextract.mutations.bagextract.htmllistparse.fetch_listing")
    def test_list_path(self, mock_fetch_listing):
        class MockListing:
            def __init__(self, name):
                self.name = name

        mock_fetch_listing.return_value = '', [
            MockListing("a"),
            MockListing("b"),
            MockListing("c"),
        ]
        self.assertEqual(['a', 'b', 'c'], BagExtractMutationsHandler()._list_path('the path'))
        mock_fetch_listing.assert_called_with('the path')

    def test_get_available_full_downloads(self):
        handler = BagExtractMutationsHandler()
        handler._get_full_download_path = MagicMock(side_effect=lambda gemeente: f"Path for {gemeente}")
        handler._list_path = MagicMock(side_effect=lambda path: [f"{path} 1", f"{path} 2"])

        self.assertEqual([
            'Path for 1234 1',
            'Path for 1234 2',
        ], handler._get_available_full_downloads("1234"))

    def test_get_available_mutations_downloads(self):
        handler = BagExtractMutationsHandler()
        handler._get_mutations_download_path = MagicMock(return_value="Mutations")
        handler._list_path = MagicMock(side_effect=lambda path: [f"{path} 1", f"{path} 2"])

        self.assertEqual([
            'Mutations 1',
            'Mutations 2',
        ], handler._get_available_mutation_downloads())

    def test_handle_import_no_full_available(self):

        handler = BagExtractMutationsHandler()

        # Last download not available...
        handler._get_available_full_downloads = lambda x: []

        with freeze_time("2021-02-10"):
            with self.assertRaises(GOBException):
                handler._get_last_full_import_location('0123', datetime.date.today())

    def test_handle_import(self):
        dataset = {
            'source': {
                'read_config': {
                    'gemeentes': [
                        '0123',
                    ],
                },
                'application': 'THE APP',
            },
            'catalogue': 'THE CAT',
            'entity': 'THE ENT',
        }

        handler = BagExtractMutationsHandler()
        # handler._last_full_import = MagicMock(return_value=datetime.date(year=2021, month=1, day=15))
        handler._get_full_download_path = lambda gemeente: f'http://example.com/full/{gemeente}/'
        handler._get_mutations_download_path = lambda: 'http://example.com/mutations/'
        handler._get_available_mutation_downloads = lambda: [
            'BAGNLDM-30012021-31012021.zip',
            'BAGNLDM-31012021-01022021.zip',
            'BAGNLDM-13012021-14012021.zip',
            'BAGNLDM-15012021-16012021.zip',
        ]

        handler._get_available_full_downloads = lambda x: [
            'BAGGEM0123L-15122020.zip',
            'BAGGEM0123L-15012021.zip',
            'BAGGEM0123L-15012021.zip',
        ]

        testcases = [
            # (last_import, expected_next_mode, expected_next_filename)
            (MutationImport(
                mode=ImportMode.MUTATIONS.value,
                filename='BAGNLDM-30012021-31012021.zip',
            ), ImportMode.MUTATIONS, 'BAGNLDM-31012021-01022021.zip', 'BAGGEM0123L-15012021.zip'),
            (MutationImport(
                mode=ImportMode.FULL.value,
                filename='BAGGEM0123L-15012021.zip',
            ), ImportMode.MUTATIONS, 'BAGNLDM-15012021-16012021.zip', 'BAGGEM0123L-15012021.zip'),
            (MutationImport(
                mode=ImportMode.MUTATIONS.value,
                filename='BAGNLDM-13012021-14012021.zip'
            ), ImportMode.FULL, 'BAGGEM0123L-15012021.zip', None),
            (None, ImportMode.FULL, 'BAGGEM0123L-15012021.zip', None),
        ]

        with freeze_time("2021-02-10"):
            for last_import, expected_mode, expected_fname, expected_full_location in testcases:
                if last_import:
                    # Not ended. Check only for when last_import is not None
                    next_import, new_dataset, date = handler.handle_import(last_import, copy.deepcopy(dataset))
                    exp_date, _ = handler._date_gemeente_from_filename(last_import.filename)
                    self.assertEqual(date, exp_date)
                    self.assertEqual(last_import.mode, next_import.mode)
                    self.assertEqual(last_import.filename, next_import.filename)

                    # Last is ended, expect next file to be triggered
                    last_import.ended_at = datetime.datetime.utcnow()

                next_import, new_dataset, date = handler.handle_import(last_import, copy.deepcopy(dataset))
                self.assertEqual(expected_mode.value, next_import.mode)
                self.assertEqual(expected_fname, next_import.filename)

                if expected_mode == ImportMode.FULL:
                    expected_download_loc = f'http://example.com/full/0123/{expected_fname}'
                else:
                    expected_download_loc = f'http://example.com/mutations/{expected_fname}'

                expected_new_dataset = {
                    'source': {
                        'read_config': {
                            'gemeentes': [
                                '0123',
                            ],
                            'download_location': expected_download_loc,
                        },
                        'application': 'THE APP',
                    },
                    'catalogue': 'THE CAT',
                    'entity': 'THE ENT',
                }
                if expected_full_location:
                    expected_new_dataset['source']['read_config'][
                        'last_full_download_location'] = f'http://example.com/full/0123/{expected_full_location}'

                self.assertEqual(expected_new_dataset, new_dataset)

            # Test Exception for when not available
            handler._get_available_mutation_downloads = lambda: []
            handler._get_available_full_downloads = lambda x: []

            for last_import, _, expected_fname, _ in testcases:
                with self.assertRaisesRegex(NothingToDo, f"File {expected_fname} not yet available for download"):
                    handler.handle_import(last_import, copy.deepcopy(dataset))

    def test_have_next(self):
        handler = BagExtractMutationsHandler()
        dataset = {
            'source': {
                'read_config': {
                    'gemeentes': [
                        '0123',
                    ],
                },
            },
        }

        handler.start_next = MagicMock()
        mutation_import = MutationImport()

        # Have next
        self.assertTrue(handler.have_next(mutation_import, dataset))
        handler.start_next.assert_called_with(mutation_import, '0123')

        # Don't have next
        handler.start_next.side_effect = NothingToDo
        self.assertFalse(handler.have_next(mutation_import, dataset))
