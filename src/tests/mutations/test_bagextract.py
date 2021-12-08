import copy
import datetime
from unittest.mock import MagicMock

from freezegun import freeze_time

from gobbagextract.config import KADASTER_PRODUCTSTORE_URL
from gobbagextract.mutations.bagextract import BagExtractMutationsHandler, ImportMode, MutationImport, \
    NothingToDo, Afgifte


class TestBagExtractMutationsHandler:

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
            expected = handler._last_full_import_date(datetime.date.fromisoformat(now)).strftime("%Y-%m-%d")
            assert result == expected

    # def test_date_gemeente_from_filename(self):
    #     testcases = [
    #         ("BAGGEM0457L-15102020.zip", (datetime.date(year=2020, month=10, day=15), '0457')),
    #         ("BAGGEM0000L-15102020.zip", (datetime.date(year=2020, month=10, day=15), '0000')),
    #         ("BAGNLDM-10112020-11112020.zip", (datetime.date(year=2020, month=11, day=11), None)),
    #         ("BAGNLDM-31012021-01022021.zip", (datetime.date(year=2021, month=2, day=1), None)),
    #     ]
    #
    #     handler = BagExtractMutationsHandler()
    #     for filename, expected_date in testcases:
    #         self.assertEqual(expected_date, handler._date_gemeente_from_filename(filename))
    #
    #     with self.assertRaises(GOBException):
    #         handler._date_gemeente_from_filename("Unparsable")

    def test_datestr(self):
        assert BagExtractMutationsHandler()._datestr(datetime.date(year=2020, month=2, day=7)) == "07022020"

    @freeze_time("2021-02-10")
    def test_handle_import(self, mock_config, mock_response_empty):
        # dataset = {
        #     'source': {
        #         'read_config': {
        #             'gemeentes': [
        #                 '0123',
        #             ],
        #         },
        #         'application': 'THE APP',
        #     },
        #     'catalogue': 'THE CAT',
        #     'entity': 'THE ENT',
        # }
        handler = BagExtractMutationsHandler()
        handler.get_daily_mutations = MagicMock(
            side_effect=lambda x: (
                ImportMode.MUTATIONS,
                Afgifte(Bestandsnaam=handler._mutations_filename(x), AfgifteID='id_mut'))
        )
        handler.get_full = MagicMock(
            side_effect=lambda *args: (
                ImportMode.MUTATIONS,
                Afgifte(Bestandsnaam=handler._full_filename(*args), AfgifteID='id_full'))
        )

        testcases = [
            # (last_import, expected_next_mode, expected_next_filename)
            (
                MutationImport(mode=ImportMode.MUTATIONS.value, filename='BAGNLDM-30012021-31012021.zip'),
                ImportMode.MUTATIONS,
                'BAGNLDM-31012021-01022021.zip',
                'BAGGEM0123L-15012021.zip'
            ),
            (
                MutationImport(mode=ImportMode.FULL.value, filename='BAGGEM0123L-15012021.zip'),
                ImportMode.MUTATIONS,
                'BAGNLDM-15012021-16012021.zip',
                'BAGGEM0123L-15012021.zip'
            ),
            (
                MutationImport(mode=ImportMode.MUTATIONS.value, filename='BAGNLDM-13012021-14012021.zip'),
                ImportMode.FULL,
                'BAGGEM0123L-15012021.zip',
                None
            ),
            (
                None, ImportMode.FULL, 'BAGGEM0123L-15012021.zip', None
            )
        ]

        for last_import, expected_mode, expected_fname, expected_full_location in testcases:
            if last_import:
                # Not ended. Check only when last_import is not None. Uses restart_import
                next_import, new_dataset, date = handler.handle_import(last_import, copy.deepcopy(mock_config))

                exp_date = Afgifte(Bestandsnaam=last_import.filename).get_date()

                assert date == exp_date
                assert last_import.mode == next_import.mode
                assert last_import.filename == next_import.filename

                # Last is ended, expect next file to be triggered
                last_import.ended_at = datetime.datetime.utcnow()

            next_import, new_dataset, date = handler.handle_import(last_import, copy.deepcopy(mock_config))
            assert expected_mode.value == next_import.mode
            assert expected_fname == next_import.filename

            if expected_mode == ImportMode.FULL:
                expected_download_loc = f'{KADASTER_PRODUCTSTORE_URL}/{expected_fname}'
            else:
                expected_download_loc = f'{KADASTER_PRODUCTSTORE_URL}/{expected_fname}'

            expected_new_dataset = {
                'version': '0.1',
                'catalogue': 'bag_test',
                'entity': 'ligplaatsen_test',
                'source': {
                    'name': 'Kadaster',
                    'application': 'BAGExtract',
                    'entity_id': 'identificatie',
                    'read_config': {
                        'object_type': 'LIG',
                        'xml_object': 'Ligplaats',
                        'gemeentes': [
                            '0457',
                        ],
                        'download_location': expected_download_loc,
                    }
                }
            }
            if expected_full_location:
                expected_new_dataset['source']['read_config']['last_full_download_location'] = \
                    f'{KADASTER_PRODUCTSTORE_URL}/{expected_full_location}'

            assert expected_new_dataset == new_dataset

        # Test Exception for when not available
        handler._get_available_mutation_downloads = lambda: []
        handler._get_available_full_downloads = lambda x: []

        for last_import, _, expected_fname, _ in testcases:
            with self.assertRaisesRegex(NothingToDo, f"File {expected_fname} not yet available for download"):
                handler.handle_import(last_import, copy.deepcopy(dataset))

    def test_have_next(self, mock_config):
        handler = BagExtractMutationsHandler()

        # handler.start_next = MagicMock()
        mutation_import = MutationImport()

        # Have next
        assert handler.have_next(mutation_import, mock_config) is True

        handler.start_next.assert_called_with(mutation_import, '0123')

        # Don't have next
        handler.start_next.side_effect = NothingToDo
        self.assertFalse(handler.have_next(mutation_import, dataset))
