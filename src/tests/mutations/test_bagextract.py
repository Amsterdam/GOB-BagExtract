import copy
import datetime
from unittest.mock import MagicMock

import freezegun
import pytest
from freezegun import freeze_time

from gobbagextract.mutations.bagextract import BagExtractMutationsHandler, ImportMode, MutationImport, \
    NothingToDo, Afgifte
from gobcore.exceptions import GOBException


class TestBagExtractSoapInterface:

    def test_get_mutaties(self, mock_response_mutaties, monkeypatch):
        handler = BagExtractMutationsHandler()
        date = datetime.date(2021, 11, 14)
        mode, afgifte = handler.get_daily_mutations(date)

        expected = Afgifte(**{
            'AfgifteID': '02b9b15c-3051-4af3-8714-9a3325bf69fa',
            'Afgiftereferentie': 'BAGNLDM-13112021-14112021.zip',
            'Bestandsnaam': 'BAGNLDM-13112021-14112021.zip',
            'artikelnummer': '2532',
            'DatumAanmelding': '2021-05-08T14:13:23.669+02:00',
            'BeschikbaarTot': '2021-11-08T14:13:23+01:00'
        })
        assert mode.value == 'mutations' and afgifte == expected

        # different date, result is empty
        with pytest.raises(NothingToDo, match='BAGNLDM-06122021-07122021.zip'):
            handler.get_daily_mutations(datetime.date(2021, 12, 7))

    def test_get_full(self, mock_response_full):
        handler = BagExtractMutationsHandler()
        mode, afgifte = handler.get_full(datetime.date(2021, 11, 14), '0457')

        expected = Afgifte(**{
            'AfgifteID': '09ec66c1-ca01-4b5a-a2e6-7d90d62cb2b2',
            'Afgiftereferentie': 'BAGGEM0457L-15102021.zip',
            'Bestandsnaam': 'BAGGEM0457L-15102021.zip',
            'artikelnummer': '2531',
            'DatumAanmelding': '2021-05-08T13:54:08.656+02:00',
            'BeschikbaarTot': '2021-11-08T13:53:51+01:00'
        })
        assert mode.value == 'full' and afgifte == expected

        # different gemeente
        with pytest.raises(NothingToDo, match='BAGGEM1234L-15052021.zip'):
            handler.get_full(datetime.date(2021, 5, 15), gemeente='1234')

        # different date
        with pytest.raises(NothingToDo, match='BAGGEM0457L-15112021.zip'):
            handler.get_full(datetime.date(2021, 12, 7), gemeente='0457')

    def test_empty_response(self, mock_response_empty):
        handler = BagExtractMutationsHandler()

        with pytest.raises(NothingToDo, match='BAGNLDM-06122021-07122021.zip'):
            handler.get_daily_mutations(datetime.date(2021, 12, 7))

        with pytest.raises(NothingToDo, match='BAGGEM0457L-15112021.zip'):
            handler.get_full(datetime.date(2021, 12, 7), '0457')

    @freeze_time("2021-02-10")
    def test_handle_import(self, mock_config, mock_response_empty):
        handler = BagExtractMutationsHandler()

        handler.get_daily_mutations = MagicMock(
            side_effect=lambda x: (
                ImportMode.MUTATIONS,
                Afgifte(Bestandsnaam=handler._mutations_filename(x), AfgifteID='id_mut'))
        )
        handler.get_full = MagicMock(
            side_effect=lambda *args: (
                ImportMode.FULL,
                Afgifte(Bestandsnaam=handler._full_filename(*args), AfgifteID='id_full'))
        )

        testcases = [
            # (last_import, expected_next_mode, expected_next_filename, expected_full_location)
            (
                MutationImport(mode=ImportMode.MUTATIONS.value, filename='BAGNLDM-30012021-31012021.zip'),
                ImportMode.MUTATIONS,
                'BAGNLDM-31012021-01022021.zip',
                 Afgifte(AfgifteID='id_full', Bestandsnaam='BAGGEM0457L-01022021.zip')
            ),
            (
                MutationImport(mode=ImportMode.FULL.value, filename='BAGGEM0123L-15012021.zip'),
                ImportMode.MUTATIONS,
                'BAGNLDM-15012021-16012021.zip',
                Afgifte(AfgifteID='id_full', Bestandsnaam='BAGGEM0457L-01022021.zip')
            ),
            (
                MutationImport(mode=ImportMode.MUTATIONS.value, filename='BAGNLDM-13012021-14012021.zip'),
                ImportMode.FULL,
                'BAGGEM0457L-15012021.zip',
                None
            ),
            (
                None, ImportMode.FULL, 'BAGGEM0457L-15012021.zip', None
            )
        ]

        for last_import, expected_mode, expected_fname, expected_full_location, expected_download_loc in testcases:
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
                expected_download_loc = 'id_full'
            else:
                expected_download_loc = 'id_mut'

            expected_new_dataset = {
                'version': '0.1',
                'catalogue': 'bag',
                'entity': 'ligplaatsen',
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
                expected_new_dataset['source']['read_config']['last_full_download_location'] = expected_full_location
                # expected_new_dataset['source']['read_config']['download_location'] = expected_full_location
                # print(expected_full_location)

            print(expected_new_dataset)
            print(new_dataset)
            print("--")

            assert expected_new_dataset == new_dataset

    def test_have_next(self, mock_config):
        handler = BagExtractMutationsHandler()

        handler.start_next = MagicMock()
        mutation_import = MutationImport()

        # Have next
        assert handler.have_next(mutation_import, mock_config) is True
        handler.start_next.assert_called_with(mutation_import, '0457')

        # Don't have next
        handler.start_next.side_effect = NothingToDo
        assert handler.have_next(mutation_import, mock_config) is False

    def test_response_date_error(self, mock_response_error):
        handler = BagExtractMutationsHandler()

        with pytest.raises(GOBException, match="Unknown format: 'FAKE_FILENAME.zip'"):
            handler.get_daily_mutations(datetime.date(2021, 12, 7))

        with pytest.raises(GOBException, match="Unknown format: 'FAKE_FILENAME.zip'"):
            handler.get_full(datetime.date(2021, 12, 7), '1234')
