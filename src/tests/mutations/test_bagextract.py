import copy
import datetime
from unittest.mock import MagicMock, patch, call

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

    @freeze_time(datetime.date(2021, 10, 15))
    def test_initial_import_empty(self, mock_config, mock_response_empty):
        with patch('gobbagextract.mutations.bagextract.logger') as mock_logger:
            handler = BagExtractMutationsHandler()

            with pytest.raises(NothingToDo, match=f'{handler.INITIAL_IMPORT_RETRY} periods'):
                handler.handle_import(None, mock_config)

            mock_logger.assert_has_calls([
                call.warning('Retrying previous initial import for: 0457 / 2021-10-15'),
                call.warning('Retrying previous initial import for: 0457 / 2021-09-15'),
                call.warning('Retrying previous initial import for: 0457 / 2021-08-15'),
                call.warning('Retrying previous initial import for: 0457 / 2021-07-15'),
                call.warning('Retrying previous initial import for: 0457 / 2021-06-15'),
            ])

    @freeze_time(datetime.date(2021, 10, 15))
    def test_initial_import_nonempty(self, mock_config, mock_response_full):
        handler = BagExtractMutationsHandler()
        mut_import, dataset, date = handler.handle_import(None, mock_config)

        assert date == datetime.date(2021, 10, 15)
        assert dataset['source']['read_config']['download_location'] == \
               Afgifte(
                   AfgifteID='09ec66c1-ca01-4b5a-a2e6-7d90d62cb2b2',
                   Afgiftereferentie='BAGGEM0457L-15102021.zip',
                   Bestandsnaam='BAGGEM0457L-15102021.zip',
                   artikelnummer='2531',
                   DatumAanmelding='2021-05-08T13:54:08.656+02:00',
                   BeschikbaarTot='2021-11-08T13:53:51+01:00',
               )
        assert mut_import.mode == ImportMode.FULL.value
        assert mut_import.filename == dataset['source']['read_config']['download_location'].Bestandsnaam

    def test_restart_import_full(self, mock_config, mock_response_full):
        handler = BagExtractMutationsHandler()

        last_import = MutationImport(mode=ImportMode.FULL.value, filename='BAGGEM0457L-15102021.zip', ended_at=None)
        mut_import, dataset, date = handler.handle_import(last_import, mock_config)

        assert date == datetime.date(2021, 10, 15)
        assert dataset['source']['read_config']['download_location'] == \
               Afgifte(
                   AfgifteID='09ec66c1-ca01-4b5a-a2e6-7d90d62cb2b2',
                   Afgiftereferentie='BAGGEM0457L-15102021.zip',
                   Bestandsnaam='BAGGEM0457L-15102021.zip',
                   artikelnummer='2531',
                   DatumAanmelding='2021-05-08T13:54:08.656+02:00',
                   BeschikbaarTot='2021-11-08T13:53:51+01:00',
               )
        assert mut_import.mode == ImportMode.FULL.value
        assert mut_import.filename == dataset['source']['read_config']['download_location'].Bestandsnaam

    def test_restart_import_mutation(self, mock_config, mock_response_mutaties):
        handler = BagExtractMutationsHandler()
        handler.get_full = MagicMock()

        last_import = MutationImport(
            mode=ImportMode.MUTATIONS.value, filename='BAGNLDM-13112021-14112021.zip', ended_at=None
        )
        mut_import, dataset, date = handler.handle_import(last_import, mock_config)

        assert date == datetime.date(2021, 11, 14)
        assert dataset['source']['read_config'].get('last_full_download_location')
        assert dataset['source']['read_config']['download_location'] == \
               Afgifte(
                   AfgifteID='02b9b15c-3051-4af3-8714-9a3325bf69fa',
                   Afgiftereferentie='BAGNLDM-13112021-14112021.zip',
                   Bestandsnaam='BAGNLDM-13112021-14112021.zip',
                   artikelnummer='2532',
                   DatumAanmelding='2021-05-08T14:13:23.669+02:00',
                   BeschikbaarTot='2021-11-08T14:13:23+01:00'
               )
        assert mut_import.mode == ImportMode.MUTATIONS.value
        assert mut_import.filename == dataset['source']['read_config']['download_location'].Bestandsnaam
        handler.get_full.assert_called_with(datetime.date(2021, 11, 14), '0457')

    def test_handle_import_next_mutation(self, mock_response_mutaties, mock_config):
        handler = BagExtractMutationsHandler()
        handler.get_full = MagicMock()

        last_import = MutationImport(
            mode=ImportMode.FULL.value,
            filename='BAGGEM0457L-13112021.zip',
            ended_at=datetime.datetime(2021, 11, 15, 12, 00)
        )
        mut_import, dataset, date = handler.handle_import(last_import, mock_config)

        assert mut_import.filename == 'BAGNLDM-13112021-14112021.zip'
        assert mut_import.mode == ImportMode.MUTATIONS.value
        assert date == datetime.date(2021, 11, 14)

    def test_handle_import_next_full(self, mock_response_full, mock_config):
        handler = BagExtractMutationsHandler()
        last_import = MutationImport(
            mode=ImportMode.MUTATIONS.value,
            filename='BAGNLDM-13102021-14102021.zip',
            ended_at=datetime.datetime(2021, 11, 15, 12, 00)
        )
        mut_import, dataset, date = handler.handle_import(last_import, mock_config)

        assert mut_import.filename == 'BAGGEM0457L-15102021.zip'
        assert mut_import.mode == ImportMode.FULL.value
        assert date == datetime.date(2021, 10, 15)

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
