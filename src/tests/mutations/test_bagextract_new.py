import datetime

import pytest

from gobbagextract.mutations.bagextract import BagExtractMutationsHandler, Afgifte
from gobbagextract.mutations.exception import NothingToDo


class TestBagExtractSoapInterface:

    def test_get_mutaties(self, mock_response_mutaties):
        handler = BagExtractMutationsHandler()
        mode, afgifte = handler.get_mutations(datetime.datetime.max)

        expected = Afgifte(**{
            'AfgifteID': '02b9b15c-3051-4af3-8714-9a3325bf69fa',
            'Afgiftereferentie': 'BAGNLDM-08042021-08052021.zip',
            'Bestandsnaam': 'BAGNLDM-08042021-08052021.zip',
            'artikelnummer': '2532',
            'DatumAanmelding': '2021-05-08T14:13:23.669+02:00',
            'BeschikbaarTot': '2021-11-08T14:13:23+01:00'
        })

        assert afgifte == expected
        assert mode.value == 'mutations'

    def test_get_full(self, mock_response_full):
        handler = BagExtractMutationsHandler()
        mode, afgifte = handler.get_full(datetime.datetime.max, '0457')

        expected = Afgifte(**{
            'AfgifteID': '09ec66c1-ca01-4b5a-a2e6-7d90d62cb2b2',
            'Afgiftereferentie': 'BAGGEM0457L-15052021.zip',
            'Bestandsnaam': 'BAGGEM0457L-15052021.zip',
            'artikelnummer': '2531',
            'DatumAanmelding': '2021-05-08T13:54:08.656+02:00',
            'BeschikbaarTot': '2021-11-08T13:53:51+01:00'
        })

        assert afgifte == expected
        assert mode.value == 'full'

        with pytest.raises(NothingToDo):
            handler.get_full(datetime.datetime.max, gemeente='1234')

    def test_empty_response(self, mock_response_empty):
        handler = BagExtractMutationsHandler()

        with pytest.raises(NothingToDo):
            handler.get_mutations(datetime.datetime.max)

        with pytest.raises(NothingToDo):
            handler.get_full(datetime.datetime.max, '0457')
