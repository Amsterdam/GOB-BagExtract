import json
import os

from unittest import TestCase

from gobbagextract.extract_config.extract_config import \
    get_extract_definition

datafile = os.path.join(os.path.dirname(__file__), '../../gobbagextract/data/bag.panden.weesp.json')


class TestExtractConfig(TestCase):

    def test_get_extract_definition(self):
        gemeente = '0457'
        def1 = get_extract_definition('bag', 'panden', gemeente)
        def2 = json.load(open(datafile))
        self.assertEqual(def1, def2)
        def1 = get_extract_definition('bag', 'panden')
        self.assertEqual(def1, def2)
