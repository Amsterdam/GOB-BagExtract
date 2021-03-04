import datetime
import pprint
from unittest import TestCase

import os
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, call, patch

from gobbagextract.datastore.bag_extract import BagExtractDatastore, \
    GOBException, _extract_nested_zip, ElementFormatter
from gobcore.enum import ImportMode


class TestModuleFunctions(TestCase):

    def test_extract_nested_zip(self):
        testfile = os.path.join(os.path.dirname(__file__), 'testzip_for_extraction.zip')
        tmpdir = TemporaryDirectory()

        _extract_nested_zip(testfile, ['zipfile.zip', 'some_nested_zip.zip'], tmpdir.name)

        self.assertEqual({
            'some_file1.txt',
            'some_file2.txt'
        }, set(os.listdir(tmpdir.name)))


class TestElementFormatter(TestCase):
    """Class is mostly tested with the test_query_full and test_query_mutations methods in the class below

    """

    @patch("gobbagextract.datastore.bag_extract.ogr.CreateGeometryFromGML")
    @patch("gobbagextract.datastore.bag_extract.ET")
    def test_gml_to_wkt(self, mock_et, mock_create_geometry):
        ef = ElementFormatter('')

        res = ef._gml_to_wkt('elm')

        mock_create_geometry.assert_called_with(mock_et.tostring().decode())
        mock_create_geometry.return_value.FlattenTo2D.assert_called_once()
        mock_create_geometry.return_value.ExportToWkt.assert_called_once()
        self.assertEqual(mock_create_geometry().ExportToWkt(), res)


class TestBagExtractDatastore(TestCase):

    def get_test_object(self):
        with patch("gobbagextract.datastore.bag_extract.TemporaryDirectory"):
            connection_config = {"connection": "config"}
            read_config = {
                "object_type": "OBJT",
                "xml_object": "Object",
                "mode": ImportMode.FULL,
                "gemeentes": ["0456"],
                "download_location": "download location",
            }
            ds = BagExtractDatastore(connection_config, read_config, None)
            ds.tmp_dir.name = "/tmp_dir_name"
            return ds

    def test_check_config(self):
        minimal_read_config = {
            'object_type': 'object type',
            'xml_object': 'xml object',
            'mode': ImportMode.FULL,
            'gemeentes': ['gemeentes'],
            'download_location': 'location',
        }
        BagExtractDatastore({}, minimal_read_config, None)

        for k in minimal_read_config.keys():
            missing_key_config = {**minimal_read_config}
            del missing_key_config[k]

            with self.assertRaisesRegex(GOBException, f"Missing {k} in read_config"):
                BagExtractDatastore({}, missing_key_config, None)

        minimal_read_config['mode'] = 'invalid mode'

        with self.assertRaises(AssertionError):
            BagExtractDatastore({}, minimal_read_config, None)

        minimal_read_config['mode'] = ImportMode.MUTATIONS

        with self.assertRaisesRegex(GOBException, "Missing last_full_download_location in read_config"):
            BagExtractDatastore({}, minimal_read_config, None)

        minimal_read_config['last_full_download_location'] = 'last full download location'
        # Should not fail
        BagExtractDatastore({}, minimal_read_config, None)

    def test_init(self):
        ds = self.get_test_object()
        self.assertEqual(ImportMode.FULL, ds.mode)
        self.assertEqual("./sl:standBestand/sl:stand/sl-bag-extract:bagObject/Objecten:Object", ds.full_xml_path)
        self.assertEqual([
            "./ml:mutatieBericht/ml:mutatieGroep/ml:toevoeging/ml:wordt/mlm:bagObject/Objecten:Object",
            "./ml:mutatieBericht/ml:mutatieGroep/ml:wijziging/ml:wordt/mlm:bagObject/Objecten:Object",
        ], ds.mutation_xml_paths)

    @patch("gobbagextract.datastore.bag_extract.os.listdir")
    @patch("gobbagextract.datastore.bag_extract._extract_nested_zip")
    def test_extract_full_file(self, mock_extract_zip, mock_listdir):
        ds = self.get_test_object()
        mock_listdir.return_value = [
            "fileA0001.xml",
            "fileA0002.xml",
            "fileA0003.zip",
        ]

        file_location = "thepath/to/BAGGEM1234L-12345678.zip"
        res = ds._extract_full_file(file_location)

        mock_extract_zip.assert_called_with(
            file_location, [
                '1234GEM12345678.zip',
                '1234OBJT12345678.zip',
            ],
            "/tmp_dir_name/full",
        )
        self.assertEqual([
            "/tmp_dir_name/full/fileA0001.xml",
            "/tmp_dir_name/full/fileA0002.xml",
        ], res)

        # Invalid filename
        with self.assertRaises(GOBException):
            ds._extract_full_file("thepath/to/BAGGEM123L-148024.zip")

    @patch("gobbagextract.datastore.bag_extract.os.listdir")
    @patch("gobbagextract.datastore.bag_extract._extract_nested_zip")
    def test_extract_mutations_file(self, mock_extract_zip, mock_listdir):
        ds = self.get_test_object()
        mock_listdir.return_value = [
            "fileA0001.xml",
            "fileA0002.xml",
            "fileA0003.zip",
        ]

        file_location = "thepath/to/BAGNLDM-12345678-12345679.zip"
        res = ds._extract_mutations_file(file_location)

        mock_extract_zip.assert_called_with(
            file_location, [
                "9999MUT12345678-12345679.zip"
            ],
            "/tmp_dir_name/mutations",
        )
        self.assertEqual([
            "/tmp_dir_name/mutations/fileA0001.xml",
            "/tmp_dir_name/mutations/fileA0002.xml",
        ], res)

        # Invalid filename
        with self.assertRaises(GOBException):
            ds._extract_mutations_file("thepath/to/BAGNLDM-12345678-123.zip")

    @patch("gobbagextract.datastore.bag_extract.ET")
    def test_get_mutation_ids(self, mock_et):
        class MockElm:
            def __init__(self, text):
                self.text = text

        ds = self.get_test_object()
        ds._download_file = MagicMock()
        ds._extract_full_file = MagicMock(return_value=['file1', 'file2'])

        ds.read_config['last_full_download_location'] = 'last/full/download/location'
        mock_et.parse.return_value.getroot.return_value.iterfind.return_value = [MockElm('id1'), MockElm('id2')]

        self.assertEqual(['id1', 'id2', 'id1', 'id2'], ds._get_mutation_ids())
        ds._get_mutation_ids()
        mock_et.parse.assert_has_calls([
            call('file1'),
            call('file2'),
        ], any_order=True)
        ds._download_file.assert_called_with('last/full/download/location')
        ds._extract_full_file.assert_called_with(ds._download_file.return_value)

    def test_connect(self):
        ds = self.get_test_object()
        ds._download_file = MagicMock()
        ds._extract_full_file = MagicMock()
        ds._extract_mutations_file = MagicMock()
        ds._get_mutation_ids = MagicMock()

        ds.connect()
        self.assertEqual(ds._extract_full_file.return_value, ds.files)
        ds._download_file.assert_called_with(ds.read_config['download_location'])
        ds._extract_full_file.assert_called_with(ds._download_file.return_value)
        ds._get_mutation_ids.assert_not_called()
        self.assertIsNone(ds.ids)

        ds.mode = ImportMode.MUTATIONS
        ds.connect()
        self.assertEqual(ds._extract_mutations_file.return_value, ds.files)
        ds._extract_mutations_file.assert_called_with(ds._download_file.return_value)
        self.assertEqual(ds._get_mutation_ids.return_value, ds.ids)

    @patch("builtins.open")
    @patch("gobbagextract.datastore.bag_extract.os.getenv", lambda x: x)
    @patch("gobbagextract.datastore.bag_extract.requests")
    def test_download_file(self, mock_requests, mock_open):
        ds = self.get_test_object()

        self.assertEqual("/tmp_dir_name/BAGGEM0457L-15112020.zip",
                         ds._download_file("http://some.location.example.com/dir/BAGGEM0457L-15112020.zip"))

        mock_requests.get.assert_called_with("http://some.location.example.com/dir/BAGGEM0457L-15112020.zip")
        mock_open.assert_called_with('/tmp_dir_name/BAGGEM0457L-15112020.zip', 'wb')
        mock_open.return_value.__enter__.return_value.write.assert_called_with(mock_requests.get.return_value.content)
        mock_requests.get.return_value.raise_for_status.assert_called_once()

    def test_query_full(self):
        """Tests query, _element_to_dict, _flatten_dict, _flatten_nested_list and _gml_to_wkt

        :return:
        """

        read_config = {
            'object_type': 'VBO',
            'xml_object': 'Verblijfsobject',
            'mode': ImportMode.FULL,
            'gemeentes': ['0457'],
            'download_location': 'the location',
        }
        date_now = datetime.datetime.now().date()
        ds = BagExtractDatastore({}, read_config, date_now)
        ds.files = [os.path.join(os.path.dirname(__file__), 'bag_extract_fixtures', 'full.xml')]
        res = list(ds.query(None))
        self.assertEqual(len(res), 1)
        pprint.pprint(res)

        expected = {
            'documentdatum': '1900-01-01',
            'documentnummer': 'docnr',
            'gebruiksdoel': ['woonfunctie', 'industriefunctie', 'kantoorfunctie'],
            'geconstateerd': 'N',
            'geometrie/punt': 'POINT (131419 482833)',  # GML to WKT, and 3D to 2D
            'heeftAlsHoofdadres/NummeraanduidingRef': 'hoofdA',  # returned as single value
            'heeftAlsNevenadres/NummeraanduidingRef': ['nevenA', 'nevenB'],  # returned as list
            'identificatie': 'votA',
            'maaktDeelUitVan/PandRef': ['pndA', 'pndB'],
            'oppervlakte': '209494',
            'status': 'Verblijfsobject in gebruik',
            'voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV': '2010-11-15T13:31:10.557',
            'voorkomen/Voorkomen/beginGeldigheid': '2010-08-31',
            'voorkomen/Voorkomen/tijdstipRegistratie': '2010-11-15T13:22:03.000',
            'voorkomen/Voorkomen/voorkomenidentificatie': '1'
        }

        self.assertEqual(len(res), 1)
        self.assertEqual(date_now, res[0]['last_update'])
        self.assertEqual(expected, res[0]['object'])

    def test_query_mutations(self):
        """Tests query, _element_to_dict, _flatten_dict, _flatten_nested_list and _gml_to_wkt

        :return:
        """
        read_config = {
            'object_type': 'VBO',
            'xml_object': 'Verblijfsobject',
            'mode': ImportMode.MUTATIONS,
            'gemeentes': ['0457'],
            'download_location': 'the location',
            'last_full_download_location': 'last full download',
        }
        ds = BagExtractDatastore({}, read_config, None)
        ds.files = [os.path.join(os.path.dirname(__file__), 'bag_extract_fixtures', 'mutations.xml')]
        ds.ids = ['0458010000059153123123123']
        res = list(ds.query(None))

        expected = [{
            'documentdatum': '2010-09-28',
            'documentnummer': 'Z.9704/D.6419',
            'gebruiksdoel': 'kantoorfunctie',
            'geconstateerd': 'N',
            'geometrie/punt': 'POINT (129955.346 481542.434)',
            'heeftAlsHoofdadres/NummeraanduidingRef': '0458200000199376',
            'identificatie': '0458010000059153123123123',
            'maaktDeelUitVan/PandRef': '0457100000056661',
            'oppervlakte': '45',
            'status': 'Verblijfsobject in gebruik',
            'voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV': '2010-11-26T09:01:30.485',
            'voorkomen/Voorkomen/beginGeldigheid': '2010-09-28',
            'voorkomen/Voorkomen/tijdstipRegistratie': '2010-11-26T08:49:50.000',
            'voorkomen/Voorkomen/voorkomenidentificatie': '1'
        }, {
            'documentdatum': '2010-09-28',
            'documentnummer': 'Z.9704/D.6419',
            'gebruiksdoel': 'kantoorfunctie',
            'geconstateerd': 'N',
            'geometrie/punt': 'POINT (129990.358 481557.42)',
            'heeftAlsHoofdadres/NummeraanduidingRef': '0457200000199395',
            'identificatie': '0457010000059164',
            'maaktDeelUitVan/PandRef': '0457100000056651',
            'oppervlakte': '51',
            'status': 'Verblijfsobject in gebruik',
            'voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV': '2020-11-15T07:42:59.308',
            'voorkomen/Voorkomen/beginGeldigheid': '2020-11-14',
            'voorkomen/Voorkomen/tijdstipRegistratie': '2020-11-14T21:56:54.615',
            'voorkomen/Voorkomen/voorkomenidentificatie': '2'
        }, {
            'documentdatum': '2010-09-28',
            'documentnummer': 'Z.9704/D.6419',
            'gebruiksdoel': 'kantoorfunctiemutatie',
            'geconstateerd': 'N',
            'geometrie/punt': 'POINT (129990.358 481557.42)',
            'heeftAlsHoofdadres/NummeraanduidingRef': '0457200000199395',
            'identificatie': '0457010000059164',
            'maaktDeelUitVan/PandRef': '0457100000056651',
            'oppervlakte': '53',
            'status': 'Verblijfsobject in gebruik mutatie',
            'voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV': '2010-11-26T09:01:30.671',
            'voorkomen/Voorkomen/beginGeldigheid': '2010-09-28',
            'voorkomen/Voorkomen/eindGeldigheid': '2020-11-14',
            'voorkomen/Voorkomen/eindRegistratie': '2020-11-14T21:56:54.615',
            'voorkomen/Voorkomen/tijdstipRegistratie': '2010-11-26T08:49:52.000',
            'voorkomen/Voorkomen/voorkomenidentificatie': '1'
        }]

        self.assertEqual(expected, [r['object'] for r in res])
