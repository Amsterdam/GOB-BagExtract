import datetime
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from gobbagextract.datastore.bag_extract import BagExtractDatastore, GOBException, _extract_nested_zip, ElementFormatter
from gobbagextract.mutations.afgifte import Afgifte
from gobcore.enum import ImportMode

mock_afgifte = Afgifte(
    Bestandsnaam="BAGGEM1234L-15122021.zip",
    AfgifteID="1234-5678-9",
    artikelnummer="2529"
)

mock_afgifte_mut = Afgifte(
    Bestandsnaam="BAGNLDM-15122021-16122021.zip",
    AfgifteID="1234-5678-9",
    artikelnummer="2529"
)

mock_afgifte_gobexception = Afgifte(
    Bestandsnaam="BAGGEM1234L-123456789.zip",
    AfgifteID="1234-5678-9",
    artikelnummer="2529"
)


class TestModuleFunctions(TestCase):

    def test_extract_nested_zip(self):
        testfile = os.path.join(os.path.dirname(__file__), "testzip_for_extraction.zip")

        with TemporaryDirectory() as tmpdir:
            _extract_nested_zip(testfile, ["zipfile.zip", "some_nested_zip.zip"], Path(tmpdir))
            self.assertEqual({"some_file1.txt", "some_file2.txt"}, set(os.listdir(tmpdir)))


class TestElementFormatter(TestCase):
    """Class is mostly tested with the test_query_full and test_query_mutations methods in the class below."""

    @patch("gobbagextract.datastore.bag_extract.ogr.CreateGeometryFromGML")
    @patch("gobbagextract.datastore.bag_extract.ElementTree")
    def test_gml_to_wkt(self, mock_et, mock_create_geometry):
        ef = ElementFormatter("")

        res = ef._gml_to_wkt("elm")

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
            ds.tmp_path = "/tmp_dir_name"
            return ds

    def test_check_config(self):
        minimal_read_config = {
            "object_type": "object type",
            "xml_object": "xml object",
            "mode": ImportMode.FULL,
            "gemeentes": ["gemeentes"],
            "download_location": "location",
        }
        BagExtractDatastore({}, minimal_read_config, None)

        for k in minimal_read_config.keys():
            missing_key_config = {**minimal_read_config}
            del missing_key_config[k]

            with self.assertRaisesRegex(GOBException, f"Missing {k} in read_config"):
                BagExtractDatastore({}, missing_key_config, None)

        minimal_read_config["mode"] = "invalid mode"

        with self.assertRaises(AssertionError):
            BagExtractDatastore({}, minimal_read_config, None)

        minimal_read_config["mode"] = ImportMode.MUTATIONS

        with self.assertRaisesRegex(GOBException, "Missing last_full_download_location in read_config"):
            BagExtractDatastore({}, minimal_read_config, None)

        minimal_read_config["last_full_download_location"] = "last full download location"
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

    @patch("gobbagextract.datastore.bag_extract._extract_nested_zip")
    def test_extract_full_file(self, mock_extract_zip):
        ds = self.get_test_object()
        files = ["/tmp_dir_name/full/fileA0001.xml", "/tmp_dir_name/full/fileA0002.xml"]

        with patch("gobbagextract.datastore.bag_extract.Path.glob", return_value=files):
            res = ds._extract_full_file(mock_afgifte)

        mock_extract_zip.assert_called_with(
            Path('/tmp_dir_name/BAGGEM1234L-15122021.zip'),
            ['1234GEM15122021.zip', '1234InOnderzoek15122021.zip', '1234IOOBJT15122021.zip'],
            Path("/tmp_dir_name/full"),
        )
        self.assertEqual(files, res)

        # Invalid filename
        with self.assertRaises(GOBException):
            ds._extract_full_file(mock_afgifte_gobexception)

    @patch("gobbagextract.datastore.bag_extract._extract_nested_zip")
    def test_extract_mutations_file(self, mock_extract_zip):
        ds = self.get_test_object()
        files = ["fileA0001.xml", "fileA0002.xml", "fileA0003.zip"]

        with patch("gobbagextract.datastore.bag_extract.Path.glob", return_value=files):
            res = ds._extract_mutations_file(mock_afgifte_mut)

        mock_extract_zip.assert_any_call(
            Path("/tmp_dir_name/BAGNLDM-15122021-16122021.zip"),
            ["9999MUT15122021-16122021.zip"],
            Path("/tmp_dir_name/mutations")
        )
        mock_extract_zip.assert_any_call(
            Path("/tmp_dir_name/BAGNLDM-15122021-16122021.zip"),
            ["9999IOMUT15122021-16122021.zip"],
            Path("/tmp_dir_name/mutations")
        )
        self.assertEqual(files, res)

        # Invalid filename
        with self.assertRaises(GOBException):
            ds._extract_mutations_file(mock_afgifte_gobexception)

    @patch("gobbagextract.datastore.bag_extract.ProductStore")
    @patch("gobbagextract.datastore.bag_extract.ElementTree")
    def test_get_mutation_ids(self, mock_et, mock_store):
        class MockElm:
            def __init__(self, text):
                self.text = text

        ds = self.get_test_object()
        ds._extract_full_file = MagicMock(return_value=["file1", "file2"])

        ds.read_config["last_full_download_location"] = "last/full/download/location"
        mock_et.parse.return_value.getroot.return_value.iterfind.return_value = [MockElm("id1"), MockElm("id2")]

        self.assertEqual(["id1", "id2", "id1", "id2"], list(ds._get_mutation_ids()))
        mock_store.method_calls = [call.download("last/full/download/location", destination="/tmp_dir_name")]

        ds._get_mutation_ids()
        mock_et.parse.assert_has_calls([call("file1"), call("file2")], any_order=True)

    @patch("gobbagextract.datastore.bag_extract.ProductStore")
    def test_connect(self, mock_store):
        ds = self.get_test_object()

        ds._extract_full_file = MagicMock()
        ds._extract_mutations_file = MagicMock()
        ds._get_mutation_ids = MagicMock()

        # full
        ds.connect()
        mock_store.download.assert_called_with(ds.read_config["download_location"], destination=ds.tmp_path)
        ds._extract_full_file.assert_called_with(ds.read_config["download_location"])
        ds._get_mutation_ids.assert_not_called()
        self.assertIsNone(ds.ids)

        # mutations
        ds.mode = ImportMode.MUTATIONS
        ds.connect()
        mock_store.download.assert_called_with(ds.read_config["download_location"], destination=ds.tmp_path)
        ds._get_mutation_ids.assert_called_once()
        ds._extract_mutations_file.assert_called_with(ds.read_config["download_location"])

    def test_disconnect(self):
        ds = self.get_test_object()
        ds.disconnect()
        ds.tmp_dir.cleanup.assert_called_once()

    def test_query_full(self):
        """Tests query if it it picks up full xml files."""
        read_config = {
            "object_type": "VBO",
            "xml_object": "Verblijfsobject",
            "mode": ImportMode.FULL,
            "gemeentes": ["0457"],
            "download_location": "the location",
        }
        date_now = datetime.datetime.now().date()
        ds = BagExtractDatastore({}, read_config, date_now)
        ds.files = [os.path.join(os.path.dirname(__file__), "bag_extract_fixtures", "full.xml")]
        res = list(ds.query(None))
        self.assertEqual(len(res), 1)

        expected = {
            "documentdatum": "1977-07-08",
            "documentnummer": "BVG-77-35/I",
            "gebruiksdoel": ["woonfunctie", "industriefunctie"],
            "geconstateerd": "N",
            "geometrie/punt": "POINT (132337 481363)",
            "heeftAlsHoofdadres/NummeraanduidingRef": "0424200000018004",
            "identificatie": "0424010000001635",
            "maaktDeelUitVan/PandRef": [
                "0424100000001609", "0424100000001610"
            ],
            "oppervlakte": "122",
            "status": "Verblijfsobject in gebruik",
            "voorkomen/Voorkomen/BeschikbaarLV/tijdstipEindRegistratieLV": "2017-11-06T11:31:11.351",
            "voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV": "2010-11-15T13:31:21.793",
            "voorkomen/Voorkomen/beginGeldigheid": "1977-07-08",
            "voorkomen/Voorkomen/eindGeldigheid": "2017-10-10",
            "voorkomen/Voorkomen/eindRegistratie": "2017-11-06T11:28:27.000",
            "voorkomen/Voorkomen/tijdstipRegistratie": "2010-11-15T13:24:58.000",
            "voorkomen/Voorkomen/voorkomenidentificatie": "1"
        }

        self.assertEqual(len(res), 1)
        self.assertEqual(date_now, res[0]["last_update"])
        self.assertEqual(expected, res[0]["object"])

    def test_query_inonderzoek(self):
        """Tests if query picks up inonderzoek xmls correctly."""
        read_config = {
            "object_type": "VBO",
            "xml_object": "Verblijfsobject",
            "mode": ImportMode.FULL,
            "gemeentes": ["0457"],
            "download_location": "the location",
        }
        date_now = datetime.datetime.now().date()
        ds = BagExtractDatastore({}, read_config, date_now)
        ds.files = [os.path.join(os.path.dirname(__file__), "bag_extract_fixtures", "full_inonderzoek.xml")]
        res = list(ds.query(None))
        self.assertEqual(len(res), 1)

        expected = {
            'kenmerk': 'oppervlakte',
            'identificatieVanVerblijfsobject': '0457010000000455',
            'inOnderzoek': 'J',
            'documentdatum': '1987-06-01',
            'documentnummer': 'BP-1987-33',
            'historieInOnderzoek/HistorieInOnderzoek/tijdstipRegistratie': '2010-11-26T08:20:55.000',
            'historieInOnderzoek/HistorieInOnderzoek/eindRegistratie': '2011-03-21T14:34:44.000',
            'historieInOnderzoek/HistorieInOnderzoek/beginGeldigheid': '1987-06-01',
            'historieInOnderzoek/HistorieInOnderzoek/eindGeldigheid': '2011-03-21',
            'historieInOnderzoek/HistorieInOnderzoek/BeschikbaarLVInOnderzoek/tijdstipRegistratieLV': '2010-11-26T08:31:36.223',
            'historieInOnderzoek/HistorieInOnderzoek/BeschikbaarLVInOnderzoek/tijdstipEindRegistratieLV': '2011-03-21T15:01:30.228'
        }

        self.assertEqual(len(res), 1)
        self.assertEqual(date_now, res[0]["last_update"])
        self.assertEqual(expected, res[0]["object"])

    def test_query_mutations(self):
        """Tests query, _element_to_dict, _flatten_dict, _flatten_nested_list and _gml_to_wkt

        :return:
        """
        read_config = {
            "object_type": "VBO",
            "xml_object": "Verblijfsobject",
            "mode": ImportMode.MUTATIONS,
            "gemeentes": ["0457"],
            "download_location": "the location",
            "last_full_download_location": "last full download",
        }
        ds = BagExtractDatastore({}, read_config, None)
        ds.files = [os.path.join(os.path.dirname(__file__), "bag_extract_fixtures", "mutations.xml")]
        ds.ids = ["0458010000059153123123123"]
        res = list(ds.query(None))

        expected = [{
            "documentdatum": "2010-09-28",
            "documentnummer": "Z.9704/D.6419",
            "gebruiksdoel": "kantoorfunctie",
            "geconstateerd": "N",
            "geometrie/punt": "POINT (129955.346 481542.434)",
            "heeftAlsHoofdadres/NummeraanduidingRef": "0458200000199376",
            "identificatie": "0458010000059153123123123",
            "maaktDeelUitVan/PandRef": "0457100000056661",
            "oppervlakte": "45",
            "status": "Verblijfsobject in gebruik",
            "voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV": "2010-11-26T09:01:30.485",
            "voorkomen/Voorkomen/beginGeldigheid": "2010-09-28",
            "voorkomen/Voorkomen/tijdstipRegistratie": "2010-11-26T08:49:50.000",
            "voorkomen/Voorkomen/voorkomenidentificatie": "1"
        }, {
            "documentdatum": "2010-09-28",
            "documentnummer": "Z.9704/D.6419",
            "gebruiksdoel": "kantoorfunctie",
            "geconstateerd": "N",
            "geometrie/punt": "POINT (129990.358 481557.42)",
            "heeftAlsHoofdadres/NummeraanduidingRef": "0457200000199395",
            "identificatie": "0457010000059164",
            "maaktDeelUitVan/PandRef": "0457100000056651",
            "oppervlakte": "51",
            "status": "Verblijfsobject in gebruik",
            "voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV": "2020-11-15T07:42:59.308",
            "voorkomen/Voorkomen/beginGeldigheid": "2020-11-14",
            "voorkomen/Voorkomen/tijdstipRegistratie": "2020-11-14T21:56:54.615",
            "voorkomen/Voorkomen/voorkomenidentificatie": "2"
        }, {
            "documentdatum": "2010-09-28",
            "documentnummer": "Z.9704/D.6419",
            "gebruiksdoel": "kantoorfunctiemutatie",
            "geconstateerd": "N",
            "geometrie/punt": "POINT (129990.358 481557.42)",
            "heeftAlsHoofdadres/NummeraanduidingRef": "0457200000199395",
            "identificatie": "0457010000059164",
            "maaktDeelUitVan/PandRef": "0457100000056651",
            "oppervlakte": "53",
            "status": "Verblijfsobject in gebruik mutatie",
            "voorkomen/Voorkomen/BeschikbaarLV/tijdstipRegistratieLV": "2010-11-26T09:01:30.671",
            "voorkomen/Voorkomen/beginGeldigheid": "2010-09-28",
            "voorkomen/Voorkomen/eindGeldigheid": "2020-11-14",
            "voorkomen/Voorkomen/eindRegistratie": "2020-11-14T21:56:54.615",
            "voorkomen/Voorkomen/tijdstipRegistratie": "2010-11-26T08:49:52.000",
            "voorkomen/Voorkomen/voorkomenidentificatie": "1"
        }]

        self.assertEqual(expected, [r["object"] for r in res])

    def test_query_mutations_inonderzoek(self):
        """Tests query, _element_to_dict, _flatten_dict, _flatten_nested_list and _gml_to_wkt

        :return:
        """
        read_config = {
            "object_type": "VBO",
            "xml_object": "Verblijfsobject",
            "mode": ImportMode.MUTATIONS,
            "gemeentes": ["0457"],
            "download_location": "the location",
            "last_full_download_location": "last full download",
        }

        ds = BagExtractDatastore({}, read_config, None)
        ds.files = [os.path.join(os.path.dirname(__file__), "bag_extract_fixtures", "mutations_inonderzoek.xml")]
        ds.ids = ["0458010000059153123123123"]
        res = list(ds.query(None))
        # last update nog?
        expected = [{
            'gemeente': '0457',
            'last_update': None,
            'object': {
                'KenmerkVerblijfsobjectInOnderzoek/documentdatum': '2022-01-17',
                 'KenmerkVerblijfsobjectInOnderzoek/documentnummer': '2022.007225 '
                                                                            'BAG',
                 'KenmerkVerblijfsobjectInOnderzoek/historieInOnderzoek/HistorieInOnderzoek/BeschikbaarLVInOnderzoek/tijdstipRegistratieLV': '2022-01-19T19:05:00.739',
                 'KenmerkVerblijfsobjectInOnderzoek/historieInOnderzoek/HistorieInOnderzoek/beginGeldigheid': '2022-01-17',
                 'KenmerkVerblijfsobjectInOnderzoek/historieInOnderzoek/HistorieInOnderzoek/tijdstipRegistratie': '2022-01-19T18:59:32.794',
                 'KenmerkVerblijfsobjectInOnderzoek/identificatieVanVerblijfsobject': '0935010000041035',
                 'KenmerkVerblijfsobjectInOnderzoek/inOnderzoek': 'J',
                 'KenmerkVerblijfsobjectInOnderzoek/kenmerk': 'gebruiksdoel'
            },
            'object_id': '0935010000041035'
        }]
        assert len(res) == 1
        assert res[0]["object_id"] == "0935010000041035"
        assert expected == res
