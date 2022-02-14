from collections import defaultdict

import io
import datetime as dt
import re
from enum import Enum

from xml.etree import ElementTree
from xml.etree.ElementTree import Element

from osgeo import ogr
from tempfile import TemporaryDirectory
from typing import List, Union, Iterator, Any, Generator, Callable
from zipfile import ZipFile
from pathlib import Path

from gobbagextract.mutations.afgifte import Afgifte
from gobbagextract.mutations.productstore import ProductStore
from gobcore.datastore.datastore import Datastore
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException


def _extract_nested_zip(zip_file, nested_zip_files: List[str], destination_dir: Path):
    """Extracts nested zip file from zip_file.

    Example:
    _extract_nested_zip("a.zip", ["b.zip", "c.zip"], "/tmp_dstdir")

    with:
    a.zip
        somefile_in_a
        other_file_in_a
        b.zip
            some_file_in_b
            c.zip
                some_file_in_c
                some_other_file_in_c
            some_other_file_in_b

    results in:
    /tmp_dst_dir/some_file_in_c
    /tmp_dst_dir/some_other_file_in_c

    :param zip_file:
    :param nested_zip_files:
    :param destination_dir:
    :return:
    """
    with ZipFile(zip_file, "r") as f:
        if len(nested_zip_files) == 0:
            f.extractall(destination_dir)
        else:
            with f.open(nested_zip_files[0], "r") as nested_zip_file:
                nested_zip_file_data = io.BytesIO(nested_zip_file.read())
                _extract_nested_zip(nested_zip_file_data, nested_zip_files[1:], destination_dir)


class ElementFormatter:
    ns_pattern = re.compile(r"{.*}")
    gml_namespace = "http://www.opengis.net/gml/3.2"

    def __init__(self, element: ElementTree.Element):
        self.element = element

    def get_dict(self):
        return self._flatten_dict(self._element_to_dict(self.element))

    @staticmethod
    def _gml_to_wkt(elm: ElementTree.Element) -> str:
        gml_str = ElementTree.tostring(elm).decode("utf-8")
        gml = ogr.CreateGeometryFromGML(gml_str)
        gml.FlattenTo2D()
        return gml.ExportToWkt()

    def _flatten_nested_list(self, lst: list, key_prefix: str) -> dict[str, Any]:
        """Flattens list, called from the _flatten_dict method. Pulls the dict keys in the list out.

        For example, when called with list [{"some_key": "A"}, {"some_key": "B"}] and key_prefix "prefix", the result
        is dict of the form:

        { "prefix/some_key": ["A", "B"] }

        :param lst:
        :param key_prefix:
        :return:
        """
        result = {}
        for item in lst:
            if isinstance(item, dict):
                # We have something of the form { ..., "key": [{"subkey": "A"}, {"subkey": "B"}], ... }
                # We transform this to { ..., "key/subkey": ["A", "B"], ... }
                d_item = self._flatten_dict(item)

                for d_key, d_value in d_item.items():
                    sub_key = f"{key_prefix}/{d_key}"
                    if sub_key not in result:
                        result[sub_key] = []

                    result[sub_key].append(d_value)
            else:
                result[key_prefix] = lst
        return result

    def _flatten_dict(self, d: dict) -> dict:
        """Flattens dictionary, separates keys by a / character.

        {
            "a": {
                "b": {
                    "c": "d",
                },
                "e": "f",
            "g": [{"h": 4}, {"h": 5}]
        }

        will become:
        {
            "a/b/c": "d",
            "a/e": "f",
            "g/h": [4, 5]
        }
        """
        def flatten(dct: dict) -> dict:
            """Recursively traverse dictionaries."""
            result = {}
            for key, value in dct.items():
                if isinstance(value, dict):

                    result |= {f"{key}/{k}": v for k, v in flatten(value).items()}
                elif isinstance(value, list):
                    result |= self._flatten_nested_list(value, key)
                else:
                    result[key] = value
            return result
        return flatten(d)

    def _element_to_dict(self, element: ElementTree.Element) -> Union[str, dict]:
        """Transforms an XML element to a dictionary."""
        childs = list(element)

        if len(childs) == 1 and self.gml_namespace in childs[0].tag:
            return self._gml_to_wkt(childs[0])

        elif childs:
            child_dicts = defaultdict(list)

            for child in childs:
                child_dicts[self.ns_pattern.sub("", child.tag)].append(self._element_to_dict(child))

            return {k: v[0] if len(v) == 1 else v for k, v in child_dicts.items()}

        else:
            return element.text.strip()


class BagFileTypes(Enum):
    """Enum describing the kind of file in the BAG zip."""
    FULL = 0
    MUTATIE = 1
    FULL_IN_ONDERZOEK = 2
    MUTATIE_IN_ONDERZOEK = 3


class BagExtractDatastore(Datastore):
    namespaces = {
        # Make sure namespaces do not change in the data source.
        "DatatypenNEN3610": "www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601",
        "Objecten": "www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601",
        "gml": "http://www.opengis.net/gml/3.2",
        "Historie": "www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601",
        "Objecten-ref": "www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601",
        "ml": "http://www.kadaster.nl/schemas/mutatielevering-generiek/1.0",
        "mlm": "http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-mutaties-lvc/v20200601",
        "nen5825": "www.kadaster.nl/schemas/lvbag/imbag/nen5825/v20200601",
        "KenmerkInOnderzoek": "www.kadaster.nl/schemas/lvbag/imbag/kenmerkinonderzoek/v20200601",
        "selecties-extract": "http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601",
        "sl-bag-extract": "http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-lvc/v20200601",
        "sl": "http://www.kadaster.nl/schemas/standlevering-generiek/1.0",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xs": "http://www.w3.org/2001/XMLSchema",
    }

    id_path = "Objecten:identificatie"
    seqnr_path = "Objecten:voorkomen/Historie:Voorkomen/Historie:voorkomenidentificatie"

    def __init__(self, connection_config: dict, read_config: dict, last_update: dt.date):
        super().__init__(connection_config, read_config)

        self.tmp_dir = TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.files = None
        self.ids = None
        self._last_update = last_update

        self._check_config()
        self._gemeente = read_config.get("gemeentes")[0]  # For now we only support Weesp

        xml_object = self.read_config.get("xml_object")
        # XML Paths to data which will be stored as json in the database.
        # full_xml_path is for files named like "0457VBO15012022-000001.xml"
        self.full_xml_path = f"./sl:standBestand/sl:stand/sl-bag-extract:bagObject/Objecten:{xml_object}"
        # full_xml_inonderzoek_path is for files named like "0457IOVBO15012022-000001"
        self.full_xml_inonderzoek_path = f"./sl:standBestand/sl:stand/sl-bag-extract:kenmerkInOnderzoek/KenmerkInOnderzoek:Kenmerk{xml_object}InOnderzoek"  # noqa: E501
        self.mutation_xml_paths = [
            # Ordering matters. First "toevoeging", then "wijziging"
            f"./ml:mutatieBericht/ml:mutatieGroep/ml:toevoeging/ml:wordt/mlm:bagObject/Objecten:{xml_object}",
            f"./ml:mutatieBericht/ml:mutatieGroep/ml:wijziging/ml:wordt/mlm:bagObject/Objecten:{xml_object}",
        ]
        self.mutation_xml_inonderzoek_path = f"./ml:mutatieBericht/ml:mutatieGroep//mlm:kenmerkInOnderzoek"

        self.mode = self.read_config["mode"]
        assert isinstance(self.mode, ImportMode), "mode should be of type ImportMode"

    def _check_config(self):
        for key in ("object_type", "xml_object", "mode", "gemeentes", "download_location"):
            if not self.read_config.get(key):
                raise GOBException(f"Missing {key} in read_config")

        if self.read_config["mode"] == ImportMode.MUTATIONS:
            if not self.read_config.get("last_full_download_location"):
                raise GOBException("Missing last_full_download_location in read_config")

    def _extract_full_file(self, afgifte: Afgifte) -> Iterator[Path]:
        object_type = self.read_config["object_type"]
        gemeente = afgifte.get_gemeente()
        datestr = afgifte.get_date().strftime("%d%m%Y")
        nested_zip_files = [f"{gemeente}GEM{datestr}.zip", f"{gemeente}{object_type}{datestr}.zip"]

        src_file = Path(self.tmp_path, afgifte.Bestandsnaam)
        dst_dir = Path(self.tmp_path, ImportMode.FULL.value)
        # Actual data
        _extract_nested_zip(src_file, nested_zip_files, dst_dir)

        # Kenmerken in onderzoek per object type.
        _extract_nested_zip(src_file, [
            f"{gemeente}GEM{datestr}.zip",
            f"{gemeente}InOnderzoek{datestr}.zip",
            f"{gemeente}IO{self.read_config['object_type']}{datestr}.zip",
        ], dst_dir)
        return dst_dir.glob("*.xml")

    def _extract_mutations_file(self, afgifte: Afgifte) -> Iterator[Path]:
        src_file = Path(self.tmp_path, afgifte.Bestandsnaam)
        dst_dir = Path(self.tmp_path, ImportMode.MUTATIONS.value)

        _extract_nested_zip(src_file, [f"9999MUT{afgifte.get_daterange()}.zip"], dst_dir)
        # Kenmerken in onderzoek
        _extract_nested_zip(src_file, [f"9999IOMUT{afgifte.get_daterange()}.zip"], dst_dir)
        return dst_dir.glob("*.xml")

    def _get_mutation_ids(self) -> Iterator[str]:
        """Get mutation ids."""
        afgifte = self.read_config["last_full_download_location"]
        ProductStore.download(afgifte, destination=self.tmp_path)

        for file in self._extract_full_file(afgifte):
            tree = ElementTree.parse(file)

            for elm in tree.getroot().iterfind(f"{self.full_xml_path}/{self.id_path}", self.namespaces):
                yield elm.text

    def connect(self):
        afgifte = self.read_config["download_location"]
        ProductStore.download(afgifte, destination=self.tmp_path)
        if self.mode == ImportMode.FULL:
            self.files = sorted(self._extract_full_file(afgifte))
        else:
            self.ids = set(self._get_mutation_ids())
            self.files = sorted(self._extract_mutations_file(afgifte))

    def disconnect(self):
        super().disconnect()
        self.tmp_dir.cleanup()

    def _determine_xml_format(self, xmlroot: Element) -> BagFileTypes:
        """Determine what kind of file in the zip it is.

        A full BAG zipfile contains many XML files, for verblijfsobjecten,
        ligplaatsen and also inonderzoek XML files for each entity. Determine
        what is the current file here.

        :param xmlroot: parsed XML file.
        :return: a BagFileType
        """
        if xmlroot.find(".//sl-bag-extract:kenmerkInOnderzoek", self.namespaces):
            return BagFileTypes.FULL_IN_ONDERZOEK
        elif xmlroot.find(".//ml:mutatieBericht//mlm:kenmerkInOnderzoek", self.namespaces):
            return BagFileTypes.MUTATIE_IN_ONDERZOEK
        elif xmlroot.find(".//ml:mutatieBericht//mlm:bagObject", self.namespaces):
            return BagFileTypes.MUTATIE
        else:
            # TODO: better to raise an exception, as it it might be unhandled XML data.
            return BagFileTypes.FULL

    def _get_object_id(self, element: Element, xml_format: BagFileTypes) -> None | str:
        """Get the object_id for the object being processed.

        The element to look for is different per entity type.

        :param element: An xml element.
        :param xml_format: what kind of XML file it is.
        :return:
        """
        if xml_format is BagFileTypes.FULL_IN_ONDERZOEK or xml_format is BagFileTypes.MUTATIE_IN_ONDERZOEK:
            id_path = f"KenmerkInOnderzoek:identificatieVan{self.read_config['xml_object']}"
            identificatie = element.find(f".//{id_path}", self.namespaces)
            return identificatie.text.strip()
        elif xml_format is BagFileTypes.MUTATIE:
            gemeentes = self.read_config.get("gemeentes", [])
            identificatie = element.find(f"./{self.id_path}", self.namespaces)
            identificatie = identificatie.text.strip() if identificatie is not None else None

            # Filter by id, or by gemeentecode prefix (first 4 digits)
            if identificatie and (identificatie in self.ids or identificatie[:4] in gemeentes):
                volgnummer = element.find(f"./{self.seqnr_path}", self.namespaces)
                return identificatie if volgnummer is None else f"{identificatie}.{volgnummer.text.strip()}"

            # In specific cases there is no object_id
            return None
        elif xml_format is BagFileTypes.FULL:
            identificatie = element.find(f"./{self.id_path}", self.namespaces)
            identificatie = identificatie.text.strip() if identificatie is not None else None
            volgnummer = element.find(f"./{self.seqnr_path}", self.namespaces)
            return identificatie if volgnummer is None else f"{identificatie}.{volgnummer.text.strip()}"

        raise ValueError(f"Unknown XML format: {xml_format}, can not get object id.")

    def _get_elements_full(
            self, xmlroot: Element, xml_format: BagFileTypes
    ) -> Generator[Element, None, None]:
        """Get all elements for the full bag zip.

        :param xmlroot: parsed root of the xml.
        :param xml_format: what kind of XML file it is.
        :return: a generator which yields elements.
        """
        yield from xmlroot.iterfind(self.full_xml_path, self.namespaces)

    def _get_elements_full_inonderzoek(
            self, xmlroot: Element, xml_format: BagFileTypes
    ) -> Generator[Element, None, None]:
        """Get al elements which are 'in onderzoek' for the full bag zip.

        :param xmlroot: parsed root of the xml.
        :param xml_format: what kind of XML file it is.
        :return: a generator which yields elements.
        """
        yield from xmlroot.iterfind(self.full_xml_inonderzoek_path, self.namespaces)

    def _get_elements_mutations(
            self, xmlroot: Element, xml_format: BagFileTypes
    ) -> Generator[Element, None, None]:
        """Get all important elements from the mutations XML file.

        :param xmlroot: parsed root of the xml.
        :param xml_format: what kind of XML file it is.
        """
        assert self.ids is not None, "self.ids should be initialised"
        # Collect mutations in dict. Only keep last mutation for an object.
        # This is why mutation_xml_paths should first visit additions, then modifications
        mutations = {}
        for path in self.mutation_xml_paths:
            for element in xmlroot.iterfind(path, self.namespaces):
                object_id = self._get_object_id(element, xml_format)
                if object_id is not None:
                    mutations[object_id] = element

        for mutation in mutations.values():
            yield mutation

    def _get_elements_mutations_inonderzoek(
            self, xmlroot: Element, xml_format: BagFileTypes
    ) -> Generator[Element, None, None]:
        """Get all elements which are 'in onderzoek' from the mutations XML file.

        :param xmlroot: parsed root of the xml.
        :param xml_format: what kind of XML file it is.
        """
        for element in xmlroot.iterfind(self.mutation_xml_inonderzoek_path, self.namespaces):
            yield element

    def _pack_object(self, row, object_id) -> dict:
        return {
            "gemeente": self._gemeente,
            "last_update": self._last_update,
            "object_id": object_id,
            "object": row,
        }

    def _parse_elements(self, xml_format: BagFileTypes) -> Callable[
            [Element, BagFileTypes],
            Generator[Element, None, None]
        ]:
        if xml_format == xml_format.FULL:
            return self._get_elements_full
        elif xml_format == xml_format.FULL_IN_ONDERZOEK:
            return self._get_elements_full_inonderzoek
        elif xml_format == xml_format.MUTATIE:
            return self._get_elements_mutations
        elif xml_format == xml_format.MUTATIE_IN_ONDERZOEK:
            return self._get_elements_mutations_inonderzoek
        else:
            raise ValueError(f"No '_get_elements' method specified for {xml_format}")

    def query(self, query, **kwargs) -> Generator[dict, None, None]:
        """Yield XML values from 'sl:stand' as a json dictionary.

        :param query: Ignored
        :param kwargs: Ignored kwargs
        """
        for file in self.files:
            tree = ElementTree.parse(file)
            xmlroot = tree.getroot()
            xml_format = self._determine_xml_format(xmlroot)
            get_elements_fn = self._parse_elements(xml_format)

            for element in get_elements_fn(xmlroot, xml_format):
                row = ElementFormatter(element).get_dict()
                object_id = self._get_object_id(element=element, xml_format=xml_format)
                yield self._pack_object(row, object_id)
