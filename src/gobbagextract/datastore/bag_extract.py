from collections import defaultdict

import io
import datetime
import os
import re
import requests
import xml.etree.ElementTree as ET
from osgeo import ogr
from tempfile import TemporaryDirectory
from typing import List
from zipfile import ZipFile

from gobcore.datastore.datastore import Datastore
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException


def _extract_nested_zip(zip_file, nested_zip_files: List[str], destination_dir: str):
    """Extracts nested zip file from zip_file.

    Example:
    _extract_nested_zip('a.zip', ['b.zip', 'c.zip'], '/tmp_dstdir')

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
    with ZipFile(zip_file, 'r') as f:
        if len(nested_zip_files) == 0:
            f.extractall(destination_dir)
        else:
            with f.open(nested_zip_files[0], 'r') as nested_zip_file:
                nested_zip_file_data = io.BytesIO(nested_zip_file.read())
                _extract_nested_zip(nested_zip_file_data, nested_zip_files[1:], destination_dir)


class ElementFormatter:
    ns_pattern = re.compile(r'{.*}')
    gml_namespace = "http://www.opengis.net/gml/3.2"

    def __init__(self, element):
        self.element = element

    def get_dict(self):
        return self._flatten_dict(self._element_to_dict(self.element))

    def _gml_to_wkt(self, elm):
        gml_str = ET.tostring(elm).decode('utf-8')
        gml = ogr.CreateGeometryFromGML(gml_str)
        gml.FlattenTo2D()
        return gml.ExportToWkt()

    def _flatten_nested_list(self, lst: list, key_prefix: str):
        """Flattens list, called from the _flatten_dict method. Pulls the dict keys in the list out.

        For example, when called with list [{'some_key': 'A'}, {'some_key': 'B'}] and key_prefix 'prefix', the result
        is dict of the form:

        { 'prefix/some_key': ['A', 'B'] }

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

    def _flatten_dict(self, d: dict):
        """Flattens dictionary, separates keys by a / character.

        {
            'a': {
                'b': {
                    'c': 'd',
                },
                'e': 'f',
            'g': [{'h': 4}, {'h': 5}]
        }

        will become:

        {
            'a/b/c': 'd',
            'a/e': 'f',
            'g/h': [4, 5]
        }

        :param d:
        :return:
        """

        def flatten(dct: dict):
            result = {}
            for key, value in dct.items():
                if isinstance(value, dict):
                    # Recursively traverse dictionaries
                    result.update({f"{key}/{k}": v for k, v in flatten(value).items()})
                elif isinstance(value, list):
                    result.update(self._flatten_nested_list(value, key))
                else:
                    result[key] = value
            return result

        return flatten(d)

    def _element_to_dict(self, element: ET.Element):
        """Transforms an XML element to a dictionary.

        :param element:
        :return:
        """
        childs = list(element)

        if len(childs) == 1 and self.gml_namespace in childs[0].tag:
            return self._gml_to_wkt(childs[0])
        elif childs:
            child_dicts = defaultdict(list)

            for child in childs:
                child_dicts[re.sub(self.ns_pattern, '', child.tag)].append(self._element_to_dict(child))

            return {k: v[0] if len(v) == 1 else v for k, v in child_dicts.items()}

        else:
            return element.text.strip()


class BagExtractDatastore(Datastore):
    namespaces = {
        # We could extract namespaces from the file, but this way we're sure they won't change in the source.
        'DatatypenNEN3610': 'www.kadaster.nl/schemas/lvbag/imbag/datatypennen3610/v20200601',
        'Objecten': 'www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601',
        'gml': 'http://www.opengis.net/gml/3.2',
        'Historie': 'www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601',
        'Objecten-ref': 'www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601',
        'ml': 'http://www.kadaster.nl/schemas/mutatielevering-generiek/1.0',
        'mlm': 'http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-mutaties-lvc/v20200601',
        'nen5825': 'www.kadaster.nl/schemas/lvbag/imbag/nen5825/v20200601',
        'KenmerkInOnderzoek': 'www.kadaster.nl/schemas/lvbag/imbag/kenmerkinonderzoek/v20200601',
        'selecties-extract': 'http://www.kadaster.nl/schemas/lvbag/extract-selecties/v20200601',
        'sl-bag-extract': 'http://www.kadaster.nl/schemas/lvbag/extract-deelbestand-lvc/v20200601',
        'sl': 'http://www.kadaster.nl/schemas/standlevering-generiek/1.0',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'xs': 'http://www.w3.org/2001/XMLSchema',
    }

    id_path = "Objecten:identificatie"
    seqnr_path = "Objecten:voorkomen/Historie:Voorkomen/Historie:voorkomenidentificatie"

    def __init__(self, connection_config: dict, read_config: dict, last_update: datetime.date):
        super().__init__(connection_config, read_config)

        self.tmp_dir = TemporaryDirectory()
        self.files = []
        self.ids = None

        self._check_config()

        xml_object = self.read_config.get('xml_object')
        self._last_update = last_update
        # For now we oly support Weesp
        self._gemeente = read_config.get('gemeentes')[0]
        self.full_xml_path = f"./sl:standBestand/sl:stand/sl-bag-extract:bagObject/Objecten:{xml_object}"
        self.mutation_xml_paths = [
            # Ordering matters. First 'toevoeging', then 'wijziging'
            f"./ml:mutatieBericht/ml:mutatieGroep/ml:toevoeging/ml:wordt/mlm:bagObject/Objecten:{xml_object}",
            f"./ml:mutatieBericht/ml:mutatieGroep/ml:wijziging/ml:wordt/mlm:bagObject/Objecten:{xml_object}",
        ]

        self.mode = self.read_config['mode']
        assert isinstance(self.mode, ImportMode), "mode should be of type ImportMode"

    def _check_config(self):
        for key in ('object_type', 'xml_object', 'mode', 'gemeentes', 'download_location'):
            if not self.read_config.get(key):
                raise GOBException(f"Missing {key} in read_config")

        if self.read_config['mode'] == ImportMode.MUTATIONS:
            if not self.read_config.get("last_full_download_location"):
                raise GOBException("Missing last_full_download_location in read_config")

    def _extract_full_file(self, file_location: str):
        filename = file_location.split('/')[-1]
        match = re.match(r'^BAGGEM(\d{4})L-(\d{8}).zip$', filename)

        if not match:
            raise GOBException(f"Unexpected filename format: {filename}")

        gemeente = match.group(1)
        datestr = match.group(2)

        dst_dir = os.path.join(self.tmp_dir.name, 'full')
        _extract_nested_zip(file_location, [
            f"{gemeente}GEM{datestr}.zip",
            f"{gemeente}{self.read_config['object_type']}{datestr}.zip",
        ], dst_dir)

        return [os.path.join(dst_dir, f)
                for f in sorted(os.listdir(dst_dir)) if f.endswith('.xml')]

    def _extract_mutations_file(self, file_location: str):
        filename = file_location.split('/')[-1]
        match = re.match(r'^BAGNLDM-(\d{8}-\d{8}).zip$', filename)

        if not match:
            raise GOBException(f"Unexpected filename format: {filename}")

        daterange = match.group(1)
        dst_dir = os.path.join(self.tmp_dir.name, 'mutations')
        _extract_nested_zip(file_location, [
            f"9999MUT{daterange}.zip",
        ], dst_dir)

        return [os.path.join(dst_dir, f)
                for f in sorted(os.listdir(dst_dir)) if f.endswith('.xml')]

    def _get_mutation_ids(self):
        """

        :return:
        """
        last_full_location = self._download_file(self.read_config['last_full_download_location'])
        full_files = self._extract_full_file(last_full_location)

        ids = []
        for file in full_files:
            tree = ET.parse(file)
            for elm in tree.getroot().iterfind(f"{self.full_xml_path}/{self.id_path}", self.namespaces):
                ids.append(elm.text)
        return ids

    def connect(self):
        file_location = self._download_file(self.read_config['download_location'])

        if self.mode == ImportMode.FULL:
            self.files = self._extract_full_file(file_location)
        else:
            self.ids = self._get_mutation_ids()
            self.files = self._extract_mutations_file(file_location)

    def disconnect(self):
        pass  # pragma: no cover

    def _download_file(self, file_location: str):
        """Downloads source file and returns file location

        :return:
        """
        fname = file_location.split('/')[-1]
        download_location = os.path.join(self.tmp_dir.name, fname)

        resp = requests.get(file_location)
        resp.raise_for_status()

        with open(download_location, 'wb') as f:
            f.write(resp.content)

        return download_location

    def _get_elements_full(self, xmlroot):
        yield from xmlroot.iterfind(self.full_xml_path, self.namespaces)

    def _get_elements_mutations(self, xmlroot):
        assert self.ids is not None, "self.ids should be initialised"

        gemeentes = self.read_config.get('gemeentes', [])

        # Collect mutations in dict. Only keep last mutation for an object.
        # This is why mutation_xml_paths should first visit additions, then modifications
        mutations = {}
        for path in self.mutation_xml_paths:

            for element in xmlroot.iterfind(path, self.namespaces):
                identificatie = element.find(f"./{self.id_path}", self.namespaces)
                identificatie = identificatie.text.strip() if identificatie is not None else None

                # Filter by id, or by gemeentecode prefix (first 4 digits)
                if identificatie and (identificatie in self.ids or identificatie[:4] in gemeentes):
                    volgnummer = element.find(f"./{self.seqnr_path}", self.namespaces)

                    object_id = identificatie \
                        if volgnummer is None \
                        else f"{identificatie}.{volgnummer.text.strip()}"
                    mutations[object_id] = element

        for mutation in mutations.values():
            yield mutation

    def _pack_object(self, row, object_id):
        return {
            'gemeente': self._gemeente,
            'last_update': self._last_update,
            'object_id': object_id,
            'object': row,
        }

    def query(self, query):
        get_elements_fn = self._get_elements_full if self.mode == ImportMode.FULL else self._get_elements_mutations

        for file in self.files:
            tree = ET.parse(file)

            for element in get_elements_fn(tree.getroot()):
                row = ElementFormatter(element).get_dict()
                identificatie = element.find(f"./{self.id_path}", self.namespaces)
                identificatie = identificatie.text.strip() if identificatie is not None else None
                volgnummer = element.find(f"./{self.seqnr_path}", self.namespaces)

                object_id = identificatie if volgnummer is None else f'{identificatie}.{volgnummer.text.strip()}'
                yield self._pack_object(row, object_id)
