from collections import defaultdict
import json
import glob
import os
from typing import Optional

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')


def get_extract_definition(
        catalogue: str, collection: str, gemeente: Optional[str] = None) -> Optional[dict]:
    collection = data_set_locations_mapping.get(catalogue, {}).get(collection, {})
    if gemeente:
        return collection.get(gemeente)
    # Must have single gemeente
    return collection[list(collection.keys())[0]]


def _build_data_set_locations_mapping():
    def nested_dict():
        return defaultdict(nested_dict)

    mapping = nested_dict()
    definitions = [(json.load(open(f)), f) for f in glob.glob(os.path.join(DATA_DIR, '*.json'))]
    for j, f in definitions:
        d = json.load(open(f))
        collection = d['entity']
        catalogue = d['catalogue']
        for gemeente in d['source']['read_config']['gemeentes']:
            mapping[catalogue][collection][gemeente] = d
    return mapping


data_set_locations_mapping = _build_data_set_locations_mapping()
