from collections import defaultdict
import json
import glob
import os
from typing import Optional, Dict, Any

# TODO: naming things

data_set_locations_mapping_cache: Optional[Dict[Any, Any]] = None


def get_extract_definition(
        catalogue: str, collection: str, gemeente: Optional[str] = None) -> Optional[dict]:
    data_set_locations_mapping = _build_data_set_locations_mapping()
    collection = data_set_locations_mapping.get(catalogue, {}).get(collection, {})
    print("collection?", collection)
    if not collection:
        raise Exception("Collection not found")
    if gemeente:
        return collection.get(gemeente)
    # Must have single gemeente
    return collection[list(collection.keys())[0]]


def _build_data_set_locations_mapping():
    DATA_DIR = os.environ.get(
        "BAG_DATA_CONFIG",
        os.path.join(os.path.dirname(__file__), '..', 'data')
    )
    if data_set_locations_mapping_cache is not None:
        return data_set_locations_mapping_cache

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
    # print(mapping)
    return mapping



