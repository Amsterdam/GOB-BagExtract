from collections import defaultdict
import json
import glob
import os
from typing import Optional, Dict, Any

data_set_locations_mapping_cache: Optional[Dict[Any, Any]] = None


def get_extract_definition(
        catalogue: str, collection: str, gemeente: Optional[str] = None) -> Optional[dict]:
    data_set_locations_mapping = _build_data_set_locations_mapping()
    collections = data_set_locations_mapping.get(catalogue, {}).get(collection, {})

    if not collections:
        raise Exception(f"No collections found for catalogue: {catalogue}, collection: {collection}.")

    if gemeente:
        return collections.get(gemeente)
    # Must have single gemeente
    return collections[list(collections.keys())[0]]


def _build_data_set_locations_mapping():
    global data_set_locations_mapping_cache
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

    data_set_locations_mapping_cache = mapping
    return data_set_locations_mapping_cache
