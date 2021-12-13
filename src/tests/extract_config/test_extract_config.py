import json
import os
from pathlib import Path

import pytest

from gobbagextract.extract_config.extract_config import get_extract_definition
from gobbagextract.extract_config import extract_config


class TestExtractConfig:

    def test_get_extract_definition(self, tests_dir):
        os.environ["BAG_DATA_CONFIG"] = str(tests_dir / "fixtures" / "bag_data")
        gemeente = '0457'
        def1 = get_extract_definition('bag', 'ligplaatsen', gemeente)
        def2 = json.loads(Path(tests_dir, "fixtures", "bag_data", "bag.test.json").read_text())
        assert def1 == def2
        def1 = get_extract_definition('bag', 'ligplaatsen')
        assert def1 == def2

    @pytest.mark.parametrize(
        "catalogue, collection", [
            ("fake_catalogue", "ligplaatsen"),
            ("bag", "fake_collection"),
            ("fake_catalogue", "fake_collection"),
        ]
    )
    def test_get_extract_definition_collection_not_found(self, catalogue, collection, tests_dir):
        gemeente = '0457'
        m = f"No collections found for catalogue: {catalogue}, collection: {collection}"
        with pytest.raises(Exception, match=m):
            get_extract_definition(catalogue, collection, gemeente)

    def test_get_extract_definition_uses_cache(self):
        gemeente = '0457'
        # Make sure cache is empty
        assert extract_config.data_set_locations_mapping_cache is None
        # Cache mapping, make sure a mapping is set and store memory id
        get_extract_definition('bag', 'ligplaatsen', gemeente)
        cached_id = id(extract_config.data_set_locations_mapping_cache)
        assert extract_config.data_set_locations_mapping_cache is not None
        # Get mapping and see if memory id of did not change
        get_extract_definition('bag', 'ligplaatsen', gemeente)
        assert id(extract_config.data_set_locations_mapping_cache) == cached_id
