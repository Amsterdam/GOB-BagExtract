import os
from pathlib import Path
from unittest.mock import MagicMock

from sqlalchemy.orm import Session

from gobbagextract.__main__ import handle_bag_extract_message


def test_handle_bag_extract_message(database: Session, disable_gob_logger: MagicMock):
    os.environ["BAG_DATA_CONFIG"] = str(Path(__file__).parent / "fixtures" / "bag_data")
    message = {
        "header": {
            "catalogue": "bag_test",
            "collection": "ligplaatsen_test",
        }
    }
    msg = handle_bag_extract_message(message)
    print(msg)
