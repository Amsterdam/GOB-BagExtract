import logging
import os
from pathlib import Path
from unittest import mock

import pytest

from gobbagextract.__main__ import handle_bag_extract_message
from gobbagextract.database.connection import connect
from gobcore.logging.logger import Logger

@pytest.fixture
def disable_gob_logger():
    with mock.patch("gobbagextract.__main__.logger") as p:
        yield p

@pytest.fixture
def database():
    if not connect():
        raise Exception("dikke error")


def test_handle_bag_extract_message(database, disable_gob_logger):
    logging.setLoggerClass(logging.Logger)
    print(dir(logging.Logger))
    print(dir(Logger))
    print(logging.getLoggerClass())
    message = {
       "header": {
          "catalogue": "bag_test",
          "collection": "ligplaatsen_test",
       }
    }
    os.environ["BAG_DATA_CONFIG"] = str(Path(__file__).parent / "fixtures" / "bag_data")
    print(message)
    msg = handle_bag_extract_message(message)
    print(msg)
