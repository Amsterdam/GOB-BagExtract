import os
from pathlib import Path
from unittest.mock import MagicMock

from sqlalchemy.orm import Session

from gobbagextract.__main__ import handle_bag_extract_message
from gobbagextract.database.model import MutationImport
from gobbagextract.database.repository import MutationImportRepository
from gobcore.enum import ImportMode


def test_handle_bag_extract_message_inserts_mutation(database: Session, gob_logger_mock: MagicMock):
    os.environ["BAG_DATA_CONFIG"] = str(Path(__file__).parent / "fixtures" / "bag_data")
    message = {
        "header": {
            "catalogue": "bag_test",
            "collection": "ligplaatsen_test",
        }
    }
    repo = MutationImportRepository(database)
    obj = MutationImport(
        mode=ImportMode.MUTATIONS.value,
        filename="data/somefile.json",
    )
    updated_obj = repo.save(obj)
    handle_bag_extract_message(message)
    # Check if mutation is inserted in the database
    assert repo.get(updated_obj.id).filename == obj.filename
    # Check if no warnings or errors where logged.
    assert gob_logger_mock.get_warnings.call_count == 0
    assert gob_logger_mock.get_errrors.call_count == 0
