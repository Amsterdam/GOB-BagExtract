import datetime
from typing import Dict, Any

import sys

from gobbagextract.config import BAGEXTRACT_NOT_AVAIL_DAYS_ERROR, BAGEXTRACT_NOT_AVAIL_DAYS_WARNING
from gobbagextract.database.connection import connect
from gobbagextract.database.repository import MutationImportRepository, MutationImport
from gobbagextract.database.session import DatabaseSession
from gobbagextract.extract_config.extract_config import get_extract_definition
from gobbagextract.mutations.exception import NothingToDo
from gobbagextract.mutations.handler import MutationsHandler
from gobbagextract.prepare.prepare_client import PrepareClient
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, BAG_EXTRACT_QUEUE, BAG_EXTRACT_RESULT_KEY
from gobcore.message_broker.messagedriven_service import messagedriven_service


def _log_no_more_left(last_import: MutationImport):
    if last_import is None:
        logger.error("No mutations available and no mutations stored yet")
    else:
        logger.info(f"Last processed mutation is {last_import.filename}")
        interval = datetime.datetime.now() - last_import.ended_at
        if interval.days > BAGEXTRACT_NOT_AVAIL_DAYS_ERROR:
            logger.error(f"No mutations available for {interval} days")
        elif interval.days > BAGEXTRACT_NOT_AVAIL_DAYS_WARNING:
            logger.warning(f"No mutation available, last mutation was {interval} ago")


def _handle_mutation_import(msg: dict, dataset: dict, mutations_handler: MutationsHandler) -> [str, bool]:
    """The dataset source is marked as a mutations import. Let the MutationsHandler decide what to import and
    which mode to use.

    MutationsHandler returns a new MutationsImport object and the updated dataset configuration to use for this
    import

    returns: Message with summary  and bool if more mutations are available
    """
    logger.info("Have mutations import. Determine next step")
    with DatabaseSession() as session:
        repo = MutationImportRepository(session)
        # If there is no last import, it switches automatically to full
        last_import = repo.get_last(
            dataset.get("catalogue"),
            dataset.get("entity"),
            dataset.get("source", {}).get("application")
        )
        try:
            mutation_import, updated_dataset, mutation_date = mutations_handler.get_next_import(last_import)
        except NothingToDo as e:
            logger.info(f"Nothing to do: {e}")
            _log_no_more_left(last_import)
            msg = {
                "header": msg.get("header", {}),
                "summary": logger.get_summary(),
            }
            return msg, False

        repo.save(mutation_import)
        logger.info(f"File to be imported is {mutation_import.filename}")

        dataset = updated_dataset
        mode = ImportMode(mutation_import.mode)

        prepare_client = PrepareClient(msg, dataset, mode, mutation_date)

        msg = prepare_client.import_dataset()
        mutation_import.ended_at = datetime.datetime.utcnow()

        repo.save(mutation_import)
        logger.info("Mutation import ended. Saving state in database")

        next_mutations = mutations_handler.have_next(mutation_import)
    return msg, next_mutations


def handle_bag_extract_message(msg: dict) -> dict:
    """Handles messages to download BAG extract data with.

    Example message:
        message = {
           "header": {
              "catalogue": "some catalogue",
              "collection": "the collection",
           }
        }



    :param msg: message with "catalogue" and "collection" keys.
    :returns: a message with the result of the message handling.
    """
    _validate_message(msg)
    dataset = get_extract_definition(
        collection=msg["header"]["collection"],
        catalogue=msg["header"]["catalogue"]
    )
    msg["header"] |= {
        "source": dataset["source"]["name"],
        "application": dataset["source"]["application"],
        "catalogue": dataset["catalogue"],
        "entity": dataset["entity"],
    }
    logger.configure(msg, "BAG EXTRACT")
    mutations_handler = MutationsHandler(dataset)
    next_mutation = True
    while next_mutation:
        msg, next_mutation = _handle_mutation_import(msg, dataset, mutations_handler)
        if next_mutation:
            logger.info("Next mutation is available, keep processing")
    logger.info("This was the last file to be exctracted for now.")
    return msg


def _validate_message(msg: Dict[str, Any]) -> None:
    """Validates the incoming message.

    Messages should have `required_keys` set.

    Example message:
        message = {
           "header": {
              "catalogue": "some catalogue",
              "collection": "the collection",
           }
        }

    Where "application" is optional when there is only one known application for given catalogue and collection

    :param msg: A message containing a catalogue and collection
    :raises: GOBException when the message does not validate.
    """
    required_keys = ["catalogue", "collection"]
    if "header" not in msg:
        raise GOBException("No 'header' key in message.")

    if not all([key in msg["header"] for key in required_keys]):
        raise GOBException(
            f"Missing dataset keys in message. Message: {msg['header']}. "
            f"Expected keys: {', '.join(required_keys)}."
        )


SERVICEDEFINITION = {
    "bag_extract_request": {
        "queue": BAG_EXTRACT_QUEUE,
        "handler": handle_bag_extract_message,
        "report": {
            "exchange": WORKFLOW_EXCHANGE,
            "key": BAG_EXTRACT_RESULT_KEY,
        },
    },
}


def init():
    connect()
    if len(sys.argv) == 1:
        messagedriven_service(SERVICEDEFINITION, "BagExtract")
    else:
        for collection in sys.argv[1:]:
            msg = {
                "header": {
                    "catalogue": "bag",
                    "collection": collection,
                }
            }
            handle_bag_extract_message(msg)


if __name__ == "__main__":  # pragma: no cover
    init()
