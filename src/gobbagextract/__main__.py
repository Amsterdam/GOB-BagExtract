import sys
import datetime

from gobcore.message_broker.config import WORKFLOW_EXCHANGE, BAG_EXTRACT_QUEUE, BAG_EXTRACT_RESULT_KEY

from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobbagextract.extract_config.extract_config import get_extract_definition
from gobbagextract.database.connection import connect
from gobbagextract.database.session import DatabaseSession
from gobbagextract.database.repository import MutationImportRepository
from gobbagextract.mutations.exception import NothingToDo

from gobbagextract.mutations.handler import MutationsHandler
from gobbagextract.prepare.prepare_client import PrepareClient


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
        last_import = repo.get_last(dataset.get('catalogue'), dataset.get('entity'), dataset.get('source', {})
                                    .get('application'))
        try:
            mutation_import, updated_dataset, mutation_date = mutations_handler.get_next_import(last_import)
        except NothingToDo as e:
            logger.info(f"Nothing to do: {e}")
            msg['summary'] = logger.get_summary()
            return msg, False

        repo.save(mutation_import)
        logger.info(f"File to be imported is {mutation_import.filename}")

        dataset = updated_dataset
        mode = ImportMode(mutation_import.mode)

        msg = prepare_client = PrepareClient(msg, dataset, mode, mutation_date)

        prepare_client.import_dataset()
        mutation_import.ended_at = datetime.datetime.utcnow()

        repo.save(mutation_import)
        logger.info("Mutation import ended. Saving state in database")

        next_mutations = mutations_handler.have_next(mutation_import)
    return msg, next_mutations


def handle_bag_extract_message(msg: dict) -> dict:

    dataset = _extract_dataset_from_msg(msg)
    msg['header'] |= {
        'source': dataset['source']['name'],
        'application': dataset['source']['application'],
        'catalogue': dataset['catalogue'],
        'entity': dataset['entity'],
    }
    logger.configure(msg, "BAG EXTRACT")
    mutations_handler = MutationsHandler(dataset)
    next_mutation = True
    while next_mutation:
        msg, next_mutation = _handle_mutation_import(msg, dataset, mutations_handler)
        if next_mutation:
            logger.info('Next mutation is available, keep processing')
    logger.info("This was the last file to be exctracted for now.")
    return msg


def _extract_dataset_from_msg(msg):
    """Returns location of dataset file from msg.

    Example message:

    message = {
       "header": {
          "catalogue": "some catalogue",
          "collection": "the collection",
       }
    }

    Where 'application' is optional when there is only one known application for given catalogue and collection

    :param msg:
    :return:
    """

    required_keys = ['catalogue', 'collection']
    header = msg.get('header', {})

    if not all([key in header for key in required_keys]):
        raise GOBException(f"Missing dataset keys. Expected keys: {','.join(required_keys)}")
    return get_extract_definition(header['catalogue'], header['collection'])


SERVICEDEFINITION = {
    'bag_extract_request': {
        'queue': BAG_EXTRACT_QUEUE,
        'handler': handle_bag_extract_message,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': BAG_EXTRACT_RESULT_KEY,
        },
    },
}


def init():
    if __name__ == "__main__":
        connect()
        if len(sys.argv) == 1:
            messagedriven_service(SERVICEDEFINITION, "BagExtract")
        else:
            for collection in sys.argv[1:]:
                msg = {
                    'header': {
                        'catalogue': 'bag',
                        'collection': collection,
                    }
                }
                handle_bag_extract_message(msg)


init()
