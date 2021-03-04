import datetime
from typing import Tuple

from gobcore.exceptions import GOBException

from gobbagextract.database.model import MutationImport
from gobbagextract.mutations.bagextract import BagExtractMutationsHandler


class MutationsHandler:
    HANDLERS = {
        "BAGExtract": BagExtractMutationsHandler,
    }

    def __init__(self, dataset: dict):
        self.dataset = dataset
        self.application = self.get_application(dataset)

        if self.application not in self.HANDLERS:
            raise GOBException(f"No handler defined for {self.application}")
        self.handler = self.HANDLERS[self.application]()

    @staticmethod
    def get_application(dataset: dict):
        return dataset.get('source', {}).get('application')

    @classmethod
    def is_mutations_import(cls, dataset: dict):
        application = cls.get_application(dataset)
        return application in cls.HANDLERS.keys()

    def get_next_import(self, last_import: MutationImport) -> Tuple[MutationImport, dict, datetime.date]:
        return self.handler.handle_import(last_import, self.dataset)

    def have_next(self, mutation_import: MutationImport):
        return self.handler.have_next(mutation_import, self.dataset)
