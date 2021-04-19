import datetime
import re
from typing import Tuple

import htmllistparse

from dateutil.relativedelta import relativedelta
from gobcore.exceptions import GOBException
from gobcore.enum import ImportMode
from gobbagextract.config import BAGEXTRACT_DOWNLOAD_URL
from gobbagextract.database.model import MutationImport
from gobbagextract.mutations.exception import NothingToDo


class BagExtractMutationsHandler:
    # Full import every 15th of the month
    FULL_IMPORT_DAY = 15

    def _last_full_import_date(self, date: datetime.date):
        if date.day < self.FULL_IMPORT_DAY:
            date -= relativedelta(months=1)
        return date.replace(day=self.FULL_IMPORT_DAY)

    def _date_gemeente_from_filename(self, filename: str) -> datetime.date:
        m = re.match(r"^BAGGEM(\d{4})L-(\d{2})(\d{2})(\d{4}).zip$", filename)
        if m:
            return datetime.date(int(m.group(4)), int(m.group(3)), int(m.group(2))), m.group(1)
        m = re.match(r"^BAGNLDM-\d{8}-(\d{2})(\d{2})(\d{4}).zip$", filename)
        if m:
            return datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1))), None

        raise GOBException(f"Could not parse filename. Unknown format: {filename}")

    def _datestr(self, date: datetime.date):
        return date.strftime("%d%m%Y")

    def _get_full_download_path(self, gemeente: str):
        return f"{BAGEXTRACT_DOWNLOAD_URL}/Gemeente LVC/{gemeente}/"

    def _get_mutations_download_path(self):
        return f"{BAGEXTRACT_DOWNLOAD_URL}/Nederland dagmutaties/"

    def _list_path(self, path: str):
        _, listing = htmllistparse.fetch_listing(path)
        return [entry.name for entry in listing]

    def _get_available_full_downloads(self, gemeente: str):
        return self._list_path(self._get_full_download_path(gemeente))

    def _get_available_mutation_downloads(self):
        return self._list_path(self._get_mutations_download_path())

    def restart_import(self, last_import: MutationImport) -> Tuple[ImportMode, str, datetime.date]:
        mode = last_import.mode
        date, gemeente = self._date_gemeente_from_filename(last_import.filename)

        if mode == ImportMode.FULL.value:
            ret = self.start_full(date, gemeente)
        else:
            ret = self.start_mutations(date)

        return ret + (date, )

    def start_next(self, last_import: MutationImport, gemeente: str) -> Tuple[ImportMode, str, datetime.date]:
        date, _ = self._date_gemeente_from_filename(last_import.filename)
        next_date = date + datetime.timedelta(days=1)

        if next_date.day == self.FULL_IMPORT_DAY:
            ret = self.start_full(next_date, gemeente)
        else:
            ret = self.start_mutations(next_date)
        return ret + (next_date, )

    def _full_filename(self, date: datetime.date, gemeente: str) -> str:
        return f"BAGGEM{gemeente}L-{self._datestr(date)}.zip"

    def _mutations_filename(self, date: datetime.date) -> str:
        return f"BAGNLDM-{self._datestr(date - datetime.timedelta(days=1))}-{self._datestr(date)}.zip"

    def start_full(self, date: datetime.date, gemeente: str) -> Tuple[ImportMode, str]:
        fname = self._full_filename(date, gemeente)

        if fname not in self._get_available_full_downloads(gemeente):
            # TODO: This implies when file is not yet availble on the 15th we
            # are failing this workflow!
            raise NothingToDo.file_not_available(fname)

        return ImportMode.FULL, fname

    def start_mutations(self, date: datetime.date) -> Tuple[ImportMode, str]:
        fname = self._mutations_filename(date)

        if fname not in self._get_available_mutation_downloads():
            raise NothingToDo.file_not_available(fname)

        return ImportMode.MUTATIONS, fname

    def _get_gemeente(self, dataset: dict):
        read_config = dataset.get('source', {}).get('read_config', {})
        return read_config.get('gemeentes')[0]

    def _get_last_full_import_location(self, gemeente: str, date: datetime.date):
        last_full_date = self._last_full_import_date(date)
        last_full_fname = self._full_filename(last_full_date, gemeente)

        if last_full_fname not in self._get_available_full_downloads(gemeente):
            raise GOBException(f"Last full file {last_full_fname} is not available")
        return f"{self._get_full_download_path(gemeente)}{last_full_fname}"

    def handle_import(self, last_import: MutationImport, dataset: dict) -> Tuple[MutationImport, dict, datetime.date]:
        gemeente = self._get_gemeente(dataset)

        if not last_import:
            date = self._last_full_import_date(datetime.date.today())
            mode, fname = self.start_full(date, gemeente)
        elif not last_import.is_ended():
            mode, fname, date = self.restart_import(last_import)
        else:
            mode, fname, date = self.start_next(last_import, gemeente)

        mutation_import = MutationImport()
        mutation_import.catalogue = dataset['catalogue']
        mutation_import.collection = dataset['entity']
        mutation_import.application = dataset['source']['application']
        mutation_import.mode = mode.value
        mutation_import.filename = fname

        if mode == ImportMode.MUTATIONS:
            # The BAGExtract Datastore needs the last full download location as well to determine the ID's to import
            update_config = {
                'download_location': f"{self._get_mutations_download_path()}{fname}",
                'last_full_download_location': self._get_last_full_import_location(gemeente, date),
            }
        else:
            update_config = {
                'download_location': f"{self._get_full_download_path(gemeente)}{fname}",
            }

        # Update read_config for importer
        dataset['source']['read_config'] |= update_config

        return mutation_import, dataset, date

    def have_next(self, mutation_import: MutationImport, dataset: dict) -> bool:
        gemeente = self._get_gemeente(dataset)
        try:
            self.start_next(mutation_import, gemeente)
        except NothingToDo:
            return False
        return True
