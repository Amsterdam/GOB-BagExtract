import datetime as dt
from typing import Tuple

from dateutil.relativedelta import relativedelta

from gobbagextract.config import ArtikelNummer
from gobbagextract.database.model import MutationImport
from gobbagextract.mutations.afgifte import Afgifte
from gobbagextract.mutations.exception import NothingToDo
from gobcore.enum import ImportMode
from gobbagextract.mutations.soap import BagSoapHandler
from gobcore.logging.logger import logger


class BagExtractMutationsHandler:
    # Full import every 15th of the month
    FULL_IMPORT_DAY = 15

    # Number of periods to lookback for inital import
    INITIAL_IMPORT_RETRY = 5

    SoapHandler = BagSoapHandler

    @staticmethod
    def _get_gemeente(dataset: dict) -> str:
        read_config = dataset.get("source", {}).get("read_config", {})
        return read_config.get("gemeentes")[0]

    @staticmethod
    def _datestr(date: dt.date) -> str:
        return date.strftime("%d%m%Y")

    def _full_filename(self, date: dt.date, gemeente: str) -> str:
        return f"BAGGEM{gemeente}L-{self._datestr(date)}.zip"

    def _mutations_filename(self, date: dt.date) -> str:
        return f"BAGNLDM-{self._datestr(date - dt.timedelta(days=1))}-{self._datestr(date)}.zip"

    def _last_full_import_date(self, date: dt.date):
        if date.day < self.FULL_IMPORT_DAY:
            date -= relativedelta(months=1)
        return date.replace(day=self.FULL_IMPORT_DAY)

    def restart_import(self, last_import: MutationImport) -> tuple[ImportMode, Afgifte, dt.date]:
        mode = last_import.mode
        afgifte = Afgifte(Bestandsnaam=last_import.filename)
        date = afgifte.get_date()
        gemeente = afgifte.get_gemeente()

        if mode == ImportMode.FULL.value:
            ret = self.get_full(date, gemeente)
        else:
            ret = self.get_daily_mutations(date)

        return ret + (date,)

    def start_next(self, last_import: MutationImport, gemeente: str) -> tuple[ImportMode, Afgifte, dt.date]:
        date = Afgifte(Bestandsnaam=last_import.filename).get_date()
        next_date = date + dt.timedelta(days=1)

        if next_date.day == self.FULL_IMPORT_DAY:
            ret = self.get_full(next_date, gemeente)
        else:
            ret = self.get_daily_mutations(next_date)

        return ret + (next_date, )

    def get_full(self, date: dt.date, gemeente: str) -> Tuple[ImportMode, Afgifte]:
        date = self._last_full_import_date(date)

        kwargs = {
            "start_date": (date - relativedelta(month=1)),
            "end_date": date,
            "artikelnummer": ArtikelNummer.VOL_GEM
        }

        for afgifte in self.SoapHandler(**kwargs).iter_afgiftes():
            if afgifte.get_gemeente() == gemeente and afgifte.get_date() == date:
                return ImportMode.FULL, afgifte

        # TODO: This implies when file is not yet availble on the 15th we are failing this workflow!
        raise NothingToDo.file_not_available(self._full_filename(date, gemeente))

    def get_daily_mutations(self, date: dt.date) -> tuple[ImportMode, Afgifte]:
        kwargs = {
            "start_date": (date - dt.timedelta(days=1)),
            "end_date": date,
            "artikelnummer": ArtikelNummer.MUT_DAG_NLD
        }

        for afgifte in self.SoapHandler(**kwargs).iter_afgiftes():
            if afgifte.get_date() == date:
                return ImportMode.MUTATIONS, afgifte

        raise NothingToDo.file_not_available(self._mutations_filename(date))

    def initial_import(self, date: dt.date, gemeente: str) -> tuple[ImportMode, Afgifte, dt.date]:
        date = self._last_full_import_date(date)

        for retry in range(self.INITIAL_IMPORT_RETRY):
            try:
                return self.get_full(date, gemeente) + (date,)
            except NothingToDo:
                logger.warning(f"Retrying previous initial import for: {gemeente} / {date}")
                date = self._last_full_import_date(date - dt.timedelta(days=1))

        raise NothingToDo(f"No initial import found for {self.INITIAL_IMPORT_RETRY} periods.")

    def handle_import(self, last_import: MutationImport, dataset: dict) -> tuple[MutationImport, dict, dt.date]:
        gemeente = self._get_gemeente(dataset)

        if not last_import:
            mode, afgifte, date = self.initial_import(dt.date.today(), gemeente)
        elif not last_import.is_ended():
            mode, afgifte, date = self.restart_import(last_import)
        else:
            mode, afgifte, date = self.start_next(last_import, gemeente)

        mutation_import = MutationImport()
        mutation_import.catalogue = dataset["catalogue"]
        mutation_import.collection = dataset["entity"]
        mutation_import.application = dataset["source"]["application"]
        mutation_import.mode = mode.value
        mutation_import.filename = afgifte.Bestandsnaam

        update_config = {"download_location": afgifte}

        if mode == ImportMode.MUTATIONS:
            # The BAGExtract Datastore needs the last full download location as well to determine the ID's to import
            update_config["last_full_download_location"] = self.get_full(date, gemeente)[1]

        # Update read_config for importer
        dataset["source"]["read_config"] |= update_config

        return mutation_import, dataset, date

    def have_next(self, mutation_import: MutationImport, dataset: dict) -> bool:
        gemeente = self._get_gemeente(dataset)
        try:
            self.start_next(mutation_import, gemeente)
        except NothingToDo:
            return False
        return True
