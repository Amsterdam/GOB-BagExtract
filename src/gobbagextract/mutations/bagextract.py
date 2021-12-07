import datetime
import re
import xml.etree.ElementTree as ET
from typing import Tuple, NamedTuple, Optional

import requests
from dateutil.relativedelta import relativedelta

from gobbagextract.config import KADASTER_PRODUCTSTORE_URL, ArtikelNummer
from gobbagextract.database.model import MutationImport
from gobbagextract.mutations.exception import NothingToDo
from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException

RX_GEMEENTE_DATE = re.compile(r"^BAGGEM(\d{4})L-(\d{2})(\d{2})(\d{4}).zip$")
RX_DATE = re.compile(r"^BAGNLDM-\d{8}-(\d{2})(\d{2})(\d{4}).zip$")


class Afgifte(NamedTuple):
    AfgifteID: str = None
    Afgiftereferentie: str = None
    Bestandsnaam: str = None
    artikelnummer: str = None
    DatumAanmelding: str = None
    BeschikbaarTot: str = None

    def get_url(self):
        return KADASTER_PRODUCTSTORE_URL + '/' + self.AfgifteID

    def get_date(self) -> datetime.date:
        if m := RX_GEMEENTE_DATE.match(self.Bestandsnaam):
            return datetime.date(int(m.group(4)), int(m.group(3)), int(m.group(2)))
        if m := RX_DATE.match(self.Bestandsnaam):
            return datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        raise GOBException(f"Could not parse filename. Unknown format: {self.Bestandsnaam}")

    def get_gemeente(self) -> Optional[str]:
        if m := RX_GEMEENTE_DATE.match(self.Bestandsnaam):
            return m.group(1)
        if m := RX_DATE.match(self.Bestandsnaam):
            return None
        raise GOBException(f"Could not parse filename. Unknown format: {self.Bestandsnaam}")


class BagSoapParser:

    MUT_REQUEST_TEMPLATE = """
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                      xmlns:v20="http://www.kadaster.nl/schemas/gds2/make2stock/v20201201">
        <soapenv:Header/>
        <soapenv:Body>
            <v20:BestandenlijstOpvragenRequest>
                <v20:Periode>
                    <v20:DatumTijdVanaf>{start}</v20:DatumTijdVanaf>
                    <v20:DatumTijdTotEnMet>{eind}</v20:DatumTijdTotEnMet>
                </v20:Periode>
                <v20:Artikelnummers>{artikelnummer}</v20:Artikelnummers>
            </v20:BestandenlijstOpvragenRequest>
        </soapenv:Body>
    </soapenv:Envelope>
    """

    NAMESPACES = {
        'v20': 'http://www.kadaster.nl/schemas/gds2/make2stock/v20201201',
        'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
        'SOAP-ENV': 'http://schemas.xmlsoap.org/soap/envelope/',
        'ns2': 'http://www.kadaster.nl/schemas/generiek/procesresultaat/v20110922',
        'ns3': 'http://www.kadaster.nl/schemas/gds2/make2stock/v20201201'
    }

    def __init__(self, artikelnummer: ArtikelNummer, start_date: datetime.date, end_date: datetime.date):
        self.url = KADASTER_PRODUCTSTORE_URL
        self.start_date = start_date
        self.end_date = end_date
        self.artikelnummer = artikelnummer

        self.tree = ET.fromstring(self._from_url())

    def _format_template(self):
        return self.MUT_REQUEST_TEMPLATE.format(
            start=self.start_date.isoformat(),
            eind=self.end_date.isoformat(),
            artikelnummer=self.artikelnummer
        ).strip()

    def _from_url(self):
        response = requests.post(url=self.url, data=self._format_template())
        response.raise_for_status()
        return response.text

    def strip_namespace(self, tag: str) -> str:
        if self.NAMESPACES:
            for nspace in self.NAMESPACES.values():
                tag = tag.replace(f'{{{nspace}}}', '')
        return tag

    def node_to_dict(self, node: ET.Element) -> dict[str, str]:
        """Returns dict with tags as key and text as value. Removes given namespaces from tags."""
        return {self.strip_namespace(subnode.tag): subnode.text for subnode in node.findall('./')}

    def create_dict_from_nodes(self, nodes: list[ET.Element]) -> list[dict]:
        return [self.node_to_dict(node) for node in nodes]

    def findall(self, path) -> list[ET.Element]:
        return self.tree.findall(path, self.NAMESPACES)


class BagExtractMutationsHandler:
    # Full import every 15th of the month
    FULL_IMPORT_DAY = 15

    def _last_full_import_date(self, date: datetime.date):
        if date.day < self.FULL_IMPORT_DAY:
            date -= relativedelta(months=1)
        return date.replace(day=self.FULL_IMPORT_DAY)

    def _datestr(self, date: datetime.date) -> str:
        return date.strftime("%d%m%Y")

    def _full_filename(self, date: datetime.date, gemeente: str) -> str:
        return f"BAGGEM{gemeente}L-{self._datestr(date)}.zip"

    def _mutations_filename(self, date: datetime.date) -> str:
        return f"BAGNLDM-{self._datestr(date - datetime.timedelta(days=1))}-{self._datestr(date)}.zip"

    def _get_gemeente(self, dataset: dict) -> str:
        read_config = dataset.get('source', {}).get('read_config', {})
        return read_config.get('gemeentes')[0]

    def restart_import(self, last_import: MutationImport) -> Tuple[ImportMode, Afgifte, datetime.date]:
        mode = last_import.mode
        afgifte = Afgifte(Bestandsnaam=last_import.filename)
        date, gemeente = afgifte.get_date(), afgifte.get_gemeente()

        if mode == ImportMode.FULL.value:
            ret = self.get_full(date, gemeente)
        else:
            ret = self.get_mutations(date)

        return ret + (date,)

    def start_next(self, last_import: MutationImport, gemeente: str) -> Tuple[ImportMode, Afgifte, datetime.date]:
        date = Afgifte(Bestandsnaam=last_import.filename).get_date()
        next_date = date + datetime.timedelta(days=1)

        if next_date.day == self.FULL_IMPORT_DAY:
            ret = self.get_full(next_date, gemeente)
        else:
            ret = self.get_mutations(next_date)

        return ret + (next_date,)

    def get_full(self, date: datetime.date, gemeente: str) -> Tuple[ImportMode, Afgifte]:
        parser = BagSoapParser(
            start_date=(date - datetime.timedelta(days=1)),
            end_date=date,
            artikelnummer=ArtikelNummer.MUT_MAAND_GEM,
        )
        afgiftes = [Afgifte(**parser.node_to_dict(node)) for node in parser.findall('*//{*}BestandAfgiftes') if node]
        afgiftes = [afg for afg in afgiftes if afg.get_gemeente() == gemeente]

        if not afgiftes:
            # TODO: This implies when file is not yet availble on the 15th we are failing this workflow!
            raise NothingToDo.file_not_available(self._full_filename(date, gemeente))

        return ImportMode.FULL, afgiftes[0]

    def get_mutations(self, date: datetime.date) -> tuple[ImportMode, Afgifte]:
        parser = BagSoapParser(
            start_date=(date - datetime.timedelta(days=1)),
            end_date=date,
            artikelnummer=ArtikelNummer.MUT_DAG_NLD,
        )
        afgiftes = [Afgifte(**parser.node_to_dict(node)) for node in parser.findall('*//{*}BestandAfgiftes') if node]

        if not afgiftes:
            raise NothingToDo.file_not_available(self._mutations_filename(date))

        return ImportMode.MUTATIONS, afgiftes[0]

    def handle_import(self, last_import: MutationImport, dataset: dict) -> tuple[MutationImport, dict, datetime.date]:
        gemeente = self._get_gemeente(dataset)

        if not last_import:
            date = self._last_full_import_date(datetime.date.today())
            mode, afgifte = self.get_full(date, gemeente)
        elif not last_import.is_ended():
            mode, afgifte, date = self.restart_import(last_import)
        else:
            mode, afgifte, date = self.start_next(last_import, gemeente)

        mutation_import = MutationImport()
        mutation_import.catalogue = dataset['catalogue']
        mutation_import.collection = dataset['entity']
        mutation_import.application = dataset['source']['application']
        mutation_import.mode = mode.value
        mutation_import.filename = afgifte.Bestandsnaam

        if mode == ImportMode.MUTATIONS:
            # The BAGExtract Datastore needs the last full download location as well to determine the ID's to import
            _, last_full = self.get_full(self._last_full_import_date(date), gemeente)
            update_config = {
                'download_location': afgifte.get_url(),
                'last_full_download_location': last_full.get_url()
            }
        else:
            update_config = {'download_location': afgifte.get_url()}

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
