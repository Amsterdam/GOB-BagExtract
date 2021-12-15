import datetime as dt
from typing import Iterator, Generator
from xml.etree import ElementTree

from gobbagextract.config import ArtikelNummer
from gobbagextract.mutations.afgifte import Afgifte
from gobbagextract.mutations.productstore import ProductStore

NAMESPACES = {
    "v20": "http://www.kadaster.nl/schemas/gds2/make2stock/v20201201",
    "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
    "SOAP-ENV": "http://schemas.xmlsoap.org/soap/envelope/",
    "ns2": "http://www.kadaster.nl/schemas/generiek/procesresultaat/v20110922",
    "ns3": "http://www.kadaster.nl/schemas/gds2/make2stock/v20201201"
}


class XmlParser:

    def __init__(self, namespaces: dict):
        self.ns = namespaces
        self.tree = None

    def parse(self, text: str):
        self.tree = ElementTree.fromstring(text)

    def strip_namespace(self, tag: str) -> str:
        if self.ns:
            for nspace in self.ns.values():
                tag = tag.replace(f"{{{nspace}}}", "")
        return tag

    def node_to_dict(self, node: ElementTree.Element) -> dict[str, str]:
        """Returns dict with tags as key and text as value. Removes given namespaces from tags."""
        return {self.strip_namespace(subnode.tag): subnode.text for subnode in node.findall("./")}

    def findall(self, path) -> Generator[ElementTree.Element, None, None]:
        yield from (node for node in self.tree.iterfind(path, self.ns) if node)


class BagSoapHandler(XmlParser):

    def __init__(self, artikelnummer: ArtikelNummer, start_date: dt.date, end_date: dt.date):
        super().__init__(NAMESPACES)
        self.artnr = artikelnummer
        self.start = start_date
        self.end = end_date

    MUT_REQUEST_TEMPLATE = \
        """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                      xmlns:v20="http://www.kadaster.nl/schemas/gds2/make2stock/v20201201">
        <soapenv:Header/>
        <soapenv:Body>
            <v20:BestandenlijstOpvragenRequest>
                {period}
                <v20:Artikelnummers>{artikelnummer}</v20:Artikelnummers>
            </v20:BestandenlijstOpvragenRequest>
        </soapenv:Body>
    </soapenv:Envelope>"""

    @staticmethod
    def _to_datetime(date: dt.date, day_end: bool = False) -> dt.datetime:
        time = dt.time(23, 59, 59) if day_end else dt.time(0, 0, 0)
        return dt.datetime.combine(date, time)

    @property
    def format_template(self):
        period = ""
        if self.start or self.end:
            period = "<v20:Periode>"
            if self.start:
                start = self._to_datetime(self.start).isoformat()
                period += f"\n<v20:DatumTijdVanaf>{start}</v20:DatumTijdVanaf>\n"
            if self.end:
                end = self._to_datetime(self.end, day_end=True).isoformat()
                period += f"<v20:DatumTijdTotEnMet>{end}</v20:DatumTijdTotEnMet>\n"
            period += "</v20:Periode>"

        return self.MUT_REQUEST_TEMPLATE.format(period=period, artikelnummer=self.artnr.value)

    def iter_afgiftes(self) -> Iterator[Afgifte]:
        resp = ProductStore.list(data=self.format_template)
        self.parse(resp.text)

        # ignore namespaces in path
        for node in self.findall("*//{*}BestandAfgiftes"):
            yield Afgifte(**self.node_to_dict(node))
