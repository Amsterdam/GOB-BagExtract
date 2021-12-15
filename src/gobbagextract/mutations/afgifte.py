import re
from typing import NamedTuple, Optional
import datetime as dt

from gobbagextract.config import RX_GEMEENTE_DATE, RX_DATE
from gobcore.exceptions import GOBException


class Afgifte(NamedTuple):
    """
    Container containing parsed response message from productstore.

     - AfgifteID: Unique reference to location
     - Afgiftereferentie: Identical to Bestandsnaam
     - Bestandsnaam: file name
     - Artikelnummer: Type of delivery
     - DatumAanmelding: Start availability
     - BeschikbaarTot: End availability
     - Bestandsgrootte: file size
    """
    AfgifteID: str = None
    Afgiftereferentie: str = None
    Bestandsnaam: str = None
    artikelnummer: str = None
    DatumAanmelding: str = None
    BeschikbaarTot: str = None
    Bestandsgrootte: str = None

    def _parse_bestandsnaam(self) -> tuple[dt.date, Optional[str]]:
        if m := RX_GEMEENTE_DATE.match(self.Bestandsnaam):
            date = dt.date(int(m.group(4)), int(m.group(3)), int(m.group(2)))
            gemeente = m.group(1)
        elif m := RX_DATE.match(self.Bestandsnaam):
            date = dt.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            gemeente = None
        else:
            raise GOBException(f"Could not parse filename. Unknown format: '{self.Bestandsnaam}'")

        return date, gemeente

    def get_date(self) -> dt.date:
        """Returns parsed date from filename."""
        return self._parse_bestandsnaam()[0]

    def get_daterange(self) -> Optional[str]:
        """Returns parsed daterange as string from filename."""
        if m := re.match(r'^BAGNLDM-(\d{8}-\d{8}).zip$', self.Bestandsnaam):
            return m.group(1)
        raise GOBException(f"Could not parse daterange from filename. Unknown format: '{self.Bestandsnaam}'")

    def get_gemeente(self) -> Optional[str]:
        """Returns gemeente code from filename, if available."""
        return self._parse_bestandsnaam()[1]
