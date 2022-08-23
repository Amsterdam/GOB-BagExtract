import os
import re
from enum import Enum


class ArtikelNummer(Enum):
    MUT_DAG_NLD = 2529
    MUT_MAAND_GEM = 2531
    MUT_MAAND_NLD = 2532
    VOL_GEM = 2535
    VOL_NLD = 2536


CONTAINER_BASE = os.getenv("CONTAINER_BASE", "acceptatie")
DATABASE_CONFIG = {
    "drivername": "postgresql",
    "username": os.getenv("GOB_BAGEXTRACT_DATABASE_USER", "gob_bagextract"),
    "password": os.getenv("GOB_BAGEXTRACT_DATABASE_PASSWORD", "insecure"),
    "host": os.getenv("GOB_BAGEXTRACT_DATABASE_HOST", "localhost"),
    "port": os.getenv("GOB_BAGEXTRACT_DATABASE_PORT", 5413),
    "database": os.getenv("GOB_BAGEXTRACT_DATABASE", "gob_bagextract"),
}

# When no BAG mutations (or full) are available, when to give a warning
BAGEXTRACT_NOT_AVAIL_DAYS_WARNING = os.getenv("BAGEXTRACT_NOT_AVAIL_DAYS_WARNING", 2)
BAGEXTRACT_NOT_AVAIL_DAYS_ERROR = os.getenv("BAGEXTRACT_NOT_AVAIL_DAYS_ERROR", 5)

KADASTER_PRODUCTSTORE_AFGIFTE_URL = os.getenv("KADASTER_PRODUCTSTORE_AFGIFTE_URL")
KADASTER_PRODUCTSTORE_DOWNLOAD_URL = os.getenv("KADASTER_PRODUCTSTORE_DOWNLOAD_URL")

KADASTER_PRODUCTSTORE_CERT = os.getenv("KADASTER_PRODUCTSTORE_CERT")
KADASTER_PRODUCTSTORE_KEY = os.getenv("KADASTER_PRODUCTSTORE_KEY")

RX_GEMEENTE_DATE = re.compile(r"^BAGGEM(\d{4})L-(\d{2})(\d{2})(\d{4}).zip$")
RX_DATE = re.compile(r"^BAGNLDM-\d{8}-(\d{2})(\d{2})(\d{4}).zip$")
