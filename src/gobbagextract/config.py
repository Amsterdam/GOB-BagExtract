import os

CONTAINER_BASE = os.getenv("CONTAINER_BASE", "acceptatie")
DATABASE_CONFIG = {
    'drivername': 'postgres',
    'username': os.getenv("GOB_BAGEXTRACT_DATABASE_USER", "gob_bagextract"),
    'password': os.getenv("GOB_BAGEXTRACT_DATABASE_PASSWORD", "insecure"),
    'host': os.getenv("GOB_BAGEXTRACT_DATABASE_HOST", "localhost"),
    'port': os.getenv("GOB_BAGEXTRACT_DATABASE_PORT", 5413),
    'database': os.getenv("GOB_BAGEXTRACT_DATABASE", 'gob_bagextract'),
}

BAGEXTRACT_DOWNLOAD_URL = os.getenv("BAGEXTRACT_DOWNLOAD_URL", "https://extracten.bag.kadaster.nl/lvbag/extracten")

# When no BAG mutations (or full) are available, when to give a warning
BAGEXTRACT_NOT_AVAIL_DAYS_WARNING = os.getenv("BAGEXTRACT_NOT_AVAIL_DAYS_WARNING", 2)
BAGEXTRACT_NOT_AVAIL_DAYS_ERROR = os.getenv("BAGEXTRACT_NOT_AVAIL_DAYS_ERROR", 5)
