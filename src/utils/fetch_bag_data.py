#!/usr/bin/env python3
"""Util to download bag zip files to ~/.

Run with gobbagextract in the python path:
PYTHONPATH=<PATH_TO_GOB>/GOB-BagExtract/src utils/fetch_bag_data.py
"""
import datetime
import os
from pprint import pprint

# Required vars
os.environ["KADASTER_PRODUCTSTORE_AFGIFTE_URL"] = "https://service30.kadaster.nl/gds2/afgifte/productstore"
os.environ["KADASTER_PRODUCTSTORE_DOWNLOAD_URL"] = "https://service30.kadaster.nl/gds2/download/productstore/"
if not os.environ.get("KADASTER_PRODUCTSTORE_CERT"):
    os.environ["KADASTER_PRODUCTSTORE_CERT"] = "../acc_api_data_amsterdam_nl.crt"
if not os.environ.get("KADASTER_PRODUCTSTORE_KEY"):
    os.environ["KADASTER_PRODUCTSTORE_KEY"] = "../GOB_kadaster_acc_amsterdam_nl.key"
if not os.environ.get("REQUESTS_CA_BUNDLE"):
    os.environ["REQUESTS_CA_BUNDLE"] = "../ca-certificates.crt"

from gobbagextract.mutations.afgifte import Afgifte  # noqa: E402
from gobbagextract.mutations.productstore import ProductStore  # noqa: E402
from gobbagextract.mutations.soap import BagSoapHandler  # noqa: E402
from gobbagextract.config import ArtikelNummer  # noqa: E402


start = datetime.date(2021, 12, 1)
end = datetime.date(2022, 1, 31)

results = {
    i.AfgifteID: i.Bestandsnaam
    for i in BagSoapHandler(artikelnummer=ArtikelNummer.VOL_GEM, start_date=start, end_date=end).iter_afgiftes()
}
print()
pprint(results)

id_ = input('\nDownload AfgifteID: ').strip()

ProductStore.download(Afgifte(AfgifteID=id_, Bestandsnaam=results[id_]), destination='~/')
