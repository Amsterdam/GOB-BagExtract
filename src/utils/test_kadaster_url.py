import datetime
import os
from pprint import pprint

# Required vars
os.environ['KADASTER_PRODUCTSTORE_AFGIFTE_URL'] = ''
os.environ['KADASTER_PRODUCTSTORE_DOWNLOAD_URL'] = ''
os.environ['KADASTER_PRODUCTSTORE_CERT'] = ''
os.environ['KADASTER_PRODUCTSTORE_KEY'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = 'ca-certificates.crt'

from gobbagextract.mutations.afgifte import Afgifte  # noqa: E402
from gobbagextract.mutations.productstore import ProductStore  # noqa: E402
from gobbagextract.mutations.soap import BagSoapHandler  # noqa: E402
from gobbagextract.config import ArtikelNummer  # noqa: E402


start = datetime.date(2021, 12, 1)
end = datetime.date(2021, 12, 13)

results = {
    i.AfgifteID: i.Bestandsnaam
    for i in BagSoapHandler(artikelnummer=ArtikelNummer.MUT_DAG_NLD, start_date=start, end_date=end).iter_afgiftes()
}
print()
pprint(results)

id_ = input('\nDownload AfgifteID: ').strip()

ProductStore.download(Afgifte(AfgifteID=id_, Bestandsnaam=results[id_]), destination='~/')
