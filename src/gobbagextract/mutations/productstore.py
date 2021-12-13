from pathlib import Path

import requests

from gobbagextract.config import KADASTER_PRODUCTSTORE_AFGIFTE_URL, KADASTER_PRODUCTSTORE_CERT, \
    KADASTER_PRODUCTSTORE_KEY, \
    KADASTER_PRODUCTSTORE_DOWNLOAD_URL
from gobbagextract.mutations.afgifte import Afgifte


class ProductStore:

    cert = (KADASTER_PRODUCTSTORE_CERT, KADASTER_PRODUCTSTORE_KEY)

    @classmethod
    def _request(cls, **kwargs) -> requests.Response:
        with requests.request(cert=cls.cert, **kwargs) as resp:
            resp.raise_for_status()
            return resp

    @classmethod
    def list(cls, **kwargs):
        return cls._request(method='POST', url=KADASTER_PRODUCTSTORE_AFGIFTE_URL, **kwargs)

    @classmethod
    def download(cls, afgifte: Afgifte, destination: Path, **kwargs) -> Path:
        url = KADASTER_PRODUCTSTORE_DOWNLOAD_URL + '/' + afgifte.AfgifteID
        file_ = Path(destination, afgifte.Bestandsnaam)

        resp = cls._request(method='POST', url=url, **kwargs)
        with open(file_, 'wb') as f:
            f.write(resp.content)
        return file_
