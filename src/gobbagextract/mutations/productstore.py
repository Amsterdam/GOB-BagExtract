from pathlib import Path
from typing import Union
from urllib.parse import urljoin

import requests

from gobbagextract.config import KADASTER_PRODUCTSTORE_AFGIFTE_URL, KADASTER_PRODUCTSTORE_CERT, \
    KADASTER_PRODUCTSTORE_KEY, KADASTER_PRODUCTSTORE_DOWNLOAD_URL
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
    def download(cls, afgifte: Afgifte, destination: Union[str, Path], **kwargs) -> Path:
        url = urljoin(KADASTER_PRODUCTSTORE_DOWNLOAD_URL, afgifte.AfgifteID)
        file_ = Path(destination).expanduser() / afgifte.Bestandsnaam

        resp = cls._request(method='POST', url=url, **kwargs)
        file_.write_bytes(resp.content)

        return file_
