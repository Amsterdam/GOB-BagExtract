from pathlib import Path
from typing import Union
from urllib.parse import urljoin

import requests

from gobbagextract.config import KADASTER_PRODUCTSTORE_AFGIFTE_URL, KADASTER_PRODUCTSTORE_CERT, \
    KADASTER_PRODUCTSTORE_KEY, KADASTER_PRODUCTSTORE_DOWNLOAD_URL
from gobbagextract.mutations.afgifte import Afgifte


class ProductStore:
    """
    Stores the connection to the Kadaster download service (productstore).
    Possible actions:
     - list: list the available `afgiftes` as xml
     - download: download an `afgifte`

     Required vars:
     - KADASTER_PRODUCTSTORE_CERT
     - KADASTER_PRODUCTSTORE_KEY
     - KADASTER_PRODUCTSTORE_AFGIFTE_URL
     - KADASTER_PRODUCTSTORE_DOWNLOAD_URL
    """
    cert = (KADASTER_PRODUCTSTORE_CERT, KADASTER_PRODUCTSTORE_KEY)

    @classmethod
    def _request(cls, **kwargs) -> requests.Response:
        with requests.request(cert=cls.cert, **kwargs) as resp:
            resp.raise_for_status()
            return resp

    @classmethod
    def list(cls, **kwargs) -> requests.Response:
        """Returns XML response containing bestandsafgiftes."""
        return cls._request(method="POST", url=KADASTER_PRODUCTSTORE_AFGIFTE_URL, **kwargs)

    @classmethod
    def download(cls, afgifte: Afgifte, destination: Union[str, Path], **kwargs) -> Path:
        """Downloads an `afgifte` to `destination`."""
        url = urljoin(KADASTER_PRODUCTSTORE_DOWNLOAD_URL, afgifte.AfgifteID)
        file_ = Path(destination).expanduser() / afgifte.Bestandsnaam

        resp = cls._request(method="POST", url=url, **kwargs)
        file_.write_bytes(resp.content)

        return file_
