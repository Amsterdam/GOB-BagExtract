from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, Mock

from gobbagextract.mutations.afgifte import Afgifte
from gobbagextract.mutations.productstore import ProductStore


class MockRequest:

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def content(self):
        return b"some_bytes"

    def raise_for_status(self):
        pass


@patch("requests.request", side_effect=MockRequest)
def test_list(mock_requests):
    ProductStore.list(kw="1")

    mock_requests.assert_called_with(
        cert=("/path/to/cert", "/path/to/key"),
        method="POST",
        url="https://kadaster.nl/productstore/afgifte",
        kw="1"
    )


@patch("requests.request", side_effect=MockRequest)
def test_download(mock_requests):
    afgifte = Afgifte(Bestandsnaam="file.zip", AfgifteID="12345-6789-123")

    with TemporaryDirectory() as tmp_dir:
        file_path = ProductStore.download(afgifte, destination=tmp_dir, kw=1)

        assert file_path == Path(tmp_dir) / afgifte.Bestandsnaam
        assert file_path.exists()

    mock_requests.assert_called_with(
        cert=("/path/to/cert", "/path/to/key"),
        method="POST",
        url="https://kadaster.nl/productstore/download/12345-6789-123",
        kw=1
    )
