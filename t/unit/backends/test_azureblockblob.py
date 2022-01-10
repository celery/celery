from unittest.mock import Mock, call, patch

import pytest

from celery import states
from celery.backends import azureblockblob
from celery.backends.azureblockblob import AzureBlockBlobBackend
from celery.exceptions import ImproperlyConfigured

MODULE_TO_MOCK = "celery.backends.azureblockblob"

pytest.importorskip('azure.storage.blob')
pytest.importorskip('azure.core.exceptions')


class test_AzureBlockBlobBackend:
    def setup(self):
        self.url = (
            "azureblockblob://"
            "DefaultEndpointsProtocol=protocol;"
            "AccountName=name;"
            "AccountKey=key;"
            "EndpointSuffix=suffix")

        self.backend = AzureBlockBlobBackend(
            app=self.app,
            url=self.url)

    @pytest.fixture(params=['', 'my_folder/'])
    def base_path(self, request):
        return request.param

    def test_missing_third_party_sdk(self):
        azurestorage = azureblockblob.azurestorage
        try:
            azureblockblob.azurestorage = None
            with pytest.raises(ImproperlyConfigured):
                AzureBlockBlobBackend(app=self.app, url=self.url)
        finally:
            azureblockblob.azurestorage = azurestorage

    def test_bad_connection_url(self):
        with pytest.raises(ImproperlyConfigured):
            AzureBlockBlobBackend._parse_url("azureblockblob://")

        with pytest.raises(ImproperlyConfigured):
            AzureBlockBlobBackend._parse_url("")

    @patch(MODULE_TO_MOCK + ".BlobServiceClient")
    def test_create_client(self, mock_blob_service_factory):
        mock_blob_service_client_instance = Mock()
        mock_blob_service_factory.from_connection_string.return_value = mock_blob_service_client_instance
        backend = AzureBlockBlobBackend(app=self.app, url=self.url)

        # ensure container gets created on client access...
        assert mock_blob_service_client_instance.create_container.call_count == 0
        assert backend._blob_service_client is not None
        assert mock_blob_service_client_instance.create_container.call_count == 1

        # ...but only once per backend instance
        assert backend._blob_service_client is not None
        assert mock_blob_service_client_instance.create_container.call_count == 1

    @patch(MODULE_TO_MOCK + ".BlobServiceClient")
    def test_configure_client(self, mock_blob_service_factory):

        connection_timeout = 3
        read_timeout = 11
        self.app.conf.update(
            {
                'azureblockblob_connection_timeout': connection_timeout,
                'azureblockblob_read_timeout': read_timeout,
            }
        )

        mock_blob_service_client_instance = Mock()
        mock_blob_service_factory.from_connection_string.return_value = (
            mock_blob_service_client_instance
        )

        base_url = "azureblockblob://"
        connection_string = "connection_string"
        backend = AzureBlockBlobBackend(
            app=self.app, url=f'{base_url}{connection_string}'
        )

        client = backend._blob_service_client
        assert client is mock_blob_service_client_instance

        (
            mock_blob_service_factory
            .from_connection_string
            .assert_called_once_with(
                connection_string,
                connection_timeout=connection_timeout,
                read_timeout=read_timeout
            )
        )

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._blob_service_client")
    def test_get(self, mock_client, base_path):
        self.backend.base_path = base_path
        self.backend.get(b"mykey")

        mock_client.get_blob_client \
            .assert_called_once_with(blob=base_path + "mykey", container="celery")

        mock_client.get_blob_client.return_value \
            .download_blob.return_value \
            .readall.return_value \
            .decode.assert_called_once()

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._blob_service_client")
    def test_get_missing(self, mock_client):
        mock_client.get_blob_client.return_value \
            .download_blob.return_value \
            .readall.side_effect = azureblockblob.ResourceNotFoundError

        assert self.backend.get(b"mykey") is None

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._blob_service_client")
    def test_set(self, mock_client, base_path):
        self.backend.base_path = base_path
        self.backend._set_with_state(b"mykey", "myvalue", states.SUCCESS)

        mock_client.get_blob_client.assert_called_once_with(
            container="celery", blob=base_path + "mykey")

        mock_client.get_blob_client.return_value \
            .upload_blob.assert_called_once_with("myvalue", overwrite=True)

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._blob_service_client")
    def test_mget(self, mock_client, base_path):
        keys = [b"mykey1", b"mykey2"]

        self.backend.base_path = base_path
        self.backend.mget(keys)

        mock_client.get_blob_client.assert_has_calls(
            [call(blob=base_path + key.decode(), container='celery') for key in keys],
            any_order=True,)

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._blob_service_client")
    def test_delete(self, mock_client, base_path):
        self.backend.base_path = base_path
        self.backend.delete(b"mykey")

        mock_client.get_blob_client.assert_called_once_with(
            container="celery", blob=base_path + "mykey")

        mock_client.get_blob_client.return_value \
            .delete_blob.assert_called_once()

    def test_base_path_conf(self, base_path):
        self.app.conf.azureblockblob_base_path = base_path
        backend = AzureBlockBlobBackend(
            app=self.app,
            url=self.url
        )
        assert backend.base_path == base_path

    def test_base_path_conf_default(self):
        backend = AzureBlockBlobBackend(
            app=self.app,
            url=self.url
        )
        assert backend.base_path == ''


class test_as_uri:
    def setup(self):
        self.url = (
            "azureblockblob://"
            "DefaultEndpointsProtocol=protocol;"
            "AccountName=name;"
            "AccountKey=account_key;"
            "EndpointSuffix=suffix"
        )
        self.backend = AzureBlockBlobBackend(
            app=self.app,
            url=self.url
        )

    def test_as_uri_include_password(self):
        assert self.backend.as_uri(include_password=True) == self.url

    def test_as_uri_exclude_password(self):
        assert self.backend.as_uri(include_password=False) == (
            "azureblockblob://"
            "DefaultEndpointsProtocol=protocol;"
            "AccountName=name;"
            "AccountKey=**;"
            "EndpointSuffix=suffix"
        )
