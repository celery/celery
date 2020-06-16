from __future__ import absolute_import, unicode_literals

import pytest
from case import Mock, call, patch, skip

from celery import states
from celery.backends import azureblockblob
from celery.backends.azureblockblob import AzureBlockBlobBackend
from celery.exceptions import ImproperlyConfigured

MODULE_TO_MOCK = "celery.backends.azureblockblob"


@skip.unless_module("azure")
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

    @patch(MODULE_TO_MOCK + ".BlockBlobService")
    def test_create_client(self, mock_blob_service_factory):
        mock_blob_service_instance = Mock()
        mock_blob_service_factory.return_value = mock_blob_service_instance
        backend = AzureBlockBlobBackend(app=self.app, url=self.url)

        # ensure container gets created on client access...
        assert mock_blob_service_instance.create_container.call_count == 0
        assert backend._client is not None
        assert mock_blob_service_instance.create_container.call_count == 1

        # ...but only once per backend instance
        assert backend._client is not None
        assert mock_blob_service_instance.create_container.call_count == 1

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._client")
    def test_get(self, mock_client):
        self.backend.get(b"mykey")

        mock_client.get_blob_to_text.assert_called_once_with(
            "celery", "mykey")

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._client")
    def test_get_missing(self, mock_client):
        mock_client.get_blob_to_text.side_effect = \
            azureblockblob.AzureMissingResourceHttpError("Missing", 404)

        assert self.backend.get(b"mykey") is None

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._client")
    def test_set(self, mock_client):
        self.backend._set_with_state(b"mykey", "myvalue", states.SUCCESS)

        mock_client.create_blob_from_text.assert_called_once_with(
            "celery", "mykey", "myvalue")

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._client")
    def test_mget(self, mock_client):
        keys = [b"mykey1", b"mykey2"]

        self.backend.mget(keys)

        mock_client.get_blob_to_text.assert_has_calls(
            [call("celery", "mykey1"),
             call("celery", "mykey2")])

    @patch(MODULE_TO_MOCK + ".AzureBlockBlobBackend._client")
    def test_delete(self, mock_client):
        self.backend.delete(b"mykey")

        mock_client.delete_blob.assert_called_once_with(
            "celery", "mykey")
