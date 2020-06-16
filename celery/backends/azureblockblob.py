"""The Azure Storage Block Blob backend for Celery."""
from __future__ import absolute_import, unicode_literals

from kombu.utils import cached_property
from kombu.utils.encoding import bytes_to_str

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

from .base import KeyValueStoreBackend

try:
    import azure.storage as azurestorage
    from azure.common import AzureMissingResourceHttpError
    from azure.storage.blob import BlockBlobService
    from azure.storage.common.retry import ExponentialRetry
except ImportError:  # pragma: no cover
    azurestorage = BlockBlobService = ExponentialRetry = \
        AzureMissingResourceHttpError = None  # noqa

__all__ = ("AzureBlockBlobBackend",)

LOGGER = get_logger(__name__)


class AzureBlockBlobBackend(KeyValueStoreBackend):
    """Azure Storage Block Blob backend for Celery."""

    def __init__(self,
                 url=None,
                 container_name=None,
                 retry_initial_backoff_sec=None,
                 retry_increment_base=None,
                 retry_max_attempts=None,
                 *args,
                 **kwargs):
        super(AzureBlockBlobBackend, self).__init__(*args, **kwargs)

        if azurestorage is None:
            raise ImproperlyConfigured(
                "You need to install the azure-storage library to use the "
                "AzureBlockBlob backend")

        conf = self.app.conf

        self._connection_string = self._parse_url(url)

        self._container_name = (
            container_name or
            conf["azureblockblob_container_name"])

        self._retry_initial_backoff_sec = (
            retry_initial_backoff_sec or
            conf["azureblockblob_retry_initial_backoff_sec"])

        self._retry_increment_base = (
            retry_increment_base or
            conf["azureblockblob_retry_increment_base"])

        self._retry_max_attempts = (
            retry_max_attempts or
            conf["azureblockblob_retry_max_attempts"])

    @classmethod
    def _parse_url(cls, url, prefix="azureblockblob://"):
        connection_string = url[len(prefix):]
        if not connection_string:
            raise ImproperlyConfigured("Invalid URL")

        return connection_string

    @cached_property
    def _client(self):
        """Return the Azure Storage Block Blob service.

        If this is the first call to the property, the client is created and
        the container is created if it doesn't yet exist.

        """
        client = BlockBlobService(connection_string=self._connection_string)

        created = client.create_container(
            container_name=self._container_name, fail_on_exist=False)

        if created:
            LOGGER.info("Created Azure Blob Storage container %s",
                        self._container_name)

        client.retry = ExponentialRetry(
            initial_backoff=self._retry_initial_backoff_sec,
            increment_base=self._retry_increment_base,
            max_attempts=self._retry_max_attempts).retry

        return client

    def get(self, key):
        """Read the value stored at the given key.

        Args:
              key: The key for which to read the value.

        """
        key = bytes_to_str(key)
        LOGGER.debug("Getting Azure Block Blob %s/%s",
                     self._container_name, key)

        try:
            return self._client.get_blob_to_text(
                self._container_name, key).content
        except AzureMissingResourceHttpError:
            return None

    def set(self, key, value):
        """Store a value for a given key.

        Args:
              key: The key at which to store the value.
              value: The value to store.

        """
        key = bytes_to_str(key)
        LOGGER.debug("Creating Azure Block Blob at %s/%s",
                     self._container_name, key)

        return self._client.create_blob_from_text(
            self._container_name, key, value)

    def mget(self, keys):
        """Read all the values for the provided keys.

        Args:
              keys: The list of keys to read.

        """
        return [self.get(key) for key in keys]

    def delete(self, key):
        """Delete the value at a given key.

        Args:
              key: The key of the value to delete.

        """
        key = bytes_to_str(key)
        LOGGER.debug("Deleting Azure Block Blob at %s/%s",
                     self._container_name, key)

        self._client.delete_blob(self._container_name, key)
