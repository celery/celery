import logging
from celery.backends.azureblockblob import (
    AzureBlockBlobBackend as _AzureBlockBlobBackend,
)
from kombu.utils import cached_property
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
from kombu.transport.azurestoragequeues import Transport as AzureStorageQueuesTransport

logger = logging.getLogger(__name__)

""" 
Context:
We are using Azure Service Bus as a broker, but it does not support a
result backend.  I don't think it would be possible to add support for it,
since Service Bus is really a queue, so you can't retrieve a message bsed on
a key.  

Since we already use blob storage, we can use the AzureBlockBlobBackend
to store the results in blob storage.  However, the existing implementation 
does not support DefaultAzureCredential, so we need to extend it.
(PR Pending to Celery)
"""


class AzureBlockBlobBackend(_AzureBlockBlobBackend):
    """

    Connection String
    =================

    Connection string can have the following formats:

    .. code-block::

        azureblockblob://CONNECTION_STRING
        azureblockblob://DefaultAzureCredential@STORAGE_ACCOUNT_URL
        azureblockblob://ManagedIdentityCredential@STORAGE_ACCOUNT_URL
    """

    @cached_property
    def _blob_service_client(self):
        logger.info("Welcome inside the custom AzureBlockBlobBackend")
        """Return the Azure Storage Blob service client.

        If this is the first call to the property, the client is created and
        the container is created if it doesn't yet exist.

        """
        # Leveraging the work that Kombu already did for us
        credential_, url = AzureStorageQueuesTransport.parse_uri(
            self._connection_string
        )
        client = BlobServiceClient(
            account_url=url,
            credential=credential_,
            read_timeout=self._read_timeout,
        )

        try:
            client.create_container(name=self._container_name)
            msg = f"Container created with name {self._container_name}."
        except ResourceExistsError:
            msg = (
                f"Container with name {self._container_name} already."
                "exists. This will not be created."
            )
        logger.info(msg)

        return client
