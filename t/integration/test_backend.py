from __future__ import absolute_import, unicode_literals

import os

from case import skip
from celery.backends.azureblockblob import AzureBlockBlobBackend


@skip.unless_module("azure")
@skip.unless_environ("AZUREBLOCKBLOB_URL")
class test_AzureBlockBlobBackend:
    def test_crud(self, manager):
        backend = AzureBlockBlobBackend(
            app=manager.app,
            url=os.environ["AZUREBLOCKBLOB_URL"])

        key_values = {("akey%d" % i).encode(): "avalue%d" % i
                      for i in range(5)}

        for key, value in key_values.items():
            backend.set(key, value)

        actual_values = backend.mget(key_values.keys())
        expected_values = list(key_values.values())

        assert expected_values == actual_values

        for key in key_values:
            backend.delete(key)

    def test_get_missing(self, manager):
        backend = AzureBlockBlobBackend(
            app=manager.app,
            url=os.environ["AZUREBLOCKBLOB_URL"])

        assert backend.get(b"doesNotExist") is None
