from __future__ import absolute_import, unicode_literals

import os

from case import skip

from celery.backends.azureblockblob import AzureBlockBlobBackend


@skip.unless_module("azure")
@skip.unless_environ("AZUREBLOCKBLOB_URL")
class test_AzureBlockBlobBackend:
    def test_live(self, manager):
        backend = AzureBlockBlobBackend(
            app=manager.app,
            url=os.environ["AZUREBLOCKBLOB_URL"])

        key_values = dict((("akey%d" % i).encode(), "avalue%d" % i)
                          for i in range(5))

        for key, value in key_values.items():
            backend.set(key, value)

        actual_values = backend.mget(key_values.keys())
        for value, actual_value in zip(key_values.values(), actual_values):
            assert value == actual_value

        for key in key_values:
            backend.delete(key)

        assert backend.get(b"doesNotExist") is None
