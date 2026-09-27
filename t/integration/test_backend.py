import os

import pytest

from celery import states
from celery.backends.azureblockblob import AzureBlockBlobBackend

pytest.importorskip('azure')


@pytest.mark.skipif(
    not os.environ.get('AZUREBLOCKBLOB_URL'),
    reason='Environment variable AZUREBLOCKBLOB_URL required'
)
class test_AzureBlockBlobBackend:
    def test_crud(self, manager):
        backend = AzureBlockBlobBackend(
            app=manager.app,
            url=os.environ["AZUREBLOCKBLOB_URL"])

        key_values = {("akey%d" % i).encode(): "avalue%d" % i
                      for i in range(5)}

        for key, value in key_values.items():
            backend._set_with_state(key, value, states.SUCCESS)

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


class test_pending_message_buffer:
    """The async result poller parks per-task metas in
    ``backend._pending_messages`` (a ``BufferMap``) between poll iterations,
    and ``AsyncResult.get()`` consumes them via ``take()``."""

    def test_get_delivers_result_landing_between_polls(self, manager):
        result = manager.app.signature('tasks.add', args=[4, 40]).apply_async()
        assert result.get(timeout=60) == 44

    def test_high_volume_single_key_does_not_evict_other_keys(self, manager):
        # Regression for the BufferMap.total accounting fixed in #10705:
        # on main, 1500 puts to one key pushed `total` to 1500 while the
        # buffer held 1000, evicting other keys' pending messages early.
        backend = manager.app.backend
        for i in range(1500):
            backend._pending_messages.put('test-busy-key', {'seq': i})
        backend._pending_messages.put('test-other-key', {'keep': True})
        assert backend._pending_messages.take('test-other-key') is not None
