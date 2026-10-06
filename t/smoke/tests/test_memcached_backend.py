"""Smoke tests for the Memcached result backend.

These tests run against a real memcached server through pymemcache, so
that any mismatch with the pymemcache client signatures (for example in
the chord counter) or a silently dropped result shows up here.
"""

from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryBackendCluster, CeleryTestSetup, MemcachedTestBackend

from celery import states
from celery.canvas import chord, group
from t.integration.tasks import add, identity, tsum

# Larger than the default memcached item size limit (1 MB).
TOO_LARGE_RESULT_SIZE = 2 * 1024 * 1024


@pytest.fixture
def celery_backend_cluster(celery_memcached_backend: MemcachedTestBackend) -> CeleryBackendCluster:
    cluster = CeleryBackendCluster(celery_memcached_backend)
    yield cluster
    cluster.teardown()


class test_memcached_backend:
    def test_result_roundtrip(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        res = add.s(2, 2).apply_async(queue=queue)
        assert res.get(timeout=RESULT_TIMEOUT) == 4
        assert res.state == states.SUCCESS

    def test_chord(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        header = group(add.si(i, i).set(queue=queue) for i in range(5))
        sig = chord(header, tsum.s().set(queue=queue))
        res = sig.apply_async(queue=queue)
        assert res.get(timeout=RESULT_TIMEOUT) == sum(i + i for i in range(5))

    def test_too_large_result_is_not_silently_dropped(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        res = identity.s("x" * TOO_LARGE_RESULT_SIZE).apply_async(queue=queue)
        with pytest.raises(Exception):
            res.get(timeout=RESULT_TIMEOUT)
        assert res.state == states.FAILURE
