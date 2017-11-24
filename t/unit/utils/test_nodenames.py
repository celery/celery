from __future__ import absolute_import, unicode_literals

from celery.utils.nodenames import worker_direct
from kombu import Queue


class test_worker_direct:

    def test_returns_if_queue(self):
        q = Queue('foo')
        assert worker_direct(q) is q
