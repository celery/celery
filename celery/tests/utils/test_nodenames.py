from __future__ import absolute_import, unicode_literals

from kombu import Queue

from celery.utils.nodenames import worker_direct

from celery.tests.case import Case


class test_worker_direct(Case):

    def test_returns_if_queue(self):
        q = Queue('foo')
        self.assertIs(worker_direct(q), q)
