<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from kombu import Queue

from celery.utils.nodenames import worker_direct


class test_worker_direct:

    def test_returns_if_queue(self):
        q = Queue('foo')
        assert worker_direct(q) is q
