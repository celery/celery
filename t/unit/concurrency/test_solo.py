<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
import operator

from celery.concurrency import solo
from celery.utils.functional import noop


class test_solo_TaskPool:

    def test_on_start(self):
        x = solo.TaskPool()
        x.on_start()

    def test_on_apply(self):
        x = solo.TaskPool()
        x.on_start()
        x.on_apply(operator.add, (2, 2), {}, noop, noop)

    def test_info(self):
        x = solo.TaskPool()
        x.on_start()
        assert x.info
