from __future__ import absolute_import, unicode_literals

import operator

import pytest

from celery.utils.functional import noop


class test_thread_TaskPool:

    def test_on_apply(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        x.on_apply(operator.add, (2, 2), {}, noop, noop)

    def test_info(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        assert x.info

    def test_on_stop(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        x.on_stop()
        with pytest.raises(RuntimeError):
            x.on_apply(operator.add, (2, 2), {}, noop, noop)
