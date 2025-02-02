import operator

import pytest

from celery.utils.functional import noop


class test_thread_TaskPool:

    def test_on_apply(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        try:
            x.on_apply(operator.add, (2, 2), {}, noop, noop)
        finally:
            x.stop()

    def test_info(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        try:
            assert x.info
        finally:
            x.stop()

    def test_on_stop(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        x.on_stop()
        with pytest.raises(RuntimeError):
            x.on_apply(operator.add, (2, 2), {}, noop, noop)
