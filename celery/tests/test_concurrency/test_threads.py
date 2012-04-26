from __future__ import absolute_import
from __future__ import with_statement

import sys

from contextlib import contextmanager
from mock import Mock
from types import ModuleType

from celery.concurrency.threads import NullDict, TaskPool, apply_target

from celery.tests.utils import Case, mask_modules


class test_NullDict(Case):

    def test_setitem(self):
        x = NullDict()
        x["foo"] = 1
        with self.assertRaises(KeyError):
            x["foo"]


@contextmanager
def threadpool_module():

    prev = sys.modules.get("threadpool")
    tp = sys.modules["threadpool"] = ModuleType("threadpool")
    tp.WorkRequest = Mock()
    tp.ThreadPool = Mock()
    yield tp
    if prev:
        sys.modules["threadpool"] = prev


class test_TaskPool(Case):

    def test_without_threadpool(self):

        with mask_modules("threadpool"):
            with self.assertRaises(ImportError):
                TaskPool()

    def test_with_threadpool(self):
        with threadpool_module():
            x = TaskPool()
            self.assertTrue(x.ThreadPool)
            self.assertTrue(x.WorkRequest)

    def test_on_start(self):
        with threadpool_module():
            x = TaskPool()
            x.on_start()
            self.assertTrue(x._pool)
            self.assertIsInstance(x._pool.workRequests, NullDict)

    def test_on_stop(self):
        with threadpool_module():
            x = TaskPool()
            x.on_start()
            x.on_stop()
            x._pool.dismissWorkers.assert_called_with(x.limit, do_join=True)

    def test_on_apply(self):
        with threadpool_module():
            x = TaskPool()
            x.on_start()
            callback = Mock()
            accept_callback = Mock()
            target = Mock()
            req = x.on_apply(target, args=(1, 2), kwargs={"a": 10},
                callback=callback, accept_callback=accept_callback)
            x.WorkRequest.assert_called_with(apply_target, (
                target, (1, 2), {"a": 10}, callback, accept_callback))
            x._pool.putRequest.assert_called_with(req)
            x._pool._results_queue.queue.clear.assert_called_with()
