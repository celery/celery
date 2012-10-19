from __future__ import absolute_import

from celery.contrib.abortable import AbortableTask, AbortableAsyncResult
from celery.result import AsyncResult
from celery.tests.utils import Case


class MyAbortableTask(AbortableTask):

    def run(self, **kwargs):
        return True


class test_AbortableTask(Case):

    def test_async_result_is_abortable(self):
        t = MyAbortableTask()
        result = t.apply_async()
        tid = result.id
        self.assertIsInstance(t.AsyncResult(tid), AbortableAsyncResult)

    def test_is_not_aborted(self):
        t = MyAbortableTask()
        t.push_request()
        try:
            result = t.apply_async()
            tid = result.id
            self.assertFalse(t.is_aborted(task_id=tid))
        finally:
            t.pop_request()

    def test_is_aborted_not_abort_result(self):
        t = MyAbortableTask()
        t.AsyncResult = AsyncResult
        t.push_request()
        try:
            t.request.id = 'foo'
            self.assertFalse(t.is_aborted())
        finally:
            t.pop_request()

    def test_abort_yields_aborted(self):
        t = MyAbortableTask()
        t.push_request()
        try:
            result = t.apply_async()
            result.abort()
            tid = result.id
            self.assertTrue(t.is_aborted(task_id=tid))
        finally:
            t.pop_request()
