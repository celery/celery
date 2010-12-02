from celery.tests.utils import unittest

from celery.contrib.abortable import AbortableTask, AbortableAsyncResult


class MyAbortableTask(AbortableTask):

    def run(self, **kwargs):
        return True


class TestAbortableTask(unittest.TestCase):

    def test_async_result_is_abortable(self):
        t = MyAbortableTask()
        result = t.apply_async()
        tid = result.task_id
        self.assertIsInstance(t.AsyncResult(tid), AbortableAsyncResult)

    def test_is_not_aborted(self):
        t = MyAbortableTask()
        result = t.apply_async()
        tid = result.task_id
        self.assertFalse(t.is_aborted(task_id=tid))

    def test_abort_yields_aborted(self):
        t = MyAbortableTask()
        result = t.apply_async()
        result.abort()
        tid = result.task_id
        self.assertTrue(t.is_aborted(task_id=tid))
