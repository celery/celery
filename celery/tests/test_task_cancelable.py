import unittest2 as unittest

from celery.contrib.cancelable import CancelableTask, CancelableAsyncResult, CANCELLED

class MyCancelableTask(CancelableTask):
    def run(self, **kwargs):
        return True

class TestCancelableTask(unittest.TestCase):
    def test_async_result_is_cancelable(self):
        t = MyCancelableTask()
        result = t.apply_async()
        tid = result.task_id
        self.assertIsInstance(t.AsyncResult(tid), \
                              CancelableAsyncResult)

    def test_is_not_cancelled(self):
        t = MyCancelableTask()
        result = t.apply_async()
        tid = result.task_id
        self.assertFalse(t.is_cancelled(task_id=tid))

    def test_cancel_yields_cancelled(self):
        t = MyCancelableTask()
        result = t.apply_async()
        result.cancel()
        tid = result.task_id
        self.assertTrue(t.is_cancelled(task_id=tid))

