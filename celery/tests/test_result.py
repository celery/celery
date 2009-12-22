import unittest

from celery.utils import gen_unique_id
from celery.tests.utils import skip_if_quick
from celery.result import AsyncResult, TaskSetResult
from celery.backends import default_backend
from celery.exceptions import TimeoutError


def mock_task(name, status, result):
    return dict(id=gen_unique_id(), name=name, status=status, result=result)


def save_result(task):
    if task["status"] == "SUCCESS":
        default_backend.mark_as_done(task["id"], task["result"])
    elif task["status"] == "RETRY":
        default_backend.mark_as_retry(task["id"], task["result"])
    else:
        default_backend.mark_as_failure(task["id"], task["result"])


def make_mock_taskset(size=10):
    tasks = [mock_task("ts%d" % i, "SUCCESS", i) for i in xrange(size)]
    [save_result(task) for task in tasks]
    return [AsyncResult(task["id"]) for task in tasks]


class TestAsyncResult(unittest.TestCase):

    def setUp(self):
        self.task1 = mock_task("task1", "SUCCESS", "the")
        self.task2 = mock_task("task2", "SUCCESS", "quick")
        self.task3 = mock_task("task3", "FAILURE", KeyError("brown"))
        self.task4 = mock_task("task3", "RETRY", KeyError("red"))

        for task in (self.task1, self.task2, self.task3, self.task4):
            save_result(task)

    def test_successful(self):
        ok_res = AsyncResult(self.task1["id"])
        nok_res = AsyncResult(self.task3["id"])
        nok_res2 = AsyncResult(self.task4["id"])

        self.assertTrue(ok_res.successful())
        self.assertFalse(nok_res.successful())
        self.assertFalse(nok_res2.successful())

    def test_str(self):
        ok_res = AsyncResult(self.task1["id"])
        ok2_res = AsyncResult(self.task2["id"])
        nok_res = AsyncResult(self.task3["id"])
        self.assertEquals(str(ok_res), self.task1["id"])
        self.assertEquals(str(ok2_res), self.task2["id"])
        self.assertEquals(str(nok_res), self.task3["id"])

    def test_repr(self):
        ok_res = AsyncResult(self.task1["id"])
        ok2_res = AsyncResult(self.task2["id"])
        nok_res = AsyncResult(self.task3["id"])
        self.assertEquals(repr(ok_res), "<AsyncResult: %s>" % (
                self.task1["id"]))
        self.assertEquals(repr(ok2_res), "<AsyncResult: %s>" % (
                self.task2["id"]))
        self.assertEquals(repr(nok_res), "<AsyncResult: %s>" % (
                self.task3["id"]))

    def test_get(self):
        ok_res = AsyncResult(self.task1["id"])
        ok2_res = AsyncResult(self.task2["id"])
        nok_res = AsyncResult(self.task3["id"])
        nok2_res = AsyncResult(self.task4["id"])

        self.assertEquals(ok_res.get(), "the")
        self.assertEquals(ok2_res.get(), "quick")
        self.assertRaises(KeyError, nok_res.get)
        self.assertTrue(isinstance(nok2_res.result, KeyError))

    def test_get_timeout(self):
        res = AsyncResult(self.task4["id"]) # has RETRY status
        self.assertRaises(TimeoutError, res.get, timeout=0.1)

    @skip_if_quick
    def test_get_timeout_longer(self):
        res = AsyncResult(self.task4["id"]) # has RETRY status
        self.assertRaises(TimeoutError, res.get, timeout=1)

    def test_ready(self):
        oks = (AsyncResult(self.task1["id"]),
               AsyncResult(self.task2["id"]),
               AsyncResult(self.task3["id"]))
        [self.assertTrue(ok.ready()) for ok in oks]
        self.assertFalse(AsyncResult(self.task4["id"]).ready())


class TestTaskSetResult(unittest.TestCase):

    def setUp(self):
        self.size = 10
        self.ts = TaskSetResult(gen_unique_id(), make_mock_taskset(self.size))

    def test_total(self):
        self.assertEquals(self.ts.total, self.size)

    def test_itersubtasks(self):

        it = self.ts.itersubtasks()

        for i, t in enumerate(it):
            self.assertEquals(t.get(), i)

    def test___iter__(self):

        it = iter(self.ts)

        results = sorted(list(it))
        self.assertEquals(results, list(xrange(self.size)))

    def test_join(self):
        joined = self.ts.join()
        self.assertEquals(joined, list(xrange(self.size)))

    def test_successful(self):
        self.assertTrue(self.ts.successful())

    def test_failed(self):
        self.assertFalse(self.ts.failed())

    def test_waiting(self):
        self.assertFalse(self.ts.waiting())

    def test_ready(self):
        self.assertTrue(self.ts.ready())

    def test_completed_count(self):
        self.assertEquals(self.ts.completed_count(), self.ts.total)


class TestPendingAsyncResult(unittest.TestCase):

    def setUp(self):
        self.task = AsyncResult(gen_unique_id())

    def test_result(self):
        self.assertTrue(self.task.result is None)


class TestFailedTaskSetResult(TestTaskSetResult):

    def setUp(self):
        self.size = 11
        subtasks = make_mock_taskset(10)
        failed = mock_task("ts11", "FAILED", KeyError("Baz"))
        save_result(failed)
        failed_res = AsyncResult(failed["id"])
        self.ts = TaskSetResult(gen_unique_id(), subtasks + [failed_res])

    def test_itersubtasks(self):

        it = self.ts.itersubtasks()

        for i in xrange(self.size - 1):
            t = it.next()
            self.assertEquals(t.get(), i)
        self.assertRaises(KeyError, it.next().get)

    def test_completed_count(self):
        self.assertEquals(self.ts.completed_count(), self.ts.total - 1)

    def test___iter__(self):
        it = iter(self.ts)

        def consume():
            return list(it)

        self.assertRaises(KeyError, consume)

    def test_join(self):
        self.assertRaises(KeyError, self.ts.join)

    def test_successful(self):
        self.assertFalse(self.ts.successful())

    def test_failed(self):
        self.assertTrue(self.ts.failed())


class TestTaskSetPending(unittest.TestCase):

    def setUp(self):
        self.ts = TaskSetResult(gen_unique_id(), [
                                        AsyncResult(gen_unique_id()),
                                        AsyncResult(gen_unique_id())])

    def test_completed_count(self):
        self.assertEquals(self.ts.completed_count(), 0)

    def test_ready(self):
        self.assertFalse(self.ts.ready())

    def test_waiting(self):
        self.assertTrue(self.ts.waiting())

    def x_join(self):
        self.assertRaises(TimeoutError, self.ts.join, timeout=0.001)

    @skip_if_quick
    def x_join_longer(self):
        self.assertRaises(TimeoutError, self.ts.join, timeout=1)
