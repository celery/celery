from __future__ import generators

from celery.tests.utils import unittest

from celery import states
from celery.app import app_or_default
from celery.utils import gen_unique_id
from celery.utils.compat import all
from celery.utils.serialization import pickle
from celery.result import AsyncResult, EagerResult, TaskSetResult
from celery.exceptions import TimeoutError
from celery.task.base import Task

from celery.tests.utils import skip_if_quick


def mock_task(name, status, result):
    return dict(id=gen_unique_id(), name=name, status=status, result=result)


def save_result(task):
    app = app_or_default()
    traceback = "Some traceback"
    if task["status"] == states.SUCCESS:
        app.backend.mark_as_done(task["id"], task["result"])
    elif task["status"] == states.RETRY:
        app.backend.mark_as_retry(task["id"], task["result"],
                traceback=traceback)
    else:
        app.backend.mark_as_failure(task["id"], task["result"],
                traceback=traceback)


def make_mock_taskset(size=10):
    tasks = [mock_task("ts%d" % i, states.SUCCESS, i) for i in xrange(size)]
    [save_result(task) for task in tasks]
    return [AsyncResult(task["id"]) for task in tasks]


class TestAsyncResult(unittest.TestCase):

    def setUp(self):
        self.task1 = mock_task("task1", states.SUCCESS, "the")
        self.task2 = mock_task("task2", states.SUCCESS, "quick")
        self.task3 = mock_task("task3", states.FAILURE, KeyError("brown"))
        self.task4 = mock_task("task3", states.RETRY, KeyError("red"))

        for task in (self.task1, self.task2, self.task3, self.task4):
            save_result(task)

    def test_reduce(self):
        a1 = AsyncResult("uuid", task_name="celery.ping")
        restored = pickle.loads(pickle.dumps(a1))
        self.assertEqual(restored.task_id, "uuid")
        self.assertEqual(restored.task_name, "celery.ping")

        a2 = AsyncResult("uuid")
        self.assertEqual(pickle.loads(pickle.dumps(a2)).task_id, "uuid")

    def test_successful(self):
        ok_res = AsyncResult(self.task1["id"])
        nok_res = AsyncResult(self.task3["id"])
        nok_res2 = AsyncResult(self.task4["id"])

        self.assertTrue(ok_res.successful())
        self.assertFalse(nok_res.successful())
        self.assertFalse(nok_res2.successful())

        pending_res = AsyncResult(gen_unique_id())
        self.assertFalse(pending_res.successful())

    def test_str(self):
        ok_res = AsyncResult(self.task1["id"])
        ok2_res = AsyncResult(self.task2["id"])
        nok_res = AsyncResult(self.task3["id"])
        self.assertEqual(str(ok_res), self.task1["id"])
        self.assertEqual(str(ok2_res), self.task2["id"])
        self.assertEqual(str(nok_res), self.task3["id"])

        pending_id = gen_unique_id()
        pending_res = AsyncResult(pending_id)
        self.assertEqual(str(pending_res), pending_id)

    def test_repr(self):
        ok_res = AsyncResult(self.task1["id"])
        ok2_res = AsyncResult(self.task2["id"])
        nok_res = AsyncResult(self.task3["id"])
        self.assertEqual(repr(ok_res), "<AsyncResult: %s>" % (
                self.task1["id"]))
        self.assertEqual(repr(ok2_res), "<AsyncResult: %s>" % (
                self.task2["id"]))
        self.assertEqual(repr(nok_res), "<AsyncResult: %s>" % (
                self.task3["id"]))

        pending_id = gen_unique_id()
        pending_res = AsyncResult(pending_id)
        self.assertEqual(repr(pending_res), "<AsyncResult: %s>" % (
                pending_id))

    def test_get_traceback(self):
        ok_res = AsyncResult(self.task1["id"])
        nok_res = AsyncResult(self.task3["id"])
        nok_res2 = AsyncResult(self.task4["id"])
        self.assertFalse(ok_res.traceback)
        self.assertTrue(nok_res.traceback)
        self.assertTrue(nok_res2.traceback)

        pending_res = AsyncResult(gen_unique_id())
        self.assertFalse(pending_res.traceback)

    def test_get(self):
        ok_res = AsyncResult(self.task1["id"])
        ok2_res = AsyncResult(self.task2["id"])
        nok_res = AsyncResult(self.task3["id"])
        nok2_res = AsyncResult(self.task4["id"])

        self.assertEqual(ok_res.get(), "the")
        self.assertEqual(ok2_res.get(), "quick")
        self.assertRaises(KeyError, nok_res.get)
        self.assertIsInstance(nok2_res.result, KeyError)
        self.assertEqual(ok_res.info, "the")

    def test_get_timeout(self):
        res = AsyncResult(self.task4["id"])             # has RETRY status
        self.assertRaises(TimeoutError, res.get, timeout=0.1)

        pending_res = AsyncResult(gen_unique_id())
        self.assertRaises(TimeoutError, pending_res.get, timeout=0.1)

    @skip_if_quick
    def test_get_timeout_longer(self):
        res = AsyncResult(self.task4["id"])             # has RETRY status
        self.assertRaises(TimeoutError, res.get, timeout=1)

    def test_ready(self):
        oks = (AsyncResult(self.task1["id"]),
               AsyncResult(self.task2["id"]),
               AsyncResult(self.task3["id"]))
        self.assertTrue(all(result.ready() for result in oks))
        self.assertFalse(AsyncResult(self.task4["id"]).ready())

        self.assertFalse(AsyncResult(gen_unique_id()).ready())


class MockAsyncResultFailure(AsyncResult):

    @property
    def result(self):
        return KeyError("baz")

    @property
    def status(self):
        return states.FAILURE


class MockAsyncResultSuccess(AsyncResult):
    forgotten = False

    def forget(self):
        self.forgotten = True

    @property
    def result(self):
        return 42

    @property
    def status(self):
        return states.SUCCESS


class SimpleBackend(object):
        ids = []

        def __init__(self, ids=[]):
            self.ids = ids

        def get_many(self, *args, **kwargs):
            return ((id, {"result": i}) for i, id in enumerate(self.ids))


class TestTaskSetResult(unittest.TestCase):

    def setUp(self):
        self.size = 10
        self.ts = TaskSetResult(gen_unique_id(), make_mock_taskset(self.size))

    def test_total(self):
        self.assertEqual(self.ts.total, self.size)

    def test_iterate_raises(self):
        ar = MockAsyncResultFailure(gen_unique_id())
        ts = TaskSetResult(gen_unique_id(), [ar])
        it = iter(ts)
        self.assertRaises(KeyError, it.next)

    def test_forget(self):
        subs = [MockAsyncResultSuccess(gen_unique_id()),
                MockAsyncResultSuccess(gen_unique_id())]
        ts = TaskSetResult(gen_unique_id(), subs)
        ts.forget()
        for sub in subs:
            self.assertTrue(sub.forgotten)

    def test_getitem(self):
        subs = [MockAsyncResultSuccess(gen_unique_id()),
                MockAsyncResultSuccess(gen_unique_id())]
        ts = TaskSetResult(gen_unique_id(), subs)
        self.assertIs(ts[0], subs[0])

    def test_save_restore(self):
        subs = [MockAsyncResultSuccess(gen_unique_id()),
                MockAsyncResultSuccess(gen_unique_id())]
        ts = TaskSetResult(gen_unique_id(), subs)
        ts.save()
        self.assertRaises(AttributeError, ts.save, backend=object())
        self.assertEqual(TaskSetResult.restore(ts.taskset_id).subtasks,
                         ts.subtasks)
        self.assertRaises(AttributeError,
                          TaskSetResult.restore, ts.taskset_id,
                          backend=object())

    def test_join_native(self):
        backend = SimpleBackend()
        subtasks = [AsyncResult(gen_unique_id(), backend=backend)
                        for i in range(10)]
        ts = TaskSetResult(gen_unique_id(), subtasks)
        backend.ids = [subtask.task_id for subtask in subtasks]
        res = ts.join_native()
        self.assertEqual(res, range(10))

    def test_iter_native(self):
        backend = SimpleBackend()
        subtasks = [AsyncResult(gen_unique_id(), backend=backend)
                        for i in range(10)]
        ts = TaskSetResult(gen_unique_id(), subtasks)
        backend.ids = [subtask.task_id for subtask in subtasks]
        self.assertEqual(len(list(ts.iter_native())), 10)

    def test_iterate_yields(self):
        ar = MockAsyncResultSuccess(gen_unique_id())
        ar2 = MockAsyncResultSuccess(gen_unique_id())
        ts = TaskSetResult(gen_unique_id(), [ar, ar2])
        it = iter(ts)
        self.assertEqual(it.next(), 42)
        self.assertEqual(it.next(), 42)

    def test_iterate_eager(self):
        ar1 = EagerResult(gen_unique_id(), 42, states.SUCCESS)
        ar2 = EagerResult(gen_unique_id(), 42, states.SUCCESS)
        ts = TaskSetResult(gen_unique_id(), [ar1, ar2])
        it = iter(ts)
        self.assertEqual(it.next(), 42)
        self.assertEqual(it.next(), 42)

    def test_join_timeout(self):
        ar = MockAsyncResultSuccess(gen_unique_id())
        ar2 = MockAsyncResultSuccess(gen_unique_id())
        ar3 = AsyncResult(gen_unique_id())
        ts = TaskSetResult(gen_unique_id(), [ar, ar2, ar3])
        self.assertRaises(TimeoutError, ts.join, timeout=0.0000001)

    def test_itersubtasks(self):

        it = self.ts.itersubtasks()

        for i, t in enumerate(it):
            self.assertEqual(t.get(), i)

    def test___iter__(self):

        it = iter(self.ts)

        results = sorted(list(it))
        self.assertListEqual(results, list(xrange(self.size)))

    def test_join(self):
        joined = self.ts.join()
        self.assertListEqual(joined, list(xrange(self.size)))

    def test_successful(self):
        self.assertTrue(self.ts.successful())

    def test_failed(self):
        self.assertFalse(self.ts.failed())

    def test_waiting(self):
        self.assertFalse(self.ts.waiting())

    def test_ready(self):
        self.assertTrue(self.ts.ready())

    def test_completed_count(self):
        self.assertEqual(self.ts.completed_count(), self.ts.total)


class TestPendingAsyncResult(unittest.TestCase):

    def setUp(self):
        self.task = AsyncResult(gen_unique_id())

    def test_result(self):
        self.assertIsNone(self.task.result)


class TestFailedTaskSetResult(TestTaskSetResult):

    def setUp(self):
        self.size = 11
        subtasks = make_mock_taskset(10)
        failed = mock_task("ts11", states.FAILURE, KeyError("Baz"))
        save_result(failed)
        failed_res = AsyncResult(failed["id"])
        self.ts = TaskSetResult(gen_unique_id(), subtasks + [failed_res])

    def test_itersubtasks(self):

        it = self.ts.itersubtasks()

        for i in xrange(self.size - 1):
            t = it.next()
            self.assertEqual(t.get(), i)
        self.assertRaises(KeyError, it.next().get)

    def test_completed_count(self):
        self.assertEqual(self.ts.completed_count(), self.ts.total - 1)

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
        self.assertEqual(self.ts.completed_count(), 0)

    def test_ready(self):
        self.assertFalse(self.ts.ready())

    def test_waiting(self):
        self.assertTrue(self.ts.waiting())

    def x_join(self):
        self.assertRaises(TimeoutError, self.ts.join, timeout=0.001)

    @skip_if_quick
    def x_join_longer(self):
        self.assertRaises(TimeoutError, self.ts.join, timeout=1)


class RaisingTask(Task):

    def run(self, x, y):
        raise KeyError("xy")


class TestEagerResult(unittest.TestCase):

    def test_wait_raises(self):
        res = RaisingTask.apply(args=[3, 3])
        self.assertRaises(KeyError, res.wait)

    def test_wait(self):
        res = EagerResult("x", "x", states.RETRY)
        res.wait()
        self.assertEqual(res.state, states.RETRY)
        self.assertEqual(res.status, states.RETRY)

    def test_revoke(self):
        res = RaisingTask.apply(args=[3, 3])
        self.assertFalse(res.revoke())
