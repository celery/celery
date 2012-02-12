from __future__ import absolute_import
from __future__ import with_statement

from celery import states
from celery.app import app_or_default
from celery.utils import uuid
from celery.utils.serialization import pickle
from celery.result import AsyncResult, EagerResult, TaskSetResult, ResultSet
from celery.exceptions import TimeoutError
from celery.task import task
from celery.task.base import Task

from celery.tests.utils import AppCase
from celery.tests.utils import skip_if_quick


@task
def mytask():
    pass


def mock_task(name, state, result):
    return dict(id=uuid(), name=name, state=state, result=result)


def save_result(task):
    app = app_or_default()
    traceback = "Some traceback"
    if task["state"] == states.SUCCESS:
        app.backend.mark_as_done(task["id"], task["result"])
    elif task["state"] == states.RETRY:
        app.backend.mark_as_retry(task["id"], task["result"],
                traceback=traceback)
    else:
        app.backend.mark_as_failure(task["id"], task["result"],
                traceback=traceback)


def make_mock_taskset(size=10):
    tasks = [mock_task("ts%d" % i, states.SUCCESS, i) for i in xrange(size)]
    [save_result(task) for task in tasks]
    return [AsyncResult(task["id"]) for task in tasks]


class TestAsyncResult(AppCase):

    def setup(self):
        self.task1 = mock_task("task1", states.SUCCESS, "the")
        self.task2 = mock_task("task2", states.SUCCESS, "quick")
        self.task3 = mock_task("task3", states.FAILURE, KeyError("brown"))
        self.task4 = mock_task("task3", states.RETRY, KeyError("red"))

        for task in (self.task1, self.task2, self.task3, self.task4):
            save_result(task)

    def test_reduce(self):
        a1 = AsyncResult("uuid", task_name=mytask.name)
        restored = pickle.loads(pickle.dumps(a1))
        self.assertEqual(restored.task_id, "uuid")
        self.assertEqual(restored.task_name, mytask.name)

        a2 = AsyncResult("uuid")
        self.assertEqual(pickle.loads(pickle.dumps(a2)).task_id, "uuid")

    def test_successful(self):
        ok_res = AsyncResult(self.task1["id"])
        nok_res = AsyncResult(self.task3["id"])
        nok_res2 = AsyncResult(self.task4["id"])

        self.assertTrue(ok_res.successful())
        self.assertFalse(nok_res.successful())
        self.assertFalse(nok_res2.successful())

        pending_res = AsyncResult(uuid())
        self.assertFalse(pending_res.successful())

    def test_str(self):
        ok_res = AsyncResult(self.task1["id"])
        ok2_res = AsyncResult(self.task2["id"])
        nok_res = AsyncResult(self.task3["id"])
        self.assertEqual(str(ok_res), self.task1["id"])
        self.assertEqual(str(ok2_res), self.task2["id"])
        self.assertEqual(str(nok_res), self.task3["id"])

        pending_id = uuid()
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

        pending_id = uuid()
        pending_res = AsyncResult(pending_id)
        self.assertEqual(repr(pending_res), "<AsyncResult: %s>" % (
                pending_id))

    def test_hash(self):
        self.assertEqual(hash(AsyncResult("x0w991")),
                         hash(AsyncResult("x0w991")))
        self.assertNotEqual(hash(AsyncResult("x0w991")),
                            hash(AsyncResult("x1w991")))

    def test_get_traceback(self):
        ok_res = AsyncResult(self.task1["id"])
        nok_res = AsyncResult(self.task3["id"])
        nok_res2 = AsyncResult(self.task4["id"])
        self.assertFalse(ok_res.traceback)
        self.assertTrue(nok_res.traceback)
        self.assertTrue(nok_res2.traceback)

        pending_res = AsyncResult(uuid())
        self.assertFalse(pending_res.traceback)

    def test_get(self):
        ok_res = AsyncResult(self.task1["id"])
        ok2_res = AsyncResult(self.task2["id"])
        nok_res = AsyncResult(self.task3["id"])
        nok2_res = AsyncResult(self.task4["id"])

        self.assertEqual(ok_res.get(), "the")
        self.assertEqual(ok2_res.get(), "quick")
        with self.assertRaises(KeyError):
            nok_res.get()
        self.assertIsInstance(nok2_res.result, KeyError)
        self.assertEqual(ok_res.info, "the")

    def test_get_timeout(self):
        res = AsyncResult(self.task4["id"])             # has RETRY state
        with self.assertRaises(TimeoutError):
            res.get(timeout=0.1)

        pending_res = AsyncResult(uuid())
        with self.assertRaises(TimeoutError):
            pending_res.get(timeout=0.1)

    @skip_if_quick
    def test_get_timeout_longer(self):
        res = AsyncResult(self.task4["id"])             # has RETRY state
        with self.assertRaises(TimeoutError):
            res.get(timeout=1)

    def test_ready(self):
        oks = (AsyncResult(self.task1["id"]),
               AsyncResult(self.task2["id"]),
               AsyncResult(self.task3["id"]))
        self.assertTrue(all(result.ready() for result in oks))
        self.assertFalse(AsyncResult(self.task4["id"]).ready())

        self.assertFalse(AsyncResult(uuid()).ready())


class test_ResultSet(AppCase):

    def test_add_discard(self):
        x = ResultSet([])
        x.add(AsyncResult("1"))
        self.assertIn(AsyncResult("1"), x.results)
        x.discard(AsyncResult("1"))
        x.discard(AsyncResult("1"))
        x.discard("1")
        self.assertNotIn(AsyncResult("1"), x.results)

        x.update([AsyncResult("2")])

    def test_clear(self):
        x = ResultSet([])
        r = x.results
        x.clear()
        self.assertIs(x.results, r)


class MockAsyncResultFailure(AsyncResult):

    @property
    def result(self):
        return KeyError("baz")

    @property
    def state(self):
        return states.FAILURE

    def get(self, propagate=True, **kwargs):
        if propagate:
            raise self.result
        return self.result


class MockAsyncResultSuccess(AsyncResult):
    forgotten = False

    def forget(self):
        self.forgotten = True

    @property
    def result(self):
        return 42

    @property
    def state(self):
        return states.SUCCESS

    def get(self, **kwargs):
        return self.result


class SimpleBackend(object):
        ids = []

        def __init__(self, ids=[]):
            self.ids = ids

        def get_many(self, *args, **kwargs):
            return ((id, {"result": i}) for i, id in enumerate(self.ids))


class TestTaskSetResult(AppCase):

    def setup(self):
        self.size = 10
        self.ts = TaskSetResult(uuid(), make_mock_taskset(self.size))

    def test_total(self):
        self.assertEqual(len(self.ts), self.size)
        self.assertEqual(self.ts.total, self.size)

    def test_iterate_raises(self):
        ar = MockAsyncResultFailure(uuid())
        ts = TaskSetResult(uuid(), [ar])
        it = iter(ts)
        with self.assertRaises(KeyError):
            it.next()

    def test_forget(self):
        subs = [MockAsyncResultSuccess(uuid()),
                MockAsyncResultSuccess(uuid())]
        ts = TaskSetResult(uuid(), subs)
        ts.forget()
        for sub in subs:
            self.assertTrue(sub.forgotten)

    def test_getitem(self):
        subs = [MockAsyncResultSuccess(uuid()),
                MockAsyncResultSuccess(uuid())]
        ts = TaskSetResult(uuid(), subs)
        self.assertIs(ts[0], subs[0])

    def test_save_restore(self):
        subs = [MockAsyncResultSuccess(uuid()),
                MockAsyncResultSuccess(uuid())]
        ts = TaskSetResult(uuid(), subs)
        ts.save()
        with self.assertRaises(AttributeError):
            ts.save(backend=object())
        self.assertEqual(TaskSetResult.restore(ts.taskset_id).subtasks,
                         ts.subtasks)
        ts.delete()
        self.assertIsNone(TaskSetResult.restore(ts.taskset_id))
        with self.assertRaises(AttributeError):
            TaskSetResult.restore(ts.taskset_id, backend=object())

    def test_join_native(self):
        backend = SimpleBackend()
        subtasks = [AsyncResult(uuid(), backend=backend)
                        for i in range(10)]
        ts = TaskSetResult(uuid(), subtasks)
        backend.ids = [subtask.task_id for subtask in subtasks]
        res = ts.join_native()
        self.assertEqual(res, range(10))

    def test_iter_native(self):
        backend = SimpleBackend()
        subtasks = [AsyncResult(uuid(), backend=backend)
                        for i in range(10)]
        ts = TaskSetResult(uuid(), subtasks)
        backend.ids = [subtask.task_id for subtask in subtasks]
        self.assertEqual(len(list(ts.iter_native())), 10)

    def test_iterate_yields(self):
        ar = MockAsyncResultSuccess(uuid())
        ar2 = MockAsyncResultSuccess(uuid())
        ts = TaskSetResult(uuid(), [ar, ar2])
        it = iter(ts)
        self.assertEqual(it.next(), 42)
        self.assertEqual(it.next(), 42)

    def test_iterate_eager(self):
        ar1 = EagerResult(uuid(), 42, states.SUCCESS)
        ar2 = EagerResult(uuid(), 42, states.SUCCESS)
        ts = TaskSetResult(uuid(), [ar1, ar2])
        it = iter(ts)
        self.assertEqual(it.next(), 42)
        self.assertEqual(it.next(), 42)

    def test_join_timeout(self):
        ar = MockAsyncResultSuccess(uuid())
        ar2 = MockAsyncResultSuccess(uuid())
        ar3 = AsyncResult(uuid())
        ts = TaskSetResult(uuid(), [ar, ar2, ar3])
        with self.assertRaises(TimeoutError):
            ts.join(timeout=0.0000001)

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
        self.assertEqual(self.ts.completed_count(), len(self.ts))


class TestPendingAsyncResult(AppCase):

    def setup(self):
        self.task = AsyncResult(uuid())

    def test_result(self):
        self.assertIsNone(self.task.result)


class TestFailedTaskSetResult(TestTaskSetResult):

    def setup(self):
        self.size = 11
        subtasks = make_mock_taskset(10)
        failed = mock_task("ts11", states.FAILURE, KeyError("Baz"))
        save_result(failed)
        failed_res = AsyncResult(failed["id"])
        self.ts = TaskSetResult(uuid(), subtasks + [failed_res])

    def test_itersubtasks(self):

        it = self.ts.itersubtasks()

        for i in xrange(self.size - 1):
            t = it.next()
            self.assertEqual(t.get(), i)
        with self.assertRaises(KeyError):
            t = it.next()   # need to do this in two lines or 2to3 borks.
            t.get()

    def test_completed_count(self):
        self.assertEqual(self.ts.completed_count(), len(self.ts) - 1)

    def test___iter__(self):
        it = iter(self.ts)

        def consume():
            return list(it)

        with self.assertRaises(KeyError):
            consume()

    def test_join(self):
        with self.assertRaises(KeyError):
            self.ts.join()

    def test_successful(self):
        self.assertFalse(self.ts.successful())

    def test_failed(self):
        self.assertTrue(self.ts.failed())


class TestTaskSetPending(AppCase):

    def setup(self):
        self.ts = TaskSetResult(uuid(), [
                                        AsyncResult(uuid()),
                                        AsyncResult(uuid())])

    def test_completed_count(self):
        self.assertEqual(self.ts.completed_count(), 0)

    def test_ready(self):
        self.assertFalse(self.ts.ready())

    def test_waiting(self):
        self.assertTrue(self.ts.waiting())

    def x_join(self):
        with self.assertRaises(TimeoutError):
            self.ts.join(timeout=0.001)

    @skip_if_quick
    def x_join_longer(self):
        with self.assertRaises(TimeoutError):
            self.ts.join(timeout=1)


class RaisingTask(Task):

    def run(self, x, y):
        raise KeyError("xy")


class TestEagerResult(AppCase):

    def test_wait_raises(self):
        res = RaisingTask.apply(args=[3, 3])
        with self.assertRaises(KeyError):
            res.wait()

    def test_wait(self):
        res = EagerResult("x", "x", states.RETRY)
        res.wait()
        self.assertEqual(res.state, states.RETRY)
        self.assertEqual(res.status, states.RETRY)

    def test_revoke(self):
        res = RaisingTask.apply(args=[3, 3])
        self.assertFalse(res.revoke())
