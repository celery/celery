from __future__ import absolute_import
from __future__ import with_statement

from pickle import loads, dumps
from mock import Mock

from celery import states
from celery.app import app_or_default
from celery.exceptions import IncompleteStream
from celery.utils import uuid
from celery.utils.serialization import pickle
from celery.result import (
    AsyncResult,
    EagerResult,
    GroupResult,
    TaskSetResult,
    ResultSet,
    from_serializable,
)
from celery.exceptions import TimeoutError
from celery.task import task
from celery.task.base import Task

from celery.tests.utils import AppCase
from celery.tests.utils import skip_if_quick


@task()
def mytask():
    pass


def mock_task(name, state, result):
    return dict(id=uuid(), name=name, state=state, result=result)


def save_result(task):
    app = app_or_default()
    traceback = 'Some traceback'
    if task['state'] == states.SUCCESS:
        app.backend.mark_as_done(task['id'], task['result'])
    elif task['state'] == states.RETRY:
        app.backend.mark_as_retry(
            task['id'], task['result'], traceback=traceback,
        )
    else:
        app.backend.mark_as_failure(
            task['id'], task['result'], traceback=traceback,
        )


def make_mock_group(size=10):
    tasks = [mock_task('ts%d' % i, states.SUCCESS, i) for i in xrange(size)]
    [save_result(task) for task in tasks]
    return [AsyncResult(task['id']) for task in tasks]


class test_AsyncResult(AppCase):

    def setup(self):
        self.task1 = mock_task('task1', states.SUCCESS, 'the')
        self.task2 = mock_task('task2', states.SUCCESS, 'quick')
        self.task3 = mock_task('task3', states.FAILURE, KeyError('brown'))
        self.task4 = mock_task('task3', states.RETRY, KeyError('red'))

        for task in (self.task1, self.task2, self.task3, self.task4):
            save_result(task)

    def test_compat_properties(self):
        x = AsyncResult('1')
        self.assertEqual(x.task_id, x.id)
        x.task_id = '2'
        self.assertEqual(x.id, '2')

    def test_children(self):
        x = AsyncResult('1')
        children = [EagerResult(str(i), i, states.SUCCESS) for i in range(3)]
        x.backend = Mock()
        x.backend.get_children.return_value = children
        x.backend.READY_STATES = states.READY_STATES
        self.assertTrue(x.children)
        self.assertEqual(len(x.children), 3)

    def test_get_children(self):
        tid = uuid()
        x = AsyncResult(tid)
        child = [AsyncResult(uuid()).serializable() for i in xrange(10)]
        x.backend._cache[tid] = {'children': child}
        self.assertTrue(x.children)
        self.assertEqual(len(x.children), 10)

        x.backend._cache[tid] = {'result': None}
        self.assertIsNone(x.children)

    def test_build_graph_get_leaf_collect(self):
        x = AsyncResult('1')
        x.backend._cache['1'] = {'status': states.SUCCESS, 'result': None}
        c = [EagerResult(str(i), i, states.SUCCESS) for i in range(3)]
        x.iterdeps = Mock()
        x.iterdeps.return_value = (
            (None, x),
            (x, c[0]),
            (c[0], c[1]),
            (c[1], c[2])
        )
        x.backend.READY_STATES = states.READY_STATES
        self.assertTrue(x.graph)

        self.assertIs(x.get_leaf(), 2)

        it = x.collect()
        self.assertListEqual(list(it), [
            (x, None),
            (c[0], 0),
            (c[1], 1),
            (c[2], 2),
        ])

    def test_iterdeps(self):
        x = AsyncResult('1')
        x.backend._cache['1'] = {'status': states.SUCCESS, 'result': None}
        c = [EagerResult(str(i), i, states.SUCCESS) for i in range(3)]
        for child in c:
            child.backend = Mock()
            child.backend.get_children.return_value = []
        x.backend.get_children = Mock()
        x.backend.get_children.return_value = c
        it = x.iterdeps()
        self.assertListEqual(list(it), [
            (None, x),
            (x, c[0]),
            (x, c[1]),
            (x, c[2]),
        ])
        x.backend._cache.pop('1')
        x.ready = Mock()
        x.ready.return_value = False
        with self.assertRaises(IncompleteStream):
            list(x.iterdeps())
        list(x.iterdeps(intermediate=True))

    def test_eq_not_implemented(self):
        self.assertFalse(AsyncResult('1') == object())

    def test_reduce(self):
        a1 = AsyncResult('uuid', task_name=mytask.name)
        restored = pickle.loads(pickle.dumps(a1))
        self.assertEqual(restored.id, 'uuid')
        self.assertEqual(restored.task_name, mytask.name)

        a2 = AsyncResult('uuid')
        self.assertEqual(pickle.loads(pickle.dumps(a2)).id, 'uuid')

    def test_successful(self):
        ok_res = AsyncResult(self.task1['id'])
        nok_res = AsyncResult(self.task3['id'])
        nok_res2 = AsyncResult(self.task4['id'])

        self.assertTrue(ok_res.successful())
        self.assertFalse(nok_res.successful())
        self.assertFalse(nok_res2.successful())

        pending_res = AsyncResult(uuid())
        self.assertFalse(pending_res.successful())

    def test_str(self):
        ok_res = AsyncResult(self.task1['id'])
        ok2_res = AsyncResult(self.task2['id'])
        nok_res = AsyncResult(self.task3['id'])
        self.assertEqual(str(ok_res), self.task1['id'])
        self.assertEqual(str(ok2_res), self.task2['id'])
        self.assertEqual(str(nok_res), self.task3['id'])

        pending_id = uuid()
        pending_res = AsyncResult(pending_id)
        self.assertEqual(str(pending_res), pending_id)

    def test_repr(self):
        ok_res = AsyncResult(self.task1['id'])
        ok2_res = AsyncResult(self.task2['id'])
        nok_res = AsyncResult(self.task3['id'])
        self.assertEqual(repr(ok_res), '<AsyncResult: %s>' % (
            self.task1['id']))
        self.assertEqual(repr(ok2_res), '<AsyncResult: %s>' % (
            self.task2['id']))
        self.assertEqual(repr(nok_res), '<AsyncResult: %s>' % (
            self.task3['id']))

        pending_id = uuid()
        pending_res = AsyncResult(pending_id)
        self.assertEqual(repr(pending_res), '<AsyncResult: %s>' % (
            pending_id))

    def test_hash(self):
        self.assertEqual(hash(AsyncResult('x0w991')),
                         hash(AsyncResult('x0w991')))
        self.assertNotEqual(hash(AsyncResult('x0w991')),
                            hash(AsyncResult('x1w991')))

    def test_get_traceback(self):
        ok_res = AsyncResult(self.task1['id'])
        nok_res = AsyncResult(self.task3['id'])
        nok_res2 = AsyncResult(self.task4['id'])
        self.assertFalse(ok_res.traceback)
        self.assertTrue(nok_res.traceback)
        self.assertTrue(nok_res2.traceback)

        pending_res = AsyncResult(uuid())
        self.assertFalse(pending_res.traceback)

    def test_get(self):
        ok_res = AsyncResult(self.task1['id'])
        ok2_res = AsyncResult(self.task2['id'])
        nok_res = AsyncResult(self.task3['id'])
        nok2_res = AsyncResult(self.task4['id'])

        self.assertEqual(ok_res.get(), 'the')
        self.assertEqual(ok2_res.get(), 'quick')
        with self.assertRaises(KeyError):
            nok_res.get()
        self.assertTrue(nok_res.get(propagate=False))
        self.assertIsInstance(nok2_res.result, KeyError)
        self.assertEqual(ok_res.info, 'the')

    def test_get_timeout(self):
        res = AsyncResult(self.task4['id'])             # has RETRY state
        with self.assertRaises(TimeoutError):
            res.get(timeout=0.1)

        pending_res = AsyncResult(uuid())
        with self.assertRaises(TimeoutError):
            pending_res.get(timeout=0.1)

    @skip_if_quick
    def test_get_timeout_longer(self):
        res = AsyncResult(self.task4['id'])             # has RETRY state
        with self.assertRaises(TimeoutError):
            res.get(timeout=1)

    def test_ready(self):
        oks = (AsyncResult(self.task1['id']),
               AsyncResult(self.task2['id']),
               AsyncResult(self.task3['id']))
        self.assertTrue(all(result.ready() for result in oks))
        self.assertFalse(AsyncResult(self.task4['id']).ready())

        self.assertFalse(AsyncResult(uuid()).ready())


class test_ResultSet(AppCase):

    def test_resultset_repr(self):
        self.assertTrue(repr(ResultSet(map(AsyncResult, ['1', '2', '3']))))

    def test_eq_other(self):
        self.assertFalse(ResultSet([1, 3, 3]) == 1)
        self.assertTrue(ResultSet([1]) == ResultSet([1]))

    def test_get(self):
        x = ResultSet(map(AsyncResult, [1, 2, 3]))
        b = x.results[0].backend = Mock()
        b.supports_native_join = False
        x.join_native = Mock()
        x.join = Mock()
        x.get()
        self.assertTrue(x.join.called)
        b.supports_native_join = True
        x.get()
        self.assertTrue(x.join_native.called)

    def test_add(self):
        x = ResultSet([1])
        x.add(2)
        self.assertEqual(len(x), 2)
        x.add(2)
        self.assertEqual(len(x), 2)

    def test_add_discard(self):
        x = ResultSet([])
        x.add(AsyncResult('1'))
        self.assertIn(AsyncResult('1'), x.results)
        x.discard(AsyncResult('1'))
        x.discard(AsyncResult('1'))
        x.discard('1')
        self.assertNotIn(AsyncResult('1'), x.results)

        x.update([AsyncResult('2')])

    def test_clear(self):
        x = ResultSet([])
        r = x.results
        x.clear()
        self.assertIs(x.results, r)


class MockAsyncResultFailure(AsyncResult):

    @property
    def result(self):
        return KeyError('baz')

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
            return ((id, {'result': i, 'status': states.SUCCESS})
                    for i, id in enumerate(self.ids))


class test_TaskSetResult(AppCase):

    def setup(self):
        self.size = 10
        self.ts = TaskSetResult(uuid(), make_mock_group(self.size))

    def test_total(self):
        self.assertEqual(self.ts.total, self.size)

    def test_compat_properties(self):
        self.assertEqual(self.ts.taskset_id, self.ts.id)
        self.ts.taskset_id = 'foo'
        self.assertEqual(self.ts.taskset_id, 'foo')

    def test_compat_subtasks_kwarg(self):
        x = TaskSetResult(uuid(), subtasks=[1, 2, 3])
        self.assertEqual(x.results, [1, 2, 3])

    def test_itersubtasks(self):
        it = self.ts.itersubtasks()

        for i, t in enumerate(it):
            self.assertEqual(t.get(), i)


class test_GroupResult(AppCase):

    def setup(self):
        self.size = 10
        self.ts = GroupResult(uuid(), make_mock_group(self.size))

    def test_len(self):
        self.assertEqual(len(self.ts), self.size)

    def test_eq_other(self):
        self.assertFalse(self.ts == 1)

    def test_reduce(self):
        self.assertTrue(loads(dumps(self.ts)))

    def test_iterate_raises(self):
        ar = MockAsyncResultFailure(uuid())
        ts = GroupResult(uuid(), [ar])
        it = iter(ts)
        with self.assertRaises(KeyError):
            it.next()

    def test_forget(self):
        subs = [MockAsyncResultSuccess(uuid()),
                MockAsyncResultSuccess(uuid())]
        ts = GroupResult(uuid(), subs)
        ts.forget()
        for sub in subs:
            self.assertTrue(sub.forgotten)

    def test_getitem(self):
        subs = [MockAsyncResultSuccess(uuid()),
                MockAsyncResultSuccess(uuid())]
        ts = GroupResult(uuid(), subs)
        self.assertIs(ts[0], subs[0])

    def test_save_restore(self):
        subs = [MockAsyncResultSuccess(uuid()),
                MockAsyncResultSuccess(uuid())]
        ts = GroupResult(uuid(), subs)
        ts.save()
        with self.assertRaises(AttributeError):
            ts.save(backend=object())
        self.assertEqual(GroupResult.restore(ts.id).subtasks,
                         ts.subtasks)
        ts.delete()
        self.assertIsNone(GroupResult.restore(ts.id))
        with self.assertRaises(AttributeError):
            GroupResult.restore(ts.id, backend=object())

    def test_join_native(self):
        backend = SimpleBackend()
        subtasks = [AsyncResult(uuid(), backend=backend)
                    for i in range(10)]
        ts = GroupResult(uuid(), subtasks)
        backend.ids = [subtask.id for subtask in subtasks]
        res = ts.join_native()
        self.assertEqual(res, range(10))

    def test_iter_native(self):
        backend = SimpleBackend()
        subtasks = [AsyncResult(uuid(), backend=backend)
                    for i in range(10)]
        ts = GroupResult(uuid(), subtasks)
        backend.ids = [subtask.id for subtask in subtasks]
        self.assertEqual(len(list(ts.iter_native())), 10)

    def test_iterate_yields(self):
        ar = MockAsyncResultSuccess(uuid())
        ar2 = MockAsyncResultSuccess(uuid())
        ts = GroupResult(uuid(), [ar, ar2])
        it = iter(ts)
        self.assertEqual(it.next(), 42)
        self.assertEqual(it.next(), 42)

    def test_iterate_eager(self):
        ar1 = EagerResult(uuid(), 42, states.SUCCESS)
        ar2 = EagerResult(uuid(), 42, states.SUCCESS)
        ts = GroupResult(uuid(), [ar1, ar2])
        it = iter(ts)
        self.assertEqual(it.next(), 42)
        self.assertEqual(it.next(), 42)

    def test_join_timeout(self):
        ar = MockAsyncResultSuccess(uuid())
        ar2 = MockAsyncResultSuccess(uuid())
        ar3 = AsyncResult(uuid())
        ts = GroupResult(uuid(), [ar, ar2, ar3])
        with self.assertRaises(TimeoutError):
            ts.join(timeout=0.0000001)

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


class test_pending_AsyncResult(AppCase):

    def setup(self):
        self.task = AsyncResult(uuid())

    def test_result(self):
        self.assertIsNone(self.task.result)


class test_failed_AsyncResult(test_GroupResult):

    def setup(self):
        self.size = 11
        subtasks = make_mock_group(10)
        failed = mock_task('ts11', states.FAILURE, KeyError('Baz'))
        save_result(failed)
        failed_res = AsyncResult(failed['id'])
        self.ts = GroupResult(uuid(), subtasks + [failed_res])

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


class test_pending_Group(AppCase):

    def setup(self):
        self.ts = GroupResult(uuid(), [AsyncResult(uuid()),
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
        raise KeyError('xy')


class test_EagerResult(AppCase):

    def test_wait_raises(self):
        res = RaisingTask.apply(args=[3, 3])
        with self.assertRaises(KeyError):
            res.wait()
        self.assertTrue(res.wait(propagate=False))

    def test_wait(self):
        res = EagerResult('x', 'x', states.RETRY)
        res.wait()
        self.assertEqual(res.state, states.RETRY)
        self.assertEqual(res.status, states.RETRY)

    def test_forget(self):
        res = EagerResult('x', 'x', states.RETRY)
        res.forget()

    def test_revoke(self):
        res = RaisingTask.apply(args=[3, 3])
        self.assertFalse(res.revoke())


class test_serializable(AppCase):

    def test_AsyncResult(self):
        x = AsyncResult(uuid())
        self.assertEqual(x, from_serializable(x.serializable(), self.app))
        self.assertEqual(x, from_serializable(x, self.app))

    def test_GroupResult(self):
        x = GroupResult(uuid(), [AsyncResult(uuid()) for _ in range(10)])
        self.assertEqual(x, from_serializable(x.serializable(), self.app))
        self.assertEqual(x, from_serializable(x, self.app))
