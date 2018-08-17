from __future__ import absolute_import, unicode_literals

import copy
import traceback
from contextlib import contextmanager

import pytest
from case import Mock, call, patch, skip

from celery import states, uuid
from celery.app.task import Context
from celery.backends.base import SyncBackendMixin
from celery.exceptions import (CPendingDeprecationWarning,
                               ImproperlyConfigured, IncompleteStream,
                               TimeoutError)
from celery.five import range
from celery.result import (AsyncResult, EagerResult, GroupResult, ResultSet,
                           assert_will_not_block, result_from_tuple)
from celery.utils.serialization import pickle

PYTRACEBACK = """\
Traceback (most recent call last):
  File "foo.py", line 2, in foofunc
    don't matter
  File "bar.py", line 3, in barfunc
    don't matter
Doesn't matter: really!\
"""


def mock_task(name, state, result, traceback=None):
    return {
        'id': uuid(), 'name': name, 'state': state,
        'result': result, 'traceback': traceback,
    }


def save_result(app, task):
    traceback = task.get('traceback') or 'Some traceback'
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


def make_mock_group(app, size=10):
    tasks = [mock_task('ts%d' % i, states.SUCCESS, i) for i in range(size)]
    [save_result(app, task) for task in tasks]
    return [app.AsyncResult(task['id']) for task in tasks]


class _MockBackend:
    def add_pending_result(self, *args, **kwargs):
        return True

    def wait_for_pending(self, *args, **kwargs):
        return True


class test_AsyncResult:

    def setup(self):
        self.app.conf.result_cache_max = 100
        self.app.conf.result_serializer = 'pickle'
        self.app.conf.result_extended = True
        self.task1 = mock_task('task1', states.SUCCESS, 'the')
        self.task2 = mock_task('task2', states.SUCCESS, 'quick')
        self.task3 = mock_task('task3', states.FAILURE, KeyError('brown'))
        self.task4 = mock_task('task3', states.RETRY, KeyError('red'))
        self.task5 = mock_task(
            'task3', states.FAILURE, KeyError('blue'), PYTRACEBACK,
        )
        for task in (self.task1, self.task2,
                     self.task3, self.task4, self.task5):
            save_result(self.app, task)

        @self.app.task(shared=False)
        def mytask():
            pass
        self.mytask = mytask

    def test_ignored_getter(self):
        result = self.app.AsyncResult(uuid())
        assert result.ignored is False
        result.__delattr__('_ignored')
        assert result.ignored is False

    @patch('celery.result.task_join_will_block')
    def test_assert_will_not_block(self, task_join_will_block):
        task_join_will_block.return_value = True
        with pytest.raises(RuntimeError):
            assert_will_not_block()
        task_join_will_block.return_value = False
        assert_will_not_block()

    @patch('celery.result.task_join_will_block')
    def test_get_sync_subtask_option(self, task_join_will_block):
        task_join_will_block.return_value = True
        tid = uuid()
        backend = _MockBackend()
        res_subtask_async = AsyncResult(tid, backend=backend)
        with pytest.raises(RuntimeError):
            res_subtask_async.get()
        res_subtask_async.get(disable_sync_subtasks=False)

    def test_without_id(self):
        with pytest.raises(ValueError):
            AsyncResult(None, app=self.app)

    def test_compat_properties(self):
        x = self.app.AsyncResult('1')
        assert x.task_id == x.id
        x.task_id = '2'
        assert x.id == '2'

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce_direct(self):
        x = AsyncResult('1', app=self.app)
        fun, args = x.__reduce__()
        assert fun(*args) == x

    def test_children(self):
        x = self.app.AsyncResult('1')
        children = [EagerResult(str(i), i, states.SUCCESS) for i in range(3)]
        x._cache = {'children': children, 'status': states.SUCCESS}
        x.backend = Mock()
        assert x.children
        assert len(x.children) == 3

    def test_propagates_for_parent(self):
        x = self.app.AsyncResult(uuid())
        x.backend = Mock(name='backend')
        x.backend.get_task_meta.return_value = {}
        x.backend.wait_for_pending.return_value = 84
        x.parent = EagerResult(uuid(), KeyError('foo'), states.FAILURE)
        with pytest.raises(KeyError):
            x.get(propagate=True)
        x.backend.wait_for_pending.assert_not_called()

        x.parent = EagerResult(uuid(), 42, states.SUCCESS)
        assert x.get(propagate=True) == 84
        x.backend.wait_for_pending.assert_called()

    def test_get_children(self):
        tid = uuid()
        x = self.app.AsyncResult(tid)
        child = [self.app.AsyncResult(uuid()).as_tuple()
                 for i in range(10)]
        x._cache = {'children': child}
        assert x.children
        assert len(x.children) == 10

        x._cache = {'status': states.SUCCESS}
        x.backend._cache[tid] = {'result': None}
        assert x.children is None

    def test_build_graph_get_leaf_collect(self):
        x = self.app.AsyncResult('1')
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
        assert x.graph
        assert x.get_leaf() is 2

        it = x.collect()
        assert list(it) == [
            (x, None),
            (c[0], 0),
            (c[1], 1),
            (c[2], 2),
        ]

    def test_iterdeps(self):
        x = self.app.AsyncResult('1')
        c = [EagerResult(str(i), i, states.SUCCESS) for i in range(3)]
        x._cache = {'status': states.SUCCESS, 'result': None, 'children': c}
        for child in c:
            child.backend = Mock()
            child.backend.get_children.return_value = []
        it = x.iterdeps()
        assert list(it) == [
            (None, x),
            (x, c[0]),
            (x, c[1]),
            (x, c[2]),
        ]
        x._cache = None
        x.ready = Mock()
        x.ready.return_value = False
        with pytest.raises(IncompleteStream):
            list(x.iterdeps())
        list(x.iterdeps(intermediate=True))

    def test_eq_not_implemented(self):
        assert self.app.AsyncResult('1') != object()

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce(self):
        a1 = self.app.AsyncResult('uuid')
        restored = pickle.loads(pickle.dumps(a1))
        assert restored.id == 'uuid'

        a2 = self.app.AsyncResult('uuid')
        assert pickle.loads(pickle.dumps(a2)).id == 'uuid'

    def test_maybe_set_cache_empty(self):
        self.app.AsyncResult('uuid')._maybe_set_cache(None)

    def test_set_cache__children(self):
        r1 = self.app.AsyncResult('id1')
        r2 = self.app.AsyncResult('id2')
        r1._set_cache({'children': [r2.as_tuple()]})
        assert r2 in r1.children

    def test_successful(self):
        ok_res = self.app.AsyncResult(self.task1['id'])
        nok_res = self.app.AsyncResult(self.task3['id'])
        nok_res2 = self.app.AsyncResult(self.task4['id'])

        assert ok_res.successful()
        assert not nok_res.successful()
        assert not nok_res2.successful()

        pending_res = self.app.AsyncResult(uuid())
        assert not pending_res.successful()

    def test_raising(self):
        notb = self.app.AsyncResult(self.task3['id'])
        withtb = self.app.AsyncResult(self.task5['id'])

        with pytest.raises(KeyError):
            notb.get()
        try:
            withtb.get()
        except KeyError:
            tb = traceback.format_exc()
            assert '  File "foo.py", line 2, in foofunc' not in tb
            assert '  File "bar.py", line 3, in barfunc' not in tb
            assert 'KeyError:' in tb
            assert "'blue'" in tb
        else:
            raise AssertionError('Did not raise KeyError.')

    @skip.unless_module('tblib')
    def test_raising_remote_tracebacks(self):
        withtb = self.app.AsyncResult(self.task5['id'])
        self.app.conf.task_remote_tracebacks = True
        try:
            withtb.get()
        except KeyError:
            tb = traceback.format_exc()
            assert '  File "foo.py", line 2, in foofunc' in tb
            assert '  File "bar.py", line 3, in barfunc' in tb
            assert 'KeyError:' in tb
            assert "'blue'" in tb
        else:
            raise AssertionError('Did not raise KeyError.')

    def test_str(self):
        ok_res = self.app.AsyncResult(self.task1['id'])
        ok2_res = self.app.AsyncResult(self.task2['id'])
        nok_res = self.app.AsyncResult(self.task3['id'])
        assert str(ok_res) == self.task1['id']
        assert str(ok2_res) == self.task2['id']
        assert str(nok_res) == self.task3['id']

        pending_id = uuid()
        pending_res = self.app.AsyncResult(pending_id)
        assert str(pending_res) == pending_id

    def test_repr(self):
        ok_res = self.app.AsyncResult(self.task1['id'])
        ok2_res = self.app.AsyncResult(self.task2['id'])
        nok_res = self.app.AsyncResult(self.task3['id'])
        assert repr(ok_res) == '<AsyncResult: %s>' % (self.task1['id'],)
        assert repr(ok2_res) == '<AsyncResult: %s>' % (self.task2['id'],)
        assert repr(nok_res) == '<AsyncResult: %s>' % (self.task3['id'],)

        pending_id = uuid()
        pending_res = self.app.AsyncResult(pending_id)
        assert repr(pending_res) == '<AsyncResult: %s>' % (pending_id,)

    def test_hash(self):
        assert (hash(self.app.AsyncResult('x0w991')) ==
                hash(self.app.AsyncResult('x0w991')))
        assert (hash(self.app.AsyncResult('x0w991')) !=
                hash(self.app.AsyncResult('x1w991')))

    def test_get_traceback(self):
        ok_res = self.app.AsyncResult(self.task1['id'])
        nok_res = self.app.AsyncResult(self.task3['id'])
        nok_res2 = self.app.AsyncResult(self.task4['id'])
        assert not ok_res.traceback
        assert nok_res.traceback
        assert nok_res2.traceback

        pending_res = self.app.AsyncResult(uuid())
        assert not pending_res.traceback

    def test_get__backend_gives_None(self):
        res = self.app.AsyncResult(self.task1['id'])
        res.backend.wait_for = Mock(name='wait_for')
        res.backend.wait_for.return_value = None
        assert res.get() is None

    def test_get(self):
        ok_res = self.app.AsyncResult(self.task1['id'])
        ok2_res = self.app.AsyncResult(self.task2['id'])
        nok_res = self.app.AsyncResult(self.task3['id'])
        nok2_res = self.app.AsyncResult(self.task4['id'])

        callback = Mock(name='callback')

        assert ok_res.get(callback=callback) == 'the'
        callback.assert_called_with(ok_res.id, 'the')
        assert ok2_res.get() == 'quick'
        with pytest.raises(KeyError):
            nok_res.get()
        assert nok_res.get(propagate=False)
        assert isinstance(nok2_res.result, KeyError)
        assert ok_res.info == 'the'

    def test_get_when_ignored(self):
        result = self.app.AsyncResult(uuid())
        result.ignored = True
        # Does not block
        assert result.get() is None

    def test_eq_ne(self):
        r1 = self.app.AsyncResult(self.task1['id'])
        r2 = self.app.AsyncResult(self.task1['id'])
        r3 = self.app.AsyncResult(self.task2['id'])
        assert r1 == r2
        assert r1 != r3
        assert r1 == r2.id
        assert r1 != r3.id

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce_restore(self):
        r1 = self.app.AsyncResult(self.task1['id'])
        fun, args = r1.__reduce__()
        assert fun(*args) == r1

    def test_get_timeout(self):
        res = self.app.AsyncResult(self.task4['id'])  # has RETRY state
        with pytest.raises(TimeoutError):
            res.get(timeout=0.001)

        pending_res = self.app.AsyncResult(uuid())
        with patch('celery.result.time') as _time:
            with pytest.raises(TimeoutError):
                pending_res.get(timeout=0.001, interval=0.001)
                _time.sleep.assert_called_with(0.001)

    def test_get_timeout_longer(self):
        res = self.app.AsyncResult(self.task4['id'])  # has RETRY state
        with patch('celery.result.time') as _time:
            with pytest.raises(TimeoutError):
                res.get(timeout=1, interval=1)
                _time.sleep.assert_called_with(1)

    def test_ready(self):
        oks = (self.app.AsyncResult(self.task1['id']),
               self.app.AsyncResult(self.task2['id']),
               self.app.AsyncResult(self.task3['id']))
        assert all(result.ready() for result in oks)
        assert not self.app.AsyncResult(self.task4['id']).ready()

        assert not self.app.AsyncResult(uuid()).ready()

    def test_del(self):
        with patch('celery.result.AsyncResult.backend') as backend:
            result = self.app.AsyncResult(self.task1['id'])
            result_clone = copy.copy(result)
            del result
            assert backend.remove_pending_result.called_once_with(
                result_clone
            )

        result = self.app.AsyncResult(self.task1['id'])
        result.backend = None
        del result

    def test_get_request_meta(self):

        x = self.app.AsyncResult('1')
        request = Context(
            task_name='foo',
            children=None,
            args=['one', 'two'],
            kwargs={'kwarg1': 'three'},
            hostname="foo",
            retries=1,
            delivery_info={'routing_key': 'celery'}
        )
        x.backend.store_result(task_id="1", result='foo', state=states.SUCCESS,
                               traceback=None, request=request)
        assert x.name == 'foo'
        assert x.args == ['one', 'two']
        assert x.kwargs == {'kwarg1': 'three'}
        assert x.worker == 'foo'
        assert x.retries == 1
        assert x.queue == 'celery'
        assert x.date_done is not None
        assert x.task_id == "1"
        assert x.state == "SUCCESS"


class test_ResultSet:

    def test_resultset_repr(self):
        assert repr(self.app.ResultSet(
            [self.app.AsyncResult(t) for t in ['1', '2', '3']]))

    def test_eq_other(self):
        assert self.app.ResultSet([
            self.app.AsyncResult(t) for t in [1, 3, 3]]) != 1
        rs1 = self.app.ResultSet([self.app.AsyncResult(1)])
        rs2 = self.app.ResultSet([self.app.AsyncResult(1)])
        assert rs1 == rs2

    def test_get(self):
        x = self.app.ResultSet([self.app.AsyncResult(t) for t in [1, 2, 3]])
        b = x.results[0].backend = Mock()
        b.supports_native_join = False
        x.join_native = Mock()
        x.join = Mock()
        x.get()
        x.join.assert_called()
        b.supports_native_join = True
        x.get()
        x.join_native.assert_called()

    def test_eq_ne(self):
        g1 = self.app.ResultSet([
            self.app.AsyncResult('id1'),
            self.app.AsyncResult('id2'),
        ])
        g2 = self.app.ResultSet([
            self.app.AsyncResult('id1'),
            self.app.AsyncResult('id2'),
        ])
        g3 = self.app.ResultSet([
            self.app.AsyncResult('id3'),
            self.app.AsyncResult('id1'),
        ])
        assert g1 == g2
        assert g1 != g3
        assert g1 != object()

    def test_takes_app_from_first_task(self):
        x = ResultSet([self.app.AsyncResult('id1')])
        assert x.app is x.results[0].app
        x.app = self.app
        assert x.app is self.app

    def test_get_empty(self):
        x = self.app.ResultSet([])
        assert x.supports_native_join is None
        x.join = Mock(name='join')
        x.get()
        x.join.assert_called()

    def test_add(self):
        x = self.app.ResultSet([self.app.AsyncResult(1)])
        x.add(self.app.AsyncResult(2))
        assert len(x) == 2
        x.add(self.app.AsyncResult(2))
        assert len(x) == 2

    @contextmanager
    def dummy_copy(self):
        with patch('celery.result.copy') as copy:

            def passt(arg):
                return arg
            copy.side_effect = passt

            yield

    def test_iterate_respects_subpolling_interval(self):
        r1 = self.app.AsyncResult(uuid())
        r2 = self.app.AsyncResult(uuid())
        backend = r1.backend = r2.backend = Mock()
        backend.subpolling_interval = 10

        ready = r1.ready = r2.ready = Mock()

        def se(*args, **kwargs):
            ready.side_effect = KeyError()
            return False
        ready.return_value = False
        ready.side_effect = se

        x = self.app.ResultSet([r1, r2])
        with self.dummy_copy():
            with patch('celery.result.time') as _time:
                with pytest.warns(CPendingDeprecationWarning):
                    with pytest.raises(KeyError):
                        list(x.iterate())
                _time.sleep.assert_called_with(10)

            backend.subpolling_interval = 0
            with patch('celery.result.time') as _time:
                with pytest.warns(CPendingDeprecationWarning):
                    with pytest.raises(KeyError):
                        ready.return_value = False
                        ready.side_effect = se
                        list(x.iterate())
                    _time.sleep.assert_not_called()

    def test_times_out(self):
        r1 = self.app.AsyncResult(uuid)
        r1.ready = Mock()
        r1.ready.return_value = False
        x = self.app.ResultSet([r1])
        with self.dummy_copy():
            with patch('celery.result.time'):
                with pytest.warns(CPendingDeprecationWarning):
                    with pytest.raises(TimeoutError):
                        list(x.iterate(timeout=1))

    def test_add_discard(self):
        x = self.app.ResultSet([])
        x.add(self.app.AsyncResult('1'))
        assert self.app.AsyncResult('1') in x.results
        x.discard(self.app.AsyncResult('1'))
        x.discard(self.app.AsyncResult('1'))
        x.discard('1')
        assert self.app.AsyncResult('1') not in x.results

        x.update([self.app.AsyncResult('2')])

    def test_clear(self):
        x = self.app.ResultSet([])
        r = x.results
        x.clear()
        assert x.results is r


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

    def __init__(self, *args, **kwargs):
        self._result = kwargs.pop('result', 42)
        super(MockAsyncResultSuccess, self).__init__(*args, **kwargs)

    def forget(self):
        self.forgotten = True

    @property
    def result(self):
        return self._result

    @property
    def state(self):
        return states.SUCCESS

    def get(self, **kwargs):
        return self.result


class SimpleBackend(SyncBackendMixin):
    ids = []

    def __init__(self, ids=[]):
        self.ids = ids

    def _ensure_not_eager(self):
        pass

    def get_many(self, *args, **kwargs):
        return ((id, {'result': i, 'status': states.SUCCESS})
                for i, id in enumerate(self.ids))


class test_GroupResult:

    def setup(self):
        self.size = 10
        self.ts = self.app.GroupResult(
            uuid(), make_mock_group(self.app, self.size),
        )

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_is_pickleable(self):
        ts = self.app.GroupResult(uuid(), [self.app.AsyncResult(uuid())])
        assert pickle.loads(pickle.dumps(ts)) == ts
        ts2 = self.app.GroupResult(uuid(), [self.app.AsyncResult(uuid())])
        assert pickle.loads(pickle.dumps(ts2)) == ts2

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce(self):
        ts = self.app.GroupResult(uuid(), [self.app.AsyncResult(uuid())])
        fun, args = ts.__reduce__()
        ts2 = fun(*args)
        assert ts2.id == ts.id
        assert ts == ts2

    def test_eq_ne(self):
        ts = self.app.GroupResult(uuid(), [self.app.AsyncResult(uuid())])
        ts2 = self.app.GroupResult(ts.id, ts.results)
        ts3 = self.app.GroupResult(uuid(), [self.app.AsyncResult(uuid())])
        ts4 = self.app.GroupResult(ts.id, [self.app.AsyncResult(uuid())])
        assert ts == ts2
        assert ts != ts3
        assert ts != ts4
        assert ts != object()

    def test_len(self):
        assert len(self.ts) == self.size

    def test_eq_other(self):
        assert self.ts != 1

    def test_eq_with_parent(self):
        # GroupResult instances with different .parent are not equal
        grp_res = self.app.GroupResult(
            uuid(), [self.app.AsyncResult(uuid()) for _ in range(10)],
            parent=self.app.AsyncResult(uuid())
        )
        grp_res_2 = self.app.GroupResult(grp_res.id, grp_res.results)
        assert grp_res != grp_res_2

        grp_res_2.parent = self.app.AsyncResult(uuid())
        assert grp_res != grp_res_2

        grp_res_2.parent = grp_res.parent
        assert grp_res == grp_res_2

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_pickleable(self):
        assert pickle.loads(pickle.dumps(self.ts))

    def test_iterate_raises(self):
        ar = MockAsyncResultFailure(uuid(), app=self.app)
        ts = self.app.GroupResult(uuid(), [ar])
        with pytest.warns(CPendingDeprecationWarning):
            it = ts.iterate()
        with pytest.raises(KeyError):
            next(it)

    def test_forget(self):
        subs = [MockAsyncResultSuccess(uuid(), app=self.app),
                MockAsyncResultSuccess(uuid(), app=self.app)]
        ts = self.app.GroupResult(uuid(), subs)
        ts.forget()
        for sub in subs:
            assert sub.forgotten

    def test_get_nested_without_native_join(self):
        backend = SimpleBackend()
        backend.supports_native_join = False
        ts = self.app.GroupResult(uuid(), [
            MockAsyncResultSuccess(uuid(), result='1.1',
                                   app=self.app, backend=backend),
            self.app.GroupResult(uuid(), [
                MockAsyncResultSuccess(uuid(), result='2.1',
                                       app=self.app, backend=backend),
                self.app.GroupResult(uuid(), [
                    MockAsyncResultSuccess(uuid(), result='3.1',
                                           app=self.app, backend=backend),
                    MockAsyncResultSuccess(uuid(), result='3.2',
                                           app=self.app, backend=backend),
                ]),
            ]),
        ])
        ts.app.backend = backend

        vals = ts.get()
        assert vals == [
            '1.1',
            [
                '2.1',
                [
                    '3.1',
                    '3.2',
                ]
            ],
        ]

    def test_getitem(self):
        subs = [MockAsyncResultSuccess(uuid(), app=self.app),
                MockAsyncResultSuccess(uuid(), app=self.app)]
        ts = self.app.GroupResult(uuid(), subs)
        assert ts[0] is subs[0]

    def test_save_restore(self):
        subs = [MockAsyncResultSuccess(uuid(), app=self.app),
                MockAsyncResultSuccess(uuid(), app=self.app)]
        ts = self.app.GroupResult(uuid(), subs)
        ts.save()
        with pytest.raises(AttributeError):
            ts.save(backend=object())
        assert self.app.GroupResult.restore(ts.id).results == ts.results
        ts.delete()
        assert self.app.GroupResult.restore(ts.id) is None
        with pytest.raises(AttributeError):
            self.app.GroupResult.restore(ts.id, backend=object())

    def test_save_restore_empty(self):
        subs = []
        ts = self.app.GroupResult(uuid(), subs)
        ts.save()
        assert isinstance(
            self.app.GroupResult.restore(ts.id),
            self.app.GroupResult,
        )
        assert self.app.GroupResult.restore(ts.id).results == ts.results == []

    def test_restore_app(self):
        subs = [MockAsyncResultSuccess(uuid(), app=self.app)]
        ts = self.app.GroupResult(uuid(), subs)
        ts.save()
        restored = GroupResult.restore(ts.id, app=self.app)
        assert restored.id == ts.id

    def test_restore_current_app_fallback(self):
        subs = [MockAsyncResultSuccess(uuid(), app=self.app)]
        ts = self.app.GroupResult(uuid(), subs)
        ts.save()
        with pytest.raises(RuntimeError,
                           message="Test depends on current_app"):
            GroupResult.restore(ts.id)

    def test_join_native(self):
        backend = SimpleBackend()
        results = [self.app.AsyncResult(uuid(), backend=backend)
                   for i in range(10)]
        ts = self.app.GroupResult(uuid(), results)
        ts.app.backend = backend
        backend.ids = [result.id for result in results]
        res = ts.join_native()
        assert res == list(range(10))
        callback = Mock(name='callback')
        assert not ts.join_native(callback=callback)
        callback.assert_has_calls([
            call(r.id, i) for i, r in enumerate(ts.results)
        ])

    def test_join_native_raises(self):
        ts = self.app.GroupResult(uuid(), [self.app.AsyncResult(uuid())])
        ts.iter_native = Mock()
        ts.iter_native.return_value = iter([
            (uuid(), {'status': states.FAILURE, 'result': KeyError()})
        ])
        with pytest.raises(KeyError):
            ts.join_native(propagate=True)

    def test_failed_join_report(self):
        res = Mock()
        ts = self.app.GroupResult(uuid(), [res])
        res.state = states.FAILURE
        res.backend.is_cached.return_value = True
        assert next(ts._failed_join_report()) is res
        res.backend.is_cached.return_value = False
        with pytest.raises(StopIteration):
            next(ts._failed_join_report())

    def test_repr(self):
        assert repr(
            self.app.GroupResult(uuid(), [self.app.AsyncResult(uuid())]))

    def test_children_is_results(self):
        ts = self.app.GroupResult(uuid(), [self.app.AsyncResult(uuid())])
        assert ts.children is ts.results

    def test_iter_native(self):
        backend = SimpleBackend()
        results = [self.app.AsyncResult(uuid(), backend=backend)
                   for i in range(10)]
        ts = self.app.GroupResult(uuid(), results)
        ts.app.backend = backend
        backend.ids = [result.id for result in results]
        assert len(list(ts.iter_native())) == 10

    def test_iterate_yields(self):
        ar = MockAsyncResultSuccess(uuid(), app=self.app)
        ar2 = MockAsyncResultSuccess(uuid(), app=self.app)
        ts = self.app.GroupResult(uuid(), [ar, ar2])
        with pytest.warns(CPendingDeprecationWarning):
            it = ts.iterate()
        assert next(it) == 42
        assert next(it) == 42

    def test_iterate_eager(self):
        ar1 = EagerResult(uuid(), 42, states.SUCCESS)
        ar2 = EagerResult(uuid(), 42, states.SUCCESS)
        ts = self.app.GroupResult(uuid(), [ar1, ar2])
        with pytest.warns(CPendingDeprecationWarning):
            it = ts.iterate()
        assert next(it) == 42
        assert next(it) == 42

    def test_join_timeout(self):
        ar = MockAsyncResultSuccess(uuid(), app=self.app)
        ar2 = MockAsyncResultSuccess(uuid(), app=self.app)
        ar3 = self.app.AsyncResult(uuid())
        ts = self.app.GroupResult(uuid(), [ar, ar2, ar3])
        with pytest.raises(TimeoutError):
            ts.join(timeout=0.0000001)

        ar4 = self.app.AsyncResult(uuid())
        ar4.get = Mock()
        ts2 = self.app.GroupResult(uuid(), [ar4])
        assert ts2.join(timeout=0.1)
        callback = Mock(name='callback')
        assert not ts2.join(timeout=0.1, callback=callback)
        callback.assert_called_with(ar4.id, ar4.get())

    def test_iter_native_when_empty_group(self):
        ts = self.app.GroupResult(uuid(), [])
        assert list(ts.iter_native()) == []

    def test_iterate_simple(self):
        with pytest.warns(CPendingDeprecationWarning):
            it = self.ts.iterate()
        results = sorted(list(it))
        assert results == list(range(self.size))

    def test___iter__(self):
        assert list(iter(self.ts)) == self.ts.results

    def test_join(self):
        joined = self.ts.join()
        assert joined == list(range(self.size))

    def test_successful(self):
        assert self.ts.successful()

    def test_failed(self):
        assert not self.ts.failed()

    def test_maybe_throw(self):
        self.ts.results = [Mock(name='r1')]
        self.ts.maybe_throw()
        self.ts.results[0].maybe_throw.assert_called_with(
            callback=None, propagate=True,
        )

    def test_join__on_message(self):
        with pytest.raises(ImproperlyConfigured):
            self.ts.join(on_message=Mock())

    def test_waiting(self):
        assert not self.ts.waiting()

    def test_ready(self):
        assert self.ts.ready()

    def test_completed_count(self):
        assert self.ts.completed_count() == len(self.ts)


class test_pending_AsyncResult:

    def test_result(self, app):
        res = app.AsyncResult(uuid())
        assert res.result is None


class test_failed_AsyncResult:

    def setup(self):
        self.size = 11
        self.app.conf.result_serializer = 'pickle'
        results = make_mock_group(self.app, 10)
        failed = mock_task('ts11', states.FAILURE, KeyError('Baz'))
        save_result(self.app, failed)
        failed_res = self.app.AsyncResult(failed['id'])
        self.ts = self.app.GroupResult(uuid(), results + [failed_res])

    def test_completed_count(self):
        assert self.ts.completed_count() == len(self.ts) - 1

    def test_iterate_simple(self):
        with pytest.warns(CPendingDeprecationWarning):
            it = self.ts.iterate()

        def consume():
            return list(it)

        with pytest.raises(KeyError):
            consume()

    def test_join(self):
        with pytest.raises(KeyError):
            self.ts.join()

    def test_successful(self):
        assert not self.ts.successful()

    def test_failed(self):
        assert self.ts.failed()


class test_pending_Group:

    def setup(self):
        self.ts = self.app.GroupResult(
            uuid(), [self.app.AsyncResult(uuid()),
                     self.app.AsyncResult(uuid())])

    def test_completed_count(self):
        assert self.ts.completed_count() == 0

    def test_ready(self):
        assert not self.ts.ready()

    def test_waiting(self):
        assert self.ts.waiting()

    def test_join(self):
        with pytest.raises(TimeoutError):
            self.ts.join(timeout=0.001)

    def test_join_longer(self):
        with pytest.raises(TimeoutError):
            self.ts.join(timeout=1)


class test_EagerResult:

    def setup(self):
        @self.app.task(shared=False)
        def raising(x, y):
            raise KeyError(x, y)
        self.raising = raising

    def test_wait_raises(self):
        res = self.raising.apply(args=[3, 3])
        with pytest.raises(KeyError):
            res.wait()
        assert res.wait(propagate=False)

    def test_wait(self):
        res = EagerResult('x', 'x', states.RETRY)
        res.wait()
        assert res.state == states.RETRY
        assert res.status == states.RETRY

    def test_forget(self):
        res = EagerResult('x', 'x', states.RETRY)
        res.forget()

    def test_revoke(self):
        res = self.raising.apply(args=[3, 3])
        assert not res.revoke()

    @patch('celery.result.task_join_will_block')
    def test_get_sync_subtask_option(self, task_join_will_block):
        task_join_will_block.return_value = True
        tid = uuid()
        res_subtask_async = EagerResult(tid, 'x', 'x', states.SUCCESS)
        with pytest.raises(RuntimeError):
            res_subtask_async.get()
        res_subtask_async.get(disable_sync_subtasks=False)


class test_tuples:

    def test_AsyncResult(self):
        x = self.app.AsyncResult(uuid())
        assert x, result_from_tuple(x.as_tuple() == self.app)
        assert x, result_from_tuple(x == self.app)

    def test_with_parent(self):
        x = self.app.AsyncResult(uuid())
        x.parent = self.app.AsyncResult(uuid())
        y = result_from_tuple(x.as_tuple(), self.app)
        assert y == x
        assert y.parent == x.parent
        assert isinstance(y.parent, AsyncResult)

    def test_compat(self):
        uid = uuid()
        x = result_from_tuple([uid, []], app=self.app)
        assert x.id == uid

    def test_GroupResult(self):
        x = self.app.GroupResult(
            uuid(), [self.app.AsyncResult(uuid()) for _ in range(10)],
        )
        assert x, result_from_tuple(x.as_tuple() == self.app)
        assert x, result_from_tuple(x == self.app)

    def test_GroupResult_with_parent(self):
        parent = self.app.AsyncResult(uuid())
        result = self.app.GroupResult(
            uuid(), [self.app.AsyncResult(uuid()) for _ in range(10)],
            parent
        )
        second_result = result_from_tuple(result.as_tuple(), self.app)
        assert second_result == result
        assert second_result.parent == parent

    def test_GroupResult_as_tuple(self):
        parent = self.app.AsyncResult(uuid())
        result = self.app.GroupResult(
            'group-result-1',
            [self.app.AsyncResult('async-result-{}'.format(i))
             for i in range(2)],
            parent
        )
        (result_id, parent_tuple), group_results = result.as_tuple()
        assert result_id == result.id
        assert parent_tuple == parent.as_tuple()
        assert parent_tuple[0][0] == parent.id
        assert isinstance(group_results, list)
        expected_grp_res = [(('async-result-{}'.format(i), None), None)
                            for i in range(2)]
        assert group_results == expected_grp_res
