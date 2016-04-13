from __future__ import absolute_import, unicode_literals

import sys
import types

from contextlib import contextmanager

from celery.exceptions import ChordError, TimeoutError
from celery.five import items, bytes_if_py2, range
from celery.utils import serialization
from celery.utils.serialization import subclass_exception
from celery.utils.serialization import find_pickleable_exception as fnpe
from celery.utils.serialization import UnpickleableExceptionWrapper
from celery.utils.serialization import get_pickleable_exception as gpe

from celery import states
from celery import group
from celery.backends.base import (
    BaseBackend,
    KeyValueStoreBackend,
    DisabledBackend,
    _nulldict,
)
from celery.result import result_from_tuple
from celery.utils import uuid
from celery.utils.functional import pass1

from celery.tests.case import ANY, AppCase, Case, Mock, call, patch, skip


class wrapobject(object):

    def __init__(self, *args, **kwargs):
        self.args = args

if sys.version_info[0] == 3 or getattr(sys, 'pypy_version_info', None):
    Oldstyle = None
else:
    Oldstyle = types.ClassType(bytes_if_py2('Oldstyle'), (), {})
Unpickleable = subclass_exception(
    bytes_if_py2('Unpickleable'), KeyError, 'foo.module',
)
Impossible = subclass_exception(
    bytes_if_py2('Impossible'), object, 'foo.module',
)
Lookalike = subclass_exception(
    bytes_if_py2('Lookalike'), wrapobject, 'foo.module',
)


class test_nulldict(Case):

    def test_nulldict(self):
        x = _nulldict()
        x['foo'] = 1
        x.update(foo=1, bar=2)
        x.setdefault('foo', 3)


class test_serialization(AppCase):

    def test_create_exception_cls(self):
        self.assertTrue(serialization.create_exception_cls('FooError', 'm'))
        self.assertTrue(serialization.create_exception_cls('FooError', 'm',
                                                           KeyError))


class test_BaseBackend_interface(AppCase):

    def setup(self):
        self.b = BaseBackend(self.app)

    def test__forget(self):
        with self.assertRaises(NotImplementedError):
            self.b._forget('SOMExx-N0Nex1stant-IDxx-')

    def test_forget(self):
        with self.assertRaises(NotImplementedError):
            self.b.forget('SOMExx-N0nex1stant-IDxx-')

    def test_on_chord_part_return(self):
        self.b.on_chord_part_return(None, None, None)

    def test_apply_chord(self, unlock='celery.chord_unlock'):
        self.app.tasks[unlock] = Mock()
        self.b.apply_chord(
            group(app=self.app), (), 'dakj221', None,
            result=[self.app.AsyncResult(x) for x in [1, 2, 3]],
        )
        self.assertTrue(self.app.tasks[unlock].apply_async.call_count)


class test_exception_pickle(AppCase):

    @skip.if_python3(reason='does not support old style classes')
    @skip.if_pypy()
    def test_oldstyle(self):
        self.assertTrue(fnpe(Oldstyle()))

    def test_BaseException(self):
        self.assertIsNone(fnpe(Exception()))

    def test_get_pickleable_exception(self):
        exc = Exception('foo')
        self.assertEqual(gpe(exc), exc)

    def test_unpickleable(self):
        self.assertIsInstance(fnpe(Unpickleable()), KeyError)
        self.assertIsNone(fnpe(Impossible()))


class test_prepare_exception(AppCase):

    def setup(self):
        self.b = BaseBackend(self.app)

    def test_unpickleable(self):
        self.b.serializer = 'pickle'
        x = self.b.prepare_exception(Unpickleable(1, 2, 'foo'))
        self.assertIsInstance(x, KeyError)
        y = self.b.exception_to_python(x)
        self.assertIsInstance(y, KeyError)

    def test_impossible(self):
        self.b.serializer = 'pickle'
        x = self.b.prepare_exception(Impossible())
        self.assertIsInstance(x, UnpickleableExceptionWrapper)
        self.assertTrue(str(x))
        y = self.b.exception_to_python(x)
        self.assertEqual(y.__class__.__name__, 'Impossible')
        if sys.version_info < (2, 5):
            self.assertTrue(y.__class__.__module__)
        else:
            self.assertEqual(y.__class__.__module__, 'foo.module')

    def test_regular(self):
        self.b.serializer = 'pickle'
        x = self.b.prepare_exception(KeyError('baz'))
        self.assertIsInstance(x, KeyError)
        y = self.b.exception_to_python(x)
        self.assertIsInstance(y, KeyError)


class KVBackend(KeyValueStoreBackend):
    mget_returns_dict = False

    def __init__(self, app, *args, **kwargs):
        self.db = {}
        super(KVBackend, self).__init__(app)

    def get(self, key):
        return self.db.get(key)

    def set(self, key, value):
        self.db[key] = value

    def mget(self, keys):
        if self.mget_returns_dict:
            return {key: self.get(key) for key in keys}
        else:
            return [self.get(k) for k in keys]

    def delete(self, key):
        self.db.pop(key, None)


class DictBackend(BaseBackend):

    def __init__(self, *args, **kwargs):
        BaseBackend.__init__(self, *args, **kwargs)
        self._data = {'can-delete': {'result': 'foo'}}

    def _restore_group(self, group_id):
        if group_id == 'exists':
            return {'result': 'group'}

    def _get_task_meta_for(self, task_id):
        if task_id == 'task-exists':
            return {'result': 'task'}

    def _delete_group(self, group_id):
        self._data.pop(group_id, None)


class test_BaseBackend_dict(AppCase):

    def setup(self):
        self.b = DictBackend(app=self.app)

    def test_delete_group(self):
        self.b.delete_group('can-delete')
        self.assertNotIn('can-delete', self.b._data)

    def test_prepare_exception_json(self):
        x = DictBackend(self.app, serializer='json')
        e = x.prepare_exception(KeyError('foo'))
        self.assertIn('exc_type', e)
        e = x.exception_to_python(e)
        self.assertEqual(e.__class__.__name__, 'KeyError')
        self.assertEqual(str(e).strip('u'), "'foo'")

    def test_save_group(self):
        b = BaseBackend(self.app)
        b._save_group = Mock()
        b.save_group('foofoo', 'xxx')
        b._save_group.assert_called_with('foofoo', 'xxx')

    def test_add_to_chord_interface(self):
        b = BaseBackend(self.app)
        with self.assertRaises(NotImplementedError):
            b.add_to_chord('group_id', 'sig')

    def test_forget_interface(self):
        b = BaseBackend(self.app)
        with self.assertRaises(NotImplementedError):
            b.forget('foo')

    def test_restore_group(self):
        self.assertIsNone(self.b.restore_group('missing'))
        self.assertIsNone(self.b.restore_group('missing'))
        self.assertEqual(self.b.restore_group('exists'), 'group')
        self.assertEqual(self.b.restore_group('exists'), 'group')
        self.assertEqual(self.b.restore_group('exists', cache=False), 'group')

    def test_reload_group_result(self):
        self.b._cache = {}
        self.b.reload_group_result('exists')
        self.b._cache['exists'] = {'result': 'group'}

    def test_reload_task_result(self):
        self.b._cache = {}
        self.b.reload_task_result('task-exists')
        self.b._cache['task-exists'] = {'result': 'task'}

    def test_fail_from_current_stack(self):
        self.b.mark_as_failure = Mock()
        try:
            raise KeyError('foo')
        except KeyError as exc:
            self.b.fail_from_current_stack('task_id')
            self.b.mark_as_failure.assert_called()
            args = self.b.mark_as_failure.call_args[0]
            self.assertEqual(args[0], 'task_id')
            self.assertIs(args[1], exc)
            self.assertTrue(args[2])

    def test_prepare_value_serializes_group_result(self):
        self.b.serializer = 'json'
        g = self.app.GroupResult('group_id', [self.app.AsyncResult('foo')])
        v = self.b.prepare_value(g)
        self.assertIsInstance(v, (list, tuple))
        self.assertEqual(result_from_tuple(v, app=self.app), g)

        v2 = self.b.prepare_value(g[0])
        self.assertIsInstance(v2, (list, tuple))
        self.assertEqual(result_from_tuple(v2, app=self.app), g[0])

        self.b.serializer = 'pickle'
        self.assertIsInstance(self.b.prepare_value(g), self.app.GroupResult)

    def test_is_cached(self):
        b = BaseBackend(app=self.app, max_cached_results=1)
        b._cache['foo'] = 1
        self.assertTrue(b.is_cached('foo'))
        self.assertFalse(b.is_cached('false'))

    def test_mark_as_done__chord(self):
        b = BaseBackend(app=self.app)
        b._store_result = Mock()
        request = Mock(name='request')
        b.on_chord_part_return = Mock()
        b.mark_as_done('id', 10, request=request)
        b.on_chord_part_return.assert_called_with(request, states.SUCCESS, 10)

    def test_mark_as_failure__chord(self):
        b = BaseBackend(app=self.app)
        b._store_result = Mock()
        request = Mock(name='request')
        request.errbacks = []
        b.on_chord_part_return = Mock()
        exc = KeyError()
        b.mark_as_failure('id', exc, request=request)
        b.on_chord_part_return.assert_called_with(request, states.FAILURE, exc)

    def test_mark_as_revoked__chord(self):
        b = BaseBackend(app=self.app)
        b._store_result = Mock()
        request = Mock(name='request')
        request.errbacks = []
        b.on_chord_part_return = Mock()
        b.mark_as_revoked('id', 'revoked', request=request)
        b.on_chord_part_return.assert_called_with(request, states.REVOKED, ANY)

    def test_chord_error_from_stack_raises(self):
        b = BaseBackend(app=self.app)
        exc = KeyError()
        callback = Mock(name='callback')
        callback.options = {'link_error': []}
        task = self.app.tasks[callback.task] = Mock()
        b.fail_from_current_stack = Mock()
        group = self.patch('celery.group')
        group.side_effect = exc
        b.chord_error_from_stack(callback, exc=ValueError())
        task.backend.fail_from_current_stack.assert_called_with(
            callback.id, exc=exc)

    def test_exception_to_python_when_None(self):
        b = BaseBackend(app=self.app)
        self.assertIsNone(b.exception_to_python(None))

    def test_wait_for__on_interval(self):
        self.patch('time.sleep')
        b = BaseBackend(app=self.app)
        b._get_task_meta_for = Mock()
        b._get_task_meta_for.return_value = {'status': states.PENDING}
        callback = Mock(name='callback')
        with self.assertRaises(TimeoutError):
            b.wait_for(task_id='1', on_interval=callback, timeout=1)
        callback.assert_called_with()

        b._get_task_meta_for.return_value = {'status': states.SUCCESS}
        b.wait_for(task_id='1', timeout=None)

    def test_get_children(self):
        b = BaseBackend(app=self.app)
        b._get_task_meta_for = Mock()
        b._get_task_meta_for.return_value = {}
        self.assertIsNone(b.get_children('id'))
        b._get_task_meta_for.return_value = {'children': 3}
        self.assertEqual(b.get_children('id'), 3)


class test_KeyValueStoreBackend(AppCase):

    def setup(self):
        self.b = KVBackend(app=self.app)

    def test_on_chord_part_return(self):
        assert not self.b.implements_incr
        self.b.on_chord_part_return(None, None, None)

    def test_get_store_delete_result(self):
        tid = uuid()
        self.b.mark_as_done(tid, 'Hello world')
        self.assertEqual(self.b.get_result(tid), 'Hello world')
        self.assertEqual(self.b.get_state(tid), states.SUCCESS)
        self.b.forget(tid)
        self.assertEqual(self.b.get_state(tid), states.PENDING)

    def test_strip_prefix(self):
        x = self.b.get_key_for_task('x1b34')
        self.assertEqual(self.b._strip_prefix(x), 'x1b34')
        self.assertEqual(self.b._strip_prefix('x1b34'), 'x1b34')

    def test_get_many(self):
        for is_dict in True, False:
            self.b.mget_returns_dict = is_dict
            ids = {uuid(): i for i in range(10)}
            for id, i in items(ids):
                self.b.mark_as_done(id, i)
            it = self.b.get_many(list(ids))
            for i, (got_id, got_state) in enumerate(it):
                self.assertEqual(got_state['result'], ids[got_id])
            self.assertEqual(i, 9)
            self.assertTrue(list(self.b.get_many(list(ids))))

            self.b._cache.clear()
            callback = Mock(name='callback')
            it = self.b.get_many(list(ids), on_message=callback)
            for i, (got_id, got_state) in enumerate(it):
                self.assertEqual(got_state['result'], ids[got_id])
            self.assertEqual(i, 9)
            self.assertTrue(list(self.b.get_many(list(ids))))
            callback.assert_has_calls([
                call(ANY) for id in ids
            ])

    def test_get_many_times_out(self):
        tasks = [uuid() for _ in range(4)]
        self.b._cache[tasks[1]] = {'status': 'PENDING'}
        with self.assertRaises(self.b.TimeoutError):
            list(self.b.get_many(tasks, timeout=0.01, interval=0.01))

    def test_chord_part_return_no_gid(self):
        self.b.implements_incr = True
        task = Mock()
        state = 'SUCCESS'
        result = 10
        task.request.group = None
        self.b.get_key_for_chord = Mock()
        self.b.get_key_for_chord.side_effect = AssertionError(
            'should not get here',
        )
        self.assertIsNone(
            self.b.on_chord_part_return(task.request, state, result),
        )

    @patch('celery.backends.base.GroupResult')
    @patch('celery.backends.base.maybe_signature')
    def test_chord_part_return_restore_raises(self, maybe_signature,
                                              GroupResult):
        self.b.implements_incr = True
        GroupResult.restore.side_effect = KeyError()
        self.b.chord_error_from_stack = Mock()
        callback = Mock(name='callback')
        request = Mock(name='request')
        request.group = 'gid'
        maybe_signature.return_value = callback
        self.b.on_chord_part_return(request, states.SUCCESS, 10)
        self.b.chord_error_from_stack.assert_called_with(
            callback, ANY,
        )

    @patch('celery.backends.base.GroupResult')
    @patch('celery.backends.base.maybe_signature')
    def test_chord_part_return_restore_empty(self, maybe_signature,
                                             GroupResult):
        self.b.implements_incr = True
        GroupResult.restore.return_value = None
        self.b.chord_error_from_stack = Mock()
        callback = Mock(name='callback')
        request = Mock(name='request')
        request.group = 'gid'
        maybe_signature.return_value = callback
        self.b.on_chord_part_return(request, states.SUCCESS, 10)
        self.b.chord_error_from_stack.assert_called_with(
            callback, ANY,
        )

    def test_filter_ready(self):
        self.b.decode_result = Mock()
        self.b.decode_result.side_effect = pass1
        self.assertEqual(
            len(list(self.b._filter_ready([
                (1, {'status': states.RETRY}),
                (2, {'status': states.FAILURE}),
                (3, {'status': states.SUCCESS}),
            ]))),
            2,
        )

    @contextmanager
    def _chord_part_context(self, b):

        @self.app.task(shared=False)
        def callback(result):
            pass

        b.implements_incr = True
        b.client = Mock()
        with patch('celery.backends.base.GroupResult') as GR:
            deps = GR.restore.return_value = Mock(name='DEPS')
            deps.__len__ = Mock()
            deps.__len__.return_value = 10
            b.incr = Mock()
            b.incr.return_value = 10
            b.expire = Mock()
            task = Mock()
            task.request.group = 'grid'
            cb = task.request.chord = callback.s()
            task.request.chord.freeze()
            callback.backend = b
            callback.backend.fail_from_current_stack = Mock()
            yield task, deps, cb

    def test_chord_part_return_propagate_set(self):
        with self._chord_part_context(self.b) as (task, deps, _):
            self.b.on_chord_part_return(task.request, 'SUCCESS', 10)
            self.b.expire.assert_not_called()
            deps.delete.assert_called_with()
            deps.join_native.assert_called_with(propagate=True, timeout=3.0)

    def test_chord_part_return_propagate_default(self):
        with self._chord_part_context(self.b) as (task, deps, _):
            self.b.on_chord_part_return(task.request, 'SUCCESS', 10)
            self.b.expire.assert_not_called()
            deps.delete.assert_called_with()
            deps.join_native.assert_called_with(propagate=True, timeout=3.0)

    def test_chord_part_return_join_raises_internal(self):
        with self._chord_part_context(self.b) as (task, deps, callback):
            deps._failed_join_report = lambda: iter([])
            deps.join_native.side_effect = KeyError('foo')
            self.b.on_chord_part_return(task.request, 'SUCCESS', 10)
            self.b.fail_from_current_stack.assert_called()
            args = self.b.fail_from_current_stack.call_args
            exc = args[1]['exc']
            self.assertIsInstance(exc, ChordError)
            self.assertIn('foo', str(exc))

    def test_chord_part_return_join_raises_task(self):
        b = KVBackend(serializer='pickle', app=self.app)
        with self._chord_part_context(b) as (task, deps, callback):
            deps._failed_join_report = lambda: iter([
                self.app.AsyncResult('culprit'),
            ])
            deps.join_native.side_effect = KeyError('foo')
            b.on_chord_part_return(task.request, 'SUCCESS', 10)
            b.fail_from_current_stack.assert_called()
            args = b.fail_from_current_stack.call_args
            exc = args[1]['exc']
            self.assertIsInstance(exc, ChordError)
            self.assertIn('Dependency culprit raised', str(exc))

    def test_restore_group_from_json(self):
        b = KVBackend(serializer='json', app=self.app)
        g = self.app.GroupResult(
            'group_id',
            [self.app.AsyncResult('a'), self.app.AsyncResult('b')],
        )
        b._save_group(g.id, g)
        g2 = b._restore_group(g.id)['result']
        self.assertEqual(g2, g)

    def test_restore_group_from_pickle(self):
        b = KVBackend(serializer='pickle', app=self.app)
        g = self.app.GroupResult(
            'group_id',
            [self.app.AsyncResult('a'), self.app.AsyncResult('b')],
        )
        b._save_group(g.id, g)
        g2 = b._restore_group(g.id)['result']
        self.assertEqual(g2, g)

    def test_chord_apply_fallback(self):
        self.b.implements_incr = False
        self.b.fallback_chord_unlock = Mock()
        self.b.apply_chord(
            group(app=self.app), (), 'group_id', 'body',
            result='result', foo=1,
        )
        self.b.fallback_chord_unlock.assert_called_with(
            'group_id', 'body', result='result', foo=1,
        )

    def test_get_missing_meta(self):
        self.assertIsNone(self.b.get_result('xxx-missing'))
        self.assertEqual(self.b.get_state('xxx-missing'), states.PENDING)

    def test_save_restore_delete_group(self):
        tid = uuid()
        tsr = self.app.GroupResult(
            tid, [self.app.AsyncResult(uuid()) for _ in range(10)],
        )
        self.b.save_group(tid, tsr)
        self.b.restore_group(tid)
        self.assertEqual(self.b.restore_group(tid), tsr)
        self.b.delete_group(tid)
        self.assertIsNone(self.b.restore_group(tid))

    def test_restore_missing_group(self):
        self.assertIsNone(self.b.restore_group('xxx-nonexistant'))


class test_KeyValueStoreBackend_interface(AppCase):

    def test_get(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend(self.app).get('a')

    def test_set(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend(self.app).set('a', 1)

    def test_incr(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend(self.app).incr('a')

    def test_cleanup(self):
        self.assertFalse(KeyValueStoreBackend(self.app).cleanup())

    def test_delete(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend(self.app).delete('a')

    def test_mget(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend(self.app).mget(['a'])

    def test_forget(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend(self.app).forget('a')


class test_DisabledBackend(AppCase):

    def test_store_result(self):
        DisabledBackend(self.app).store_result()

    def test_is_disabled(self):
        with self.assertRaises(NotImplementedError):
            DisabledBackend(self.app).get_state('foo')

    def test_as_uri(self):
        self.assertEqual(DisabledBackend(self.app).as_uri(), 'disabled://')


class test_as_uri(AppCase):

    def setup(self):
        self.b = BaseBackend(
            app=self.app,
            url='sch://uuuu:pwpw@hostname.dom'
        )

    def test_as_uri_include_password(self):
        self.assertEqual(self.b.as_uri(True), self.b.url)

    def test_as_uri_exclude_password(self):
        self.assertEqual(self.b.as_uri(), 'sch://uuuu:**@hostname.dom/')
