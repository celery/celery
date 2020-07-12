import pickle
from contextlib import contextmanager
from datetime import timedelta
from pickle import dumps, loads
from queue import Empty, Queue

import pytest
from billiard.einfo import ExceptionInfo
from case import Mock, mock

from celery import states, uuid
from celery.app.task import Context
from celery.backends.amqp import AMQPBackend
from celery.result import AsyncResult


class SomeClass:

    def __init__(self, data):
        self.data = data


class test_AMQPBackend:

    def setup(self):
        self.app.conf.result_cache_max = 100

    def create_backend(self, **opts):
        opts = dict({'serializer': 'pickle', 'persistent': True}, **opts)
        return AMQPBackend(self.app, **opts)

    def test_destination_for(self):
        b = self.create_backend()
        request = Mock()
        assert b.destination_for('id', request) == (
            b.rkey('id'), request.correlation_id,
        )

    def test_store_result__no_routing_key(self):
        b = self.create_backend()
        b.destination_for = Mock()
        b.destination_for.return_value = None, None
        b.store_result('id', None, states.SUCCESS)

    def test_mark_as_done(self):
        tb1 = self.create_backend(max_cached_results=1)
        tb2 = self.create_backend(max_cached_results=1)

        tid = uuid()

        tb1.mark_as_done(tid, 42)
        assert tb2.get_state(tid) == states.SUCCESS
        assert tb2.get_result(tid) == 42
        assert tb2._cache.get(tid)
        assert tb2.get_result(tid), 42

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_pickleable(self):
        assert loads(dumps(self.create_backend()))

    def test_revive(self):
        tb = self.create_backend()
        tb.revive(None)

    def test_is_pickled(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid2 = uuid()
        result = {'foo': 'baz', 'bar': SomeClass(12345)}
        tb1.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb2.get_result(tid2)
        assert rindb.get('foo') == 'baz'
        assert rindb.get('bar').data == 12345

    def test_mark_as_failure(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid3 = uuid()
        try:
            raise KeyError('foo')
        except KeyError as exception:
            einfo = ExceptionInfo()
            tb1.mark_as_failure(tid3, exception, traceback=einfo.traceback)
            assert tb2.get_state(tid3) == states.FAILURE
            assert isinstance(tb2.get_result(tid3), KeyError)
            assert tb2.get_traceback(tid3) == einfo.traceback

    def test_repair_uuid(self):
        from celery.backends.amqp import repair_uuid
        for i in range(10):
            tid = uuid()
            assert repair_uuid(tid.replace('-', '')) == tid

    def test_expires_is_int(self):
        b = self.create_backend(expires=48)
        q = b._create_binding('x1y2z3')
        assert q.expires == 48

    def test_expires_is_float(self):
        b = self.create_backend(expires=48.3)
        q = b._create_binding('x1y2z3')
        assert q.expires == 48.3

    def test_expires_is_timedelta(self):
        b = self.create_backend(expires=timedelta(minutes=1))
        q = b._create_binding('x1y2z3')
        assert q.expires == 60

    @mock.sleepdeprived()
    def test_store_result_retries(self):
        iterations = [0]
        stop_raising_at = [5]

        def publish(*args, **kwargs):
            if iterations[0] > stop_raising_at[0]:
                return
            iterations[0] += 1
            raise KeyError('foo')

        backend = AMQPBackend(self.app)
        from celery.app.amqp import Producer
        prod, Producer.publish = Producer.publish, publish
        try:
            with pytest.raises(KeyError):
                backend.retry_policy['max_retries'] = None
                backend.store_result('foo', 'bar', 'STARTED')

            with pytest.raises(KeyError):
                backend.retry_policy['max_retries'] = 10
                backend.store_result('foo', 'bar', 'STARTED')
        finally:
            Producer.publish = prod

    def test_poll_no_messages(self):
        b = self.create_backend()
        assert b.get_task_meta(uuid())['status'] == states.PENDING

    @contextmanager
    def _result_context(self):
        results = Queue()

        class Message:
            acked = 0
            requeued = 0

            def __init__(self, **merge):
                self.payload = dict({'status': states.STARTED,
                                     'result': None}, **merge)
                self.properties = {'correlation_id': merge.get('task_id')}
                self.body = pickle.dumps(self.payload)
                self.content_type = 'application/x-python-serialize'
                self.content_encoding = 'binary'

            def ack(self, *args, **kwargs):
                self.acked += 1

            def requeue(self, *args, **kwargs):
                self.requeued += 1

        class MockBinding:

            def __init__(self, *args, **kwargs):
                self.channel = Mock()

            def __call__(self, *args, **kwargs):
                return self

            def declare(self):
                pass

            def get(self, no_ack=False, accept=None):
                try:
                    m = results.get(block=False)
                    if m:
                        m.accept = accept
                    return m
                except Empty:
                    pass

            def is_bound(self):
                return True

        class MockBackend(AMQPBackend):
            Queue = MockBinding

        backend = MockBackend(self.app, max_cached_results=100)
        backend._republish = Mock()

        yield results, backend, Message

    def test_backlog_limit_exceeded(self):
        with self._result_context() as (results, backend, Message):
            for i in range(1001):
                results.put(Message(task_id='id', status=states.RECEIVED))
            with pytest.raises(backend.BacklogLimitExceeded):
                backend.get_task_meta('id')

    def test_poll_result(self):
        with self._result_context() as (results, backend, Message):
            tid = uuid()
            # FFWD's to the latest state.
            state_messages = [
                Message(task_id=tid, status=states.RECEIVED, seq=1),
                Message(task_id=tid, status=states.STARTED, seq=2),
                Message(task_id=tid, status=states.FAILURE, seq=3),
            ]
            for state_message in state_messages:
                results.put(state_message)
            r1 = backend.get_task_meta(tid)
            # FFWDs to the last state.
            assert r1['status'] == states.FAILURE
            assert r1['seq'] == 3

            # Caches last known state.
            tid = uuid()
            results.put(Message(task_id=tid))
            backend.get_task_meta(tid)
            assert tid, backend._cache in 'Caches last known state'

            assert state_messages[-1].requeued

            # Returns cache if no new states.
            results.queue.clear()
            assert not results.qsize()
            backend._cache[tid] = 'hello'
            # returns cache if no new states.
            assert backend.get_task_meta(tid) == 'hello'

    def test_drain_events_decodes_exceptions_in_meta(self):
        tid = uuid()
        b = self.create_backend(serializer='json')
        b.store_result(tid, RuntimeError('aap'), states.FAILURE)
        result = AsyncResult(tid, backend=b)

        with pytest.raises(Exception) as excinfo:
            result.get()

        assert excinfo.value.__class__.__name__ == 'RuntimeError'
        assert str(excinfo.value) == 'aap'

    def test_no_expires(self):
        b = self.create_backend(expires=None)
        app = self.app
        app.conf.result_expires = None
        b = self.create_backend(expires=None)
        q = b._create_binding('foo')
        assert q.expires is None

    def test_process_cleanup(self):
        self.create_backend().process_cleanup()

    def test_reload_task_result(self):
        with pytest.raises(NotImplementedError):
            self.create_backend().reload_task_result('x')

    def test_reload_group_result(self):
        with pytest.raises(NotImplementedError):
            self.create_backend().reload_group_result('x')

    def test_save_group(self):
        with pytest.raises(NotImplementedError):
            self.create_backend().save_group('x', 'x')

    def test_restore_group(self):
        with pytest.raises(NotImplementedError):
            self.create_backend().restore_group('x')

    def test_delete_group(self):
        with pytest.raises(NotImplementedError):
            self.create_backend().delete_group('x')


class test_AMQPBackend_result_extended:
    def setup(self):
        self.app.conf.result_extended = True

    def test_store_result(self):
        b = AMQPBackend(self.app)
        tid = uuid()

        request = Context(args=(1, 2, 3), kwargs={'foo': 'bar'},
                          task_name='mytask', retries=2,
                          hostname='celery@worker_1',
                          delivery_info={'routing_key': 'celery'})

        b.store_result(tid, {'fizz': 'buzz'}, states.SUCCESS, request=request)

        meta = b.get_task_meta(tid)
        assert meta == {
            'args': [1, 2, 3],
            'children': [],
            'kwargs': {'foo': 'bar'},
            'name': 'mytask',
            'queue': 'celery',
            'result': {'fizz': 'buzz'},
            'retries': 2,
            'status': 'SUCCESS',
            'task_id': tid,
            'traceback': None,
            'worker': 'celery@worker_1',
        }
