from __future__ import absolute_import
from __future__ import with_statement

import pickle
import socket

from datetime import timedelta
from Queue import Empty, Queue

from celery import current_app
from celery import states
from celery.app import app_or_default
from celery.backends.amqp import AMQPBackend
from celery.datastructures import ExceptionInfo
from celery.exceptions import TimeoutError
from celery.utils import uuid

from celery.tests.utils import AppCase, sleepdeprived, Mock


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class test_AMQPBackend(AppCase):

    def create_backend(self, **opts):
        opts = dict(dict(serializer='pickle', persistent=False), **opts)
        return AMQPBackend(**opts)

    def test_mark_as_done(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid = uuid()

        tb1.mark_as_done(tid, 42)
        self.assertEqual(tb2.get_status(tid), states.SUCCESS)
        self.assertEqual(tb2.get_result(tid), 42)
        self.assertTrue(tb2._cache.get(tid))
        self.assertTrue(tb2.get_result(tid), 42)

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
        self.assertEqual(rindb.get('foo'), 'baz')
        self.assertEqual(rindb.get('bar').data, 12345)

    def test_mark_as_failure(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid3 = uuid()
        try:
            raise KeyError('foo')
        except KeyError, exception:
            einfo = ExceptionInfo()
            tb1.mark_as_failure(tid3, exception, traceback=einfo.traceback)
            self.assertEqual(tb2.get_status(tid3), states.FAILURE)
            self.assertIsInstance(tb2.get_result(tid3), KeyError)
            self.assertEqual(tb2.get_traceback(tid3), einfo.traceback)

    def test_repair_uuid(self):
        from celery.backends.amqp import repair_uuid
        for i in range(10):
            tid = uuid()
            self.assertEqual(repair_uuid(tid.replace('-', '')), tid)

    def test_expires_defaults_to_config_deprecated_setting(self):
        app = app_or_default()
        prev = app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES
        app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = 10
        try:
            b = self.create_backend()
            self.assertEqual(b.queue_arguments.get('x-expires'), 10 * 1000.0)
        finally:
            app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = prev

    def test_expires_is_int(self):
        b = self.create_backend(expires=48)
        self.assertEqual(b.queue_arguments.get('x-expires'), 48 * 1000.0)

    def test_expires_is_float(self):
        b = self.create_backend(expires=48.3)
        self.assertEqual(b.queue_arguments.get('x-expires'), 48.3 * 1000.0)

    def test_expires_is_timedelta(self):
        b = self.create_backend(expires=timedelta(minutes=1))
        self.assertEqual(b.queue_arguments.get('x-expires'), 60 * 1000.0)

    @sleepdeprived()
    def test_store_result_retries(self):
        iterations = [0]
        stop_raising_at = [5]

        def publish(*args, **kwargs):
            if iterations[0] > stop_raising_at[0]:
                return
            iterations[0] += 1
            raise KeyError('foo')

        backend = AMQPBackend()
        from celery.app.amqp import TaskProducer
        prod, TaskProducer.publish = TaskProducer.publish, publish
        try:
            with self.assertRaises(KeyError):
                backend.retry_policy['max_retries'] = None
                backend.store_result('foo', 'bar', 'STARTED')

            with self.assertRaises(KeyError):
                backend.retry_policy['max_retries'] = 10
                backend.store_result('foo', 'bar', 'STARTED')
        finally:
            TaskProducer.publish = prod

    def assertState(self, retval, state):
        self.assertEqual(retval['status'], state)

    def test_poll_no_messages(self):
        b = self.create_backend()
        self.assertState(b.get_task_meta(uuid()), states.PENDING)

    def test_poll_result(self):

        results = Queue()

        class Message(object):
            acked = 0
            requeued = 0

            def __init__(self, **merge):
                self.payload = dict({'status': states.STARTED,
                                     'result': None}, **merge)
                self.body = pickle.dumps(self.payload)
                self.content_type = 'application/x-python-serialize'
                self.content_encoding = 'binary'

            def ack(self, *args, **kwargs):
                self.acked += 1

            def requeue(self, *args, **kwargs):
                self.requeued += 1

        class MockBinding(object):

            def __init__(self, *args, **kwargs):
                self.channel = Mock()

            def __call__(self, *args, **kwargs):
                return self

            def declare(self):
                pass

            def get(self, no_ack=False):
                try:
                    return results.get(block=False)
                except Empty:
                    pass

            def is_bound(self):
                return True

        class MockBackend(AMQPBackend):
            Queue = MockBinding

        backend = MockBackend()
        backend._republish = Mock()

        # FFWD's to the latest state.
        state_messages = [
            Message(status=states.RECEIVED, seq=1),
            Message(status=states.STARTED, seq=2),
            Message(status=states.FAILURE, seq=3),
        ]
        for state_message in state_messages:
            results.put(state_message)
        r1 = backend.get_task_meta(uuid())
        self.assertDictContainsSubset({'status': states.FAILURE,
                                       'seq': 3}, r1,
                                      'FFWDs to the last state')

        # Caches last known state.
        results.put(Message())
        tid = uuid()
        backend.get_task_meta(tid)
        self.assertIn(tid, backend._cache, 'Caches last known state')

        self.assertTrue(state_messages[-1].requeued)

        # Returns cache if no new states.
        results.queue.clear()
        assert not results.qsize()
        backend._cache[tid] = 'hello'
        self.assertEqual(backend.get_task_meta(tid), 'hello',
                         'Returns cache if no new states')

    def test_wait_for(self):
        b = self.create_backend()

        tid = uuid()
        with self.assertRaises(TimeoutError):
            b.wait_for(tid, timeout=0.1)
        b.store_result(tid, None, states.STARTED)
        with self.assertRaises(TimeoutError):
            b.wait_for(tid, timeout=0.1)
        b.store_result(tid, None, states.RETRY)
        with self.assertRaises(TimeoutError):
            b.wait_for(tid, timeout=0.1)
        b.store_result(tid, 42, states.SUCCESS)
        self.assertEqual(b.wait_for(tid, timeout=1), 42)
        b.store_result(tid, 56, states.SUCCESS)
        self.assertEqual(b.wait_for(tid, timeout=1), 42,
                         'result is cached')
        self.assertEqual(b.wait_for(tid, timeout=1, cache=False), 56)
        b.store_result(tid, KeyError('foo'), states.FAILURE)
        with self.assertRaises(KeyError):
            b.wait_for(tid, timeout=1, cache=False)

    def test_drain_events_remaining_timeouts(self):

        class Connection(object):

            def drain_events(self, timeout=None):
                pass

        b = self.create_backend()
        with current_app.pool.acquire_channel(block=False) as (_, channel):
            binding = b._create_binding(uuid())
            consumer = b.Consumer(channel, binding, no_ack=True)
            with self.assertRaises(socket.timeout):
                b.drain_events(Connection(), consumer, timeout=0.1)

    def test_get_many(self):
        b = self.create_backend()

        tids = []
        for i in xrange(10):
            tid = uuid()
            b.store_result(tid, i, states.SUCCESS)
            tids.append(tid)

        res = list(b.get_many(tids, timeout=1))
        expected_results = [(tid, {'status': states.SUCCESS,
                                   'result': i,
                                   'traceback': None,
                                   'task_id': tid,
                                   'children': None})
                            for i, tid in enumerate(tids)]
        self.assertEqual(sorted(res), sorted(expected_results))
        self.assertDictEqual(b._cache[res[0][0]], res[0][1])
        cached_res = list(b.get_many(tids, timeout=1))
        self.assertEqual(sorted(cached_res), sorted(expected_results))
        b._cache[res[0][0]]['status'] = states.RETRY
        with self.assertRaises(socket.timeout):
            list(b.get_many(tids, timeout=0.01))

    def test_test_get_many_raises_outer_block(self):

        class Backend(AMQPBackend):

            def Consumer(*args, **kwargs):
                raise KeyError('foo')

        b = Backend()
        with self.assertRaises(KeyError):
            b.get_many(['id1']).next()

    def test_test_get_many_raises_inner_block(self):

        class Backend(AMQPBackend):

            def drain_events(self, *args, **kwargs):
                raise KeyError('foo')

        b = Backend()
        with self.assertRaises(KeyError):
            b.get_many(['id1']).next()

    def test_no_expires(self):
        b = self.create_backend(expires=None)
        app = app_or_default()
        prev = app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES
        app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = None
        try:
            b = self.create_backend(expires=None)
            with self.assertRaises(KeyError):
                b.queue_arguments['x-expires']
        finally:
            app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = prev

    def test_process_cleanup(self):
        self.create_backend().process_cleanup()

    def test_reload_task_result(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().reload_task_result('x')

    def test_reload_group_result(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().reload_group_result('x')

    def test_save_group(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().save_group('x', 'x')

    def test_restore_group(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().restore_group('x')

    def test_delete_group(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().delete_group('x')
