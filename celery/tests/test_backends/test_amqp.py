from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from datetime import timedelta
from Queue import Empty, Queue

from celery import current_app
from celery import states
from celery.app import app_or_default
from celery.backends.amqp import AMQPBackend
from celery.datastructures import ExceptionInfo
from celery.exceptions import TimeoutError
from celery.utils import uuid

from celery.tests.utils import unittest
from celery.tests.utils import sleepdeprived


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class test_AMQPBackend(unittest.TestCase):

    def create_backend(self, **opts):
        opts = dict(dict(serializer="pickle", persistent=False), **opts)
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

    def test_is_pickled(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid2 = uuid()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb1.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb2.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid3 = uuid()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            einfo = ExceptionInfo(sys.exc_info())
            tb1.mark_as_failure(tid3, exception, traceback=einfo.traceback)
            self.assertEqual(tb2.get_status(tid3), states.FAILURE)
            self.assertIsInstance(tb2.get_result(tid3), KeyError)
            self.assertEqual(tb2.get_traceback(tid3), einfo.traceback)

    def test_repair_uuid(self):
        from celery.backends.amqp import repair_uuid
        for i in range(10):
            tid = uuid()
            self.assertEqual(repair_uuid(tid.replace("-", "")), tid)

    def test_expires_defaults_to_config_deprecated_setting(self):
        app = app_or_default()
        prev = app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES
        app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = 10
        try:
            b = self.create_backend()
            self.assertEqual(b.queue_arguments.get("x-expires"), 10 * 1000.0)
        finally:
            app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = prev

    def test_expires_is_int(self):
        b = self.create_backend(expires=48)
        self.assertEqual(b.queue_arguments.get("x-expires"), 48 * 1000.0)

    def test_expires_is_timedelta(self):
        b = self.create_backend(expires=timedelta(minutes=1))
        self.assertEqual(b.queue_arguments.get("x-expires"), 60 * 1000.0)

    @sleepdeprived()
    def test_store_result_retries(self):

        class _Producer(object):
            iterations = 0
            stop_raising_at = 5

            def __init__(self, *args, **kwargs):
                pass

            def publish(self, msg, *args, **kwargs):
                if self.iterations > self.stop_raising_at:
                    return
                raise KeyError("foo")

        class Backend(AMQPBackend):
            Producer = _Producer

        backend = Backend()
        with self.assertRaises(KeyError):
            backend.store_result("foo", "bar", "STARTED", max_retries=None)

        with self.assertRaises(KeyError):
            backend.store_result("foo", "bar", "STARTED", max_retries=10)

    def assertState(self, retval, state):
        self.assertEqual(retval["status"], state)

    def test_poll_no_messages(self):
        b = self.create_backend()
        self.assertState(b.poll(uuid()), states.PENDING)

    def test_poll_result(self):

        results = Queue()

        class Message(object):

            def __init__(self, **merge):
                self.payload = dict({"status": states.STARTED,
                                     "result": None}, **merge)

        class MockBinding(object):

            def __init__(self, *args, **kwargs):
                pass

            def __call__(self, *args, **kwargs):
                return self

            def declare(self):
                pass

            def get(self, no_ack=False):
                try:
                    return results.get(block=False)
                except Empty:
                    pass

        class MockBackend(AMQPBackend):
            Queue = MockBinding

        backend = MockBackend()

        # FFWD's to the latest state.
        results.put(Message(status=states.RECEIVED, seq=1))
        results.put(Message(status=states.STARTED, seq=2))
        results.put(Message(status=states.FAILURE, seq=3))
        r1 = backend.poll(uuid())
        self.assertDictContainsSubset({"status": states.FAILURE,
                                       "seq": 3}, r1,
                                       "FFWDs to the last state")

        # Caches last known state.
        results.put(Message())
        tid = uuid()
        backend.poll(tid)
        self.assertIn(tid, backend._cache, "Caches last known state")

        # Returns cache if no new states.
        results.queue.clear()
        assert not results.qsize()
        backend._cache[tid] = "hello"
        self.assertEqual(backend.poll(tid), "hello",
                         "Returns cache if no new states")

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
                         "result is cached")
        self.assertEqual(b.wait_for(tid, timeout=1, cache=False), 56)
        b.store_result(tid, KeyError("foo"), states.FAILURE)
        with self.assertRaises(KeyError):
            b.wait_for(tid, timeout=1, cache=False)

    def test_drain_events_remaining_timeouts(self):

        class Connection(object):

            def drain_events(self, timeout=None):
                pass

        b = self.create_backend()
        with current_app.pool.acquire_channel(block=False) as (_, channel):
            binding = b._create_binding(uuid())
            consumer = b._create_consumer(binding, channel)
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
        expected_results = [(tid, {"status": states.SUCCESS,
                                    "result": i,
                                    "traceback": None,
                                    "task_id": tid})
                                for i, tid in enumerate(tids)]
        self.assertEqual(sorted(res), sorted(expected_results))
        self.assertDictEqual(b._cache[res[0][0]], res[0][1])
        cached_res = list(b.get_many(tids, timeout=1))
        self.assertEqual(sorted(cached_res), sorted(expected_results))
        b._cache[res[0][0]]["status"] = states.RETRY
        with self.assertRaises(socket.timeout):
            list(b.get_many(tids, timeout=0.01))

    def test_test_get_many_raises_outer_block(self):

        class Backend(AMQPBackend):

            def _create_consumer(self, *args, **kwargs):
                raise KeyError("foo")

        b = Backend()
        with self.assertRaises(KeyError):
            b.get_many(["id1"]).next()

    def test_test_get_many_raises_inner_block(self):

        class Backend(AMQPBackend):

            def drain_events(self, *args, **kwargs):
                raise KeyError("foo")

        b = Backend()
        with self.assertRaises(KeyError):
            b.get_many(["id1"]).next()

    def test_no_expires(self):
        b = self.create_backend(expires=None)
        app = app_or_default()
        prev = app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES
        app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = None
        try:
            b = self.create_backend(expires=None)
            with self.assertRaises(KeyError):
                b.queue_arguments["x-expires"]
        finally:
            app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = prev

    def test_process_cleanup(self):
        self.create_backend().process_cleanup()

    def test_reload_task_result(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().reload_task_result("x")

    def test_reload_taskset_result(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().reload_taskset_result("x")

    def test_save_taskset(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().save_taskset("x", "x")

    def test_restore_taskset(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().restore_taskset("x")

    def test_delete_taskset(self):
        with self.assertRaises(NotImplementedError):
            self.create_backend().delete_taskset("x")
