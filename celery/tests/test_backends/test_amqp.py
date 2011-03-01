import socket
import sys

from datetime import timedelta
from Queue import Empty, Queue

from celery import states
from celery.app import app_or_default
from celery.backends.amqp import AMQPBackend
from celery.datastructures import ExceptionInfo
from celery.exceptions import TimeoutError
from celery.utils import gen_unique_id

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

        tid = gen_unique_id()

        tb1.mark_as_done(tid, 42)
        self.assertEqual(tb2.get_status(tid), states.SUCCESS)
        self.assertEqual(tb2.get_result(tid), 42)
        self.assertTrue(tb2._cache.get(tid))
        self.assertTrue(tb2.get_result(tid), 42)

    def test_is_pickled(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb1.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb2.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid3 = gen_unique_id()
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
            uuid = gen_unique_id()
            self.assertEqual(repair_uuid(uuid.replace("-", "")), uuid)

    def test_expires_defaults_to_config(self):
        app = app_or_default()
        prev = app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES
        app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = 10
        try:
            b = self.create_backend(expires=None)
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
        self.assertRaises(KeyError, backend.store_result,
                          "foo", "bar", "STARTED", max_retries=None)

        print(backend.store_result)
        self.assertRaises(KeyError, backend.store_result,
                          "foo", "bar", "STARTED", max_retries=10)

    def assertState(self, retval, state):
        self.assertEqual(retval["status"], state)

    def test_poll_no_messages(self):
        b = self.create_backend()
        self.assertState(b.poll(gen_unique_id()), states.PENDING)

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
        r1 = backend.poll(gen_unique_id())
        self.assertDictContainsSubset({"status": states.FAILURE,
                                       "seq": 3}, r1,
                                       "FFWDs to the last state")

        # Caches last known state.
        results.put(Message())
        uuid = gen_unique_id()
        backend.poll(uuid)
        self.assertIn(uuid, backend._cache, "Caches last known state")

        # Returns cache if no new states.
        results.queue.clear()
        assert not results.qsize()
        backend._cache[uuid] = "hello"
        self.assertEqual(backend.poll(uuid), "hello",
                         "Returns cache if no new states")

    def test_wait_for(self):
        b = self.create_backend()

        uuid = gen_unique_id()
        self.assertRaises(TimeoutError, b.wait_for, uuid, timeout=0.1)
        b.store_result(uuid, None, states.STARTED)
        self.assertRaises(TimeoutError, b.wait_for, uuid, timeout=0.1)
        b.store_result(uuid, None, states.RETRY)
        self.assertRaises(TimeoutError, b.wait_for, uuid, timeout=0.1)
        b.store_result(uuid, 42, states.SUCCESS)
        self.assertEqual(b.wait_for(uuid, timeout=1), 42)
        b.store_result(uuid, 56, states.SUCCESS)
        self.assertEqual(b.wait_for(uuid, timeout=1), 42,
                         "result is cached")
        self.assertEqual(b.wait_for(uuid, timeout=1, cache=False), 56)
        b.store_result(uuid, KeyError("foo"), states.FAILURE)
        self.assertRaises(KeyError, b.wait_for, uuid, timeout=1, cache=False)

    def test_drain_events_remaining_timeouts(self):

        class Connection(object):

            def drain_events(self, timeout=None):
                pass

        b = self.create_backend()
        conn = b.pool.acquire(block=False)
        channel = conn.channel()
        try:
            binding = b._create_binding(gen_unique_id())
            consumer = b._create_consumer(binding, channel)
            self.assertRaises(socket.timeout, b.drain_events,
                              Connection(), consumer, timeout=0.1)
        finally:
            channel.close()
            conn.release()

    def test_get_many(self):
        b = self.create_backend()

        uuids = []
        for i in xrange(10):
            uuid = gen_unique_id()
            b.store_result(uuid, i, states.SUCCESS)
            uuids.append(uuid)

        res = list(b.get_many(uuids, timeout=1))
        expected_results = [(uuid, {"status": states.SUCCESS,
                                    "result": i,
                                    "traceback": None,
                                    "task_id": uuid})
                                for i, uuid in enumerate(uuids)]
        self.assertItemsEqual(res, expected_results)
        self.assertDictEqual(b._cache[res[0][0]], res[0][1])
        cached_res = list(b.get_many(uuids, timeout=1))
        self.assertItemsEqual(cached_res, expected_results)
        b._cache[res[0][0]]["status"] = states.RETRY
        self.assertRaises(socket.timeout, list,
                          b.get_many(uuids, timeout=0.01))

    def test_test_get_many_raises_outer_block(self):

        class Backend(AMQPBackend):

            def _create_consumer(self, *args, **kwargs):
                raise KeyError("foo")

        b = Backend()
        self.assertRaises(KeyError, b.get_many(["id1"]).next)

    def test_test_get_many_raises_inner_block(self):

        class Backend(AMQPBackend):

            def drain_events(self, *args, **kwargs):
                raise KeyError("foo")

        b = Backend()
        self.assertRaises(KeyError, b.get_many(["id1"]).next)

    def test_no_expires(self):
        b = self.create_backend(expires=None)
        app = app_or_default()
        prev = app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES
        app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = None
        try:
            b = self.create_backend(expires=None)
            self.assertRaises(KeyError, b.queue_arguments.__getitem__,
                              "x-expires")
        finally:
            app.conf.CELERY_AMQP_TASK_RESULT_EXPIRES = prev

    def test_process_cleanup(self):
        self.create_backend().process_cleanup()

    def test_reload_task_result(self):
        self.assertRaises(NotImplementedError,
                          self.create_backend().reload_task_result, "x")

    def test_reload_taskset_result(self):
        self.assertRaises(NotImplementedError,
                          self.create_backend().reload_taskset_result, "x")

    def test_save_taskset(self):
        self.assertRaises(NotImplementedError,
                          self.create_backend().save_taskset, "x", "x")

    def test_restore_taskset(self):
        self.assertRaises(NotImplementedError,
                          self.create_backend().restore_taskset, "x")
