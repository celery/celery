from __future__ import absolute_import
from __future__ import with_statement

from datetime import timedelta

from mock import Mock, patch
from nose import SkipTest
from pickle import loads, dumps

from kombu.utils import cached_property, uuid

from celery import current_app
from celery import states
from celery.datastructures import AttributeDict
from celery.exceptions import ImproperlyConfigured
from celery.result import AsyncResult
from celery.task import subtask
from celery.utils.timeutils import timedelta_seconds

from celery.tests.utils import Case


class Redis(object):

    class Connection(object):
        connected = True

        def disconnect(self):
            self.connected = False

    def __init__(self, host=None, port=None, db=None, password=None, **kw):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.connection = self.Connection()
        self.keyspace = {}
        self.expiry = {}

    def get(self, key):
        return self.keyspace.get(key)

    def setex(self, key, value, expires):
        self.set(key, value)
        self.expire(key, expires)

    def set(self, key, value):
        self.keyspace[key] = value

    def expire(self, key, expires):
        self.expiry[key] = expires

    def delete(self, key):
        self.keyspace.pop(key)

    def publish(self, key, value):
        pass


class redis(object):
    Redis = Redis

    class ConnectionPool(object):

        def __init__(self, **kwargs):
            pass


class test_RedisBackend(Case):

    def get_backend(self):
        from celery.backends import redis

        class RedisBackend(redis.RedisBackend):
            redis = redis

        return RedisBackend

    def setUp(self):
        self.Backend = self.get_backend()

        class MockBackend(self.Backend):

            @cached_property
            def client(self):
                return Mock()

        self.MockBackend = MockBackend

    def test_reduce(self):
        try:
            from celery.backends.redis import RedisBackend
            x = RedisBackend()
            self.assertTrue(loads(dumps(x)))
        except ImportError:
            raise SkipTest('redis not installed')

    def test_no_redis(self):
        self.MockBackend.redis = None
        with self.assertRaises(ImproperlyConfigured):
            self.MockBackend()

    def test_url(self):
        x = self.MockBackend('redis://foobar//1')
        self.assertEqual(x.host, 'foobar')
        self.assertEqual(x.db, '1')

    def test_conf_raises_KeyError(self):
        conf = AttributeDict({'CELERY_RESULT_SERIALIZER': 'json',
                              'CELERY_MAX_CACHED_RESULTS': 1,
                              'CELERY_TASK_RESULT_EXPIRES': None})
        prev, current_app.conf = current_app.conf, conf
        try:
            self.MockBackend()
        finally:
            current_app.conf = prev

    def test_expires_defaults_to_config(self):
        conf = current_app.conf
        prev = conf.CELERY_TASK_RESULT_EXPIRES
        conf.CELERY_TASK_RESULT_EXPIRES = 10
        try:
            b = self.Backend(expires=None)
            self.assertEqual(b.expires, 10)
        finally:
            conf.CELERY_TASK_RESULT_EXPIRES = prev

    def test_expires_is_int(self):
        b = self.Backend(expires=48)
        self.assertEqual(b.expires, 48)

    def test_expires_is_None(self):
        b = self.Backend(expires=None)
        self.assertEqual(b.expires, timedelta_seconds(
            current_app.conf.CELERY_TASK_RESULT_EXPIRES))

    def test_expires_is_timedelta(self):
        b = self.Backend(expires=timedelta(minutes=1))
        self.assertEqual(b.expires, 60)

    def test_on_chord_apply(self):
        self.Backend().on_chord_apply('group_id', {},
                                      result=map(AsyncResult, [1, 2, 3]))

    def test_mget(self):
        b = self.MockBackend()
        self.assertTrue(b.mget(['a', 'b', 'c']))
        b.client.mget.assert_called_with(['a', 'b', 'c'])

    def test_set_no_expire(self):
        b = self.MockBackend()
        b.expires = None
        b.set('foo', 'bar')

    @patch('celery.result.GroupResult')
    def test_on_chord_part_return(self, setresult):
        b = self.MockBackend()
        deps = Mock()
        deps.__len__ = Mock()
        deps.__len__.return_value = 10
        setresult.restore.return_value = deps
        b.client.incr.return_value = 1
        task = Mock()
        task.name = 'foobarbaz'
        try:
            current_app.tasks['foobarbaz'] = task
            task.request.chord = subtask(task)
            task.request.group = 'group_id'

            b.on_chord_part_return(task)
            self.assertTrue(b.client.incr.call_count)

            b.client.incr.return_value = len(deps)
            b.on_chord_part_return(task)
            deps.join_native.assert_called_with(propagate=False)
            deps.delete.assert_called_with()

            self.assertTrue(b.client.expire.call_count)
        finally:
            current_app.tasks.pop('foobarbaz')

    def test_process_cleanup(self):
        self.Backend().process_cleanup()

    def test_get_set_forget(self):
        b = self.Backend()
        tid = uuid()
        b.store_result(tid, 42, states.SUCCESS)
        self.assertEqual(b.get_status(tid), states.SUCCESS)
        self.assertEqual(b.get_result(tid), 42)
        b.forget(tid)
        self.assertEqual(b.get_status(tid), states.PENDING)

    def test_set_expires(self):
        b = self.Backend(expires=512)
        tid = uuid()
        key = b.get_key_for_task(tid)
        b.store_result(tid, 42, states.SUCCESS)
        self.assertEqual(b.client.expiry[key], 512)
