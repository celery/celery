# -*- coding: utf-8 -*-
import sys
import unittest
from celery.worker.job import jail
from celery.worker.job import TaskWrapper
from celery.datastructures import ExceptionInfo
from celery.models import TaskMeta
from celery.registry import tasks, NotRegistered
from celery.pool import TaskPool
from celery.utils import gen_unique_id
from carrot.backends.base import BaseMessage
from StringIO import StringIO
from celery.log import setup_logger
from django.core import cache
import simplejson
import logging

scratch = {"ACK": False}


def on_ack():
    scratch["ACK"] = True


def mytask(i, **kwargs):
    return i ** i
tasks.register(mytask, name="cu.mytask")


def mytask_raising(i, **kwargs):
    raise KeyError(i)
tasks.register(mytask_raising, name="cu.mytask-raising")


def get_db_connection(i, **kwargs):
    from django.db import connection
    return id(connection)
get_db_connection.ignore_result = True


class TestJail(unittest.TestCase):

    def test_execute_jail_success(self):
        ret = jail(gen_unique_id(), gen_unique_id(), mytask, [2], {})
        self.assertEquals(ret, 4)

    def test_execute_jail_failure(self):
        ret = jail(gen_unique_id(), gen_unique_id(), mytask_raising, [4], {})
        self.assertTrue(isinstance(ret, ExceptionInfo))
        self.assertEquals(ret.exception.args, (4, ))

    def test_django_db_connection_is_closed(self):
        from django.db import connection
        connection._was_closed = False
        old_connection_close = connection.close

        def monkeypatched_connection_close(*args, **kwargs):
            connection._was_closed = True
            return old_connection_close(*args, **kwargs)

        connection.close = monkeypatched_connection_close

        ret = jail(gen_unique_id(), gen_unique_id(),
                   get_db_connection, [2], {})
        self.assertTrue(connection._was_closed)

        connection.close = old_connection_close

    def test_django_cache_connection_is_closed(self):
        old_cache_close = getattr(cache.cache, "close", None)
        old_backend = cache.settings.CACHE_BACKEND
        cache.settings.CACHE_BACKEND = "libmemcached"
        cache._was_closed = False
        old_cache_parse_backend = getattr(cache, "parse_backend_uri", None)
        if old_cache_parse_backend: # checks to make sure attr exists
            delattr(cache, 'parse_backend_uri')

        def monkeypatched_cache_close(*args, **kwargs):
            cache._was_closed = True

        cache.cache.close = monkeypatched_cache_close

        jail(gen_unique_id(), gen_unique_id(), mytask, [4], {})
        self.assertTrue(cache._was_closed)
        cache.cache.close = old_cache_close
        cache.settings.CACHE_BACKEND = old_backend
        if old_cache_parse_backend:
            cache.parse_backend_uri = old_cache_parse_backend

    def test_django_cache_connection_is_closed_django_1_1(self):
        old_cache_close = getattr(cache.cache, "close", None)
        old_backend = cache.settings.CACHE_BACKEND
        cache.settings.CACHE_BACKEND = "libmemcached"
        cache._was_closed = False
        old_cache_parse_backend = getattr(cache, "parse_backend_uri", None)
        cache.parse_backend_uri = lambda uri: ["libmemcached", "1", "2"]

        def monkeypatched_cache_close(*args, **kwargs):
            cache._was_closed = True

        cache.cache.close = monkeypatched_cache_close

        jail(gen_unique_id(), gen_unique_id(), mytask, [4], {})
        self.assertTrue(cache._was_closed)
        cache.cache.close = old_cache_close
        cache.settings.CACHE_BACKEND = old_backend
        if old_cache_parse_backend:
            cache.parse_backend_uri = old_cache_parse_backend
        else:
            del(cache.parse_backend_uri)


class TestTaskWrapper(unittest.TestCase):

    def test_task_wrapper_attrs(self):
        tw = TaskWrapper(gen_unique_id(), gen_unique_id(),
                         mytask, [1], {"f": "x"})
        for attr in ("task_name", "task_id", "args", "kwargs", "logger"):
            self.assertTrue(getattr(tw, attr, None))

    def test_task_wrapper_repr(self):
        tw = TaskWrapper(gen_unique_id(), gen_unique_id(),
                         mytask, [1], {"f": "x"})
        self.assertTrue(repr(tw))

    def test_task_wrapper_mail_attrs(self):
        tw = TaskWrapper(gen_unique_id(), gen_unique_id(), mytask, [], {})
        x = tw.success_msg % {"name": tw.task_name,
                              "id": tw.task_id,
                              "return_value": 10}
        self.assertTrue(x)
        x = tw.fail_msg % {"name": tw.task_name,
                           "id": tw.task_id,
                           "exc": "FOOBARBAZ",
                           "traceback": "foobarbaz"}
        self.assertTrue(x)
        x = tw.fail_email_subject % {"name": tw.task_name,
                                     "id": tw.task_id,
                                     "exc": "FOOBARBAZ",
                                     "hostname": "lana"}
        self.assertTrue(x)

    def test_from_message(self):
        body = {"task": "cu.mytask", "id": gen_unique_id(),
                "args": [2], "kwargs": {u"æØåveéðƒeæ": "bar"}}
        m = BaseMessage(body=simplejson.dumps(body), backend="foo",
                        content_type="application/json",
                        content_encoding="utf-8")
        tw = TaskWrapper.from_message(m, m.decode())
        self.assertTrue(isinstance(tw, TaskWrapper))
        self.assertEquals(tw.task_name, body["task"])
        self.assertEquals(tw.task_id, body["id"])
        self.assertEquals(tw.args, body["args"])
        self.assertEquals(tw.kwargs.keys()[0],
                          u"æØåveéðƒeæ".encode("utf-8"))
        self.assertFalse(isinstance(tw.kwargs.keys()[0], unicode))
        self.assertEquals(id(mytask), id(tw.task_func))
        self.assertTrue(tw.logger)

    def test_from_message_nonexistant_task(self):
        body = {"task": "cu.mytask.doesnotexist", "id": gen_unique_id(),
                "args": [2], "kwargs": {u"æØåveéðƒeæ": "bar"}}
        m = BaseMessage(body=simplejson.dumps(body), backend="foo",
                        content_type="application/json",
                        content_encoding="utf-8")
        self.assertRaises(NotRegistered, TaskWrapper.from_message,
                          m, m.decode())

    def test_execute(self):
        tid = gen_unique_id()
        tw = TaskWrapper("cu.mytask", tid, mytask, [4], {"f": "x"})
        self.assertEquals(tw.execute(), 256)
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertEquals(meta.result, 256)
        self.assertEquals(meta.status, "DONE")

    def test_execute_ack(self):
        tid = gen_unique_id()
        tw = TaskWrapper("cu.mytask", tid, mytask, [4], {"f": "x"},
                        on_ack=on_ack)
        self.assertEquals(tw.execute(), 256)
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertTrue(scratch["ACK"])
        self.assertEquals(meta.result, 256)
        self.assertEquals(meta.status, "DONE")

    def test_execute_fail(self):
        tid = gen_unique_id()
        tw = TaskWrapper("cu.mytask-raising", tid, mytask_raising, [4],
                         {"f": "x"})
        self.assertTrue(isinstance(tw.execute(), ExceptionInfo))
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertEquals(meta.status, "FAILURE")
        self.assertTrue(isinstance(meta.result, KeyError))

    def test_execute_using_pool(self):
        tid = gen_unique_id()
        tw = TaskWrapper("cu.mytask", tid, mytask, [4], {"f": "x"})
        p = TaskPool(2)
        p.start()
        asyncres = tw.execute_using_pool(p)
        self.assertTrue(asyncres.get(), 256)
        p.stop()

    def test_default_kwargs(self):
        tid = gen_unique_id()
        tw = TaskWrapper("cu.mytask", tid, mytask, [4], {"f": "x"})
        self.assertEquals(tw.extend_with_default_kwargs(10, "some_logfile"), {
            "f": "x",
            "logfile": "some_logfile",
            "loglevel": 10,
            "task_id": tw.task_id,
            "task_retries": 0,
            "task_name": tw.task_name})

    def test_on_failure(self):
        tid = gen_unique_id()
        tw = TaskWrapper("cu.mytask", tid, mytask, [4], {"f": "x"})
        try:
            raise Exception("Inside unit tests")
        except Exception:
            exc_info = ExceptionInfo(sys.exc_info())

        logfh = StringIO()
        tw.logger.handlers = []
        tw.logger = setup_logger(logfile=logfh, loglevel=logging.INFO)

        from celery import conf
        conf.SEND_CELERY_TASK_ERROR_EMAILS = True

        tw.on_failure(exc_info, {"task_id": tid, "task_name": "cu.mytask"})
        logvalue = logfh.getvalue()
        self.assertTrue("cu.mytask" in logvalue)
        self.assertTrue(tid in logvalue)
        self.assertTrue("ERROR" in logvalue)

        conf.SEND_CELERY_TASK_ERROR_EMAILS = False
