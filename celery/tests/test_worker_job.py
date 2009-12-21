# -*- coding: utf-8 -*-
import sys
import logging
import unittest
import simplejson
from StringIO import StringIO

from django.core import cache
from carrot.backends.base import BaseMessage

from celery.log import setup_logger
from celery.task.base import Task
from celery.utils import gen_unique_id
from celery.models import TaskMeta
from celery.result import AsyncResult
from celery.worker.job import WorkerTaskTrace, TaskWrapper
from celery.worker.pool import TaskPool
from celery.exceptions import RetryTaskError, NotRegistered
from celery.decorators import task as task_dec
from celery.datastructures import ExceptionInfo

scratch = {"ACK": False}
some_kwargs_scratchpad = {}


def jail(task_id, task_name, args, kwargs):
    return WorkerTaskTrace(task_name, task_id, args, kwargs)()


def on_ack():
    scratch["ACK"] = True


@task_dec()
def mytask(i, **kwargs):
    return i ** i


@task_dec()
def mytask_no_kwargs(i):
    return i ** i


class MyTaskIgnoreResult(Task):
    ignore_result = True

    def run(self, i):
        return i ** i


@task_dec()
def mytask_some_kwargs(i, logfile):
    some_kwargs_scratchpad["logfile"] = logfile
    return i ** i


@task_dec()
def mytask_raising(i, **kwargs):
    raise KeyError(i)


@task_dec()
def get_db_connection(i, **kwargs):
    from django.db import connection
    return id(connection)
get_db_connection.ignore_result = True


class TestRetryTaskError(unittest.TestCase):

    def test_retry_task_error(self):
        try:
            raise Exception("foo")
        except Exception, exc:
            ret = RetryTaskError("Retrying task", exc)

        self.assertEquals(ret.exc, exc)


class TestJail(unittest.TestCase):

    def test_execute_jail_success(self):
        ret = jail(gen_unique_id(), mytask.name, [2], {})
        self.assertEquals(ret, 4)

    def test_execute_jail_failure(self):
        ret = jail(gen_unique_id(), mytask_raising.name,
                   [4], {})
        self.assertTrue(isinstance(ret, ExceptionInfo))
        self.assertEquals(ret.exception.args, (4, ))

    def test_execute_ignore_result(self):
        task_id = gen_unique_id()
        ret = jail(id, MyTaskIgnoreResult.name,
                   [4], {})
        self.assertTrue(ret, 8)
        self.assertFalse(AsyncResult(task_id).ready())

    def test_django_db_connection_is_closed(self):
        from django.db import connection
        connection._was_closed = False
        old_connection_close = connection.close

        def monkeypatched_connection_close(*args, **kwargs):
            connection._was_closed = True
            return old_connection_close(*args, **kwargs)

        connection.close = monkeypatched_connection_close
        try:
            jail(gen_unique_id(), get_db_connection.name, [2], {})
            self.assertTrue(connection._was_closed)
        finally:
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

        jail(gen_unique_id(), mytask.name, [4], {})
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

        jail(gen_unique_id(), mytask.name, [4], {})
        self.assertTrue(cache._was_closed)
        cache.cache.close = old_cache_close
        cache.settings.CACHE_BACKEND = old_backend
        if old_cache_parse_backend:
            cache.parse_backend_uri = old_cache_parse_backend
        else:
            del(cache.parse_backend_uri)


class TestTaskWrapper(unittest.TestCase):

    def test_task_wrapper_repr(self):
        tw = TaskWrapper(mytask.name, gen_unique_id(), [1], {"f": "x"})
        self.assertTrue(repr(tw))

    def test_task_wrapper_mail_attrs(self):
        tw = TaskWrapper(mytask.name, gen_unique_id(), [], {})
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
        body = {"task": mytask.name, "id": gen_unique_id(),
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
        tw = TaskWrapper(mytask.name, tid, [4], {"f": "x"})
        self.assertEquals(tw.execute(), 256)
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertEquals(meta.result, 256)
        self.assertEquals(meta.status, "SUCCESS")

    def test_execute_success_no_kwargs(self):
        tid = gen_unique_id()
        tw = TaskWrapper(mytask_no_kwargs.name, tid, [4], {})
        self.assertEquals(tw.execute(), 256)
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertEquals(meta.result, 256)
        self.assertEquals(meta.status, "SUCCESS")

    def test_execute_success_some_kwargs(self):
        tid = gen_unique_id()
        tw = TaskWrapper(mytask_some_kwargs.name, tid, [4], {})
        self.assertEquals(tw.execute(logfile="foobaz.log"), 256)
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertEquals(some_kwargs_scratchpad.get("logfile"), "foobaz.log")
        self.assertEquals(meta.result, 256)
        self.assertEquals(meta.status, "SUCCESS")

    def test_execute_ack(self):
        tid = gen_unique_id()
        tw = TaskWrapper(mytask.name, tid, [4], {"f": "x"},
                        on_ack=on_ack)
        self.assertEquals(tw.execute(), 256)
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertTrue(scratch["ACK"])
        self.assertEquals(meta.result, 256)
        self.assertEquals(meta.status, "SUCCESS")

    def test_execute_fail(self):
        tid = gen_unique_id()
        tw = TaskWrapper(mytask_raising.name, tid, [4], {"f": "x"})
        self.assertTrue(isinstance(tw.execute(), ExceptionInfo))
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertEquals(meta.status, "FAILURE")
        self.assertTrue(isinstance(meta.result, KeyError))

    def test_execute_using_pool(self):
        tid = gen_unique_id()
        tw = TaskWrapper(mytask.name, tid, [4], {"f": "x"})
        p = TaskPool(2)
        p.start()
        asyncres = tw.execute_using_pool(p)
        self.assertTrue(asyncres.get(), 256)
        p.stop()

    def test_default_kwargs(self):
        tid = gen_unique_id()
        tw = TaskWrapper(mytask.name, tid, [4], {"f": "x"})
        self.assertEquals(tw.extend_with_default_kwargs(10, "some_logfile"), {
            "f": "x",
            "logfile": "some_logfile",
            "loglevel": 10,
            "task_id": tw.task_id,
            "task_retries": 0,
            "task_name": tw.task_name})

    def test_on_failure(self):
        tid = gen_unique_id()
        tw = TaskWrapper(mytask.name, tid, [4], {"f": "x"})
        try:
            raise Exception("Inside unit tests")
        except Exception:
            exc_info = ExceptionInfo(sys.exc_info())

        logfh = StringIO()
        tw.logger.handlers = []
        tw.logger = setup_logger(logfile=logfh, loglevel=logging.INFO)

        from celery import conf
        conf.CELERY_SEND_TASK_ERROR_EMAILS = True

        tw.on_failure(exc_info)
        logvalue = logfh.getvalue()
        self.assertTrue(mytask.name in logvalue)
        self.assertTrue(tid in logvalue)
        self.assertTrue("ERROR" in logvalue)

        conf.CELERY_SEND_TASK_ERROR_EMAILS = False
