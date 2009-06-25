# -*- coding: utf-8 -*-
import unittest
from celery.worker.job import jail
from celery.worker.job import TaskWrapper
from celery.datastructures import ExceptionInfo
from celery.models import TaskMeta
from celery.registry import tasks, NotRegistered
from celery.pool import TaskPool
from uuid import uuid4
from carrot.backends.base import BaseMessage
import simplejson


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
        ret = jail(str(uuid4()), str(uuid4()), mytask, [2], {})
        self.assertEquals(ret, 4)
    
    def test_execute_jail_failure(self):
        ret = jail(str(uuid4()), str(uuid4()), mytask_raising, [4], {})
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

        ret = jail(str(uuid4()), str(uuid4()), get_db_connection, [2], {})
        self.assertTrue(connection._was_closed)

        connection.close = old_connection_close


class TestTaskWrapper(unittest.TestCase):

    def test_task_wrapper_attrs(self):
        tw = TaskWrapper(str(uuid4()), str(uuid4()), mytask, [1], {"f":"x"})
        for attr in ("task_name", "task_id", "args", "kwargs", "logger"):
            self.assertTrue(getattr(tw, attr, None))

    def test_task_wrapper_repr(self):
        tw = TaskWrapper(str(uuid4()), str(uuid4()), mytask, [1], {"f":"x"})
        self.assertTrue(repr(tw))

    def test_task_wrapper_mail_attrs(self):
        tw = TaskWrapper(str(uuid4()), str(uuid4()), mytask, [], {})
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
        body = {"task": "cu.mytask", "id": str(uuid4()),
                "args": [2], "kwargs": {u"æØåveéðƒeæ": "bar"}}
        m = BaseMessage(body=simplejson.dumps(body), backend="foo",
                        content_type="application/json",
                        content_encoding="utf-8")
        tw = TaskWrapper.from_message(m, m.decode())
        self.assertTrue(isinstance(tw, TaskWrapper))
        self.assertEquals(tw.task_name, body["task"])
        self.assertEquals(tw.task_id, body["id"])
        self.assertEquals(tw.args, body["args"])
        self.assertEquals(tw.kwargs.keys()[0], u"æØåveéðƒeæ".encode("utf-8"))
        self.assertFalse(isinstance(tw.kwargs.keys()[0], unicode))
        self.assertEquals(id(mytask), id(tw.task_func))
        self.assertTrue(tw.logger)

    def test_from_message_nonexistant_task(self):
        body = {"task": "cu.mytask.doesnotexist", "id": str(uuid4()),
                "args": [2], "kwargs": {u"æØåveéðƒeæ": "bar"}}
        m = BaseMessage(body=simplejson.dumps(body), backend="foo",
                        content_type="application/json",
                        content_encoding="utf-8")
        self.assertRaises(NotRegistered, TaskWrapper.from_message,
                          m, m.decode())

    def test_execute(self):
        tid = str(uuid4())
        tw = TaskWrapper("cu.mytask", tid, mytask, [4], {"f":"x"})
        self.assertEquals(tw.execute(), 256)
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertEquals(meta.result, 256)
        self.assertEquals(meta.status, "DONE")

    def test_execute_fail(self):
        tid = str(uuid4())
        tw = TaskWrapper("cu.mytask-raising", tid, mytask_raising, [4],
                         {"f":"x"})
        self.assertTrue(isinstance(tw.execute(), ExceptionInfo))
        meta = TaskMeta.objects.get(task_id=tid)
        self.assertEquals(meta.status, "FAILURE")
        self.assertTrue(isinstance(meta.result, KeyError))

    def test_execute_using_pool(self):
        tid = str(uuid4())
        tw = TaskWrapper("cu.mytask", tid, mytask, [4], {"f":"x"})
        p = TaskPool(2)
        p.start()
        asyncres = tw.execute_using_pool(p)
        self.assertTrue(asyncres.get(), 256)
        p.stop()

    def test_default_kwargs(self):
        tid = str(uuid4())
        tw = TaskWrapper("cu.mytask", tid, mytask, [4], {"f":"x"})
        self.assertEquals(tw.extend_with_default_kwargs(10, "some_logfile"), {
            "f": "x",
            "logfile": "some_logfile",
            "loglevel": 10,
            "task_id": tw.task_id,
            "task_name": tw.task_name})
