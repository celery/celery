# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import with_statement

import anyjson
import logging
import os
import sys
import time

from datetime import datetime, timedelta

from kombu.transport.base import Message
from mock import Mock
from nose import SkipTest

from celery import states
from celery.app import app_or_default
from celery.concurrency.base import BasePool
from celery.datastructures import ExceptionInfo
from celery.task import task as task_dec
from celery.exceptions import RetryTaskError, NotRegistered, WorkerLostError
from celery.log import setup_logger
from celery.result import AsyncResult
from celery.task.base import Task
from celery.utils import uuid
from celery.utils.encoding import from_utf8
from celery.worker.job import (WorkerTaskTrace, TaskRequest,
                               InvalidTaskError, execute_and_trace,
                               default_encode)
from celery.worker.state import revoked

from celery.tests.compat import catch_warnings
from celery.tests.utils import unittest
from celery.tests.utils import WhateverIO, wrap_logger


scratch = {"ACK": False}
some_kwargs_scratchpad = {}


def jail(task_id, task_name, args, kwargs):
    return WorkerTaskTrace(task_name, task_id, args, kwargs)()


def on_ack():
    scratch["ACK"] = True


@task_dec(accept_magic_kwargs=True)
def mytask(i, **kwargs):
    return i ** i


@task_dec               # traverses coverage for decorator without parens
def mytask_no_kwargs(i):
    return i ** i


class MyTaskIgnoreResult(Task):
    ignore_result = True

    def run(self, i):
        return i ** i


@task_dec(accept_magic_kwargs=True)
def mytask_some_kwargs(i, logfile):
    some_kwargs_scratchpad["logfile"] = logfile
    return i ** i


@task_dec(accept_magic_kwargs=True)
def mytask_raising(i, **kwargs):
    raise KeyError(i)


class test_default_encode(unittest.TestCase):

    def setUp(self):
        if sys.version_info >= (3, 0):
            raise SkipTest("py3k: not relevant")

    def test_jython(self):
        prev, sys.platform = sys.platform, "java 1.6.1"
        try:
            self.assertEqual(default_encode("foo"), "foo")
        finally:
            sys.platform = prev

    def test_cython(self):
        prev, sys.platform = sys.platform, "darwin"
        gfe, sys.getfilesystemencoding = sys.getfilesystemencoding, \
                                         lambda: "utf-8"
        try:
            self.assertEqual(default_encode("foo"), "foo")
        finally:
            sys.platform = prev
            sys.getfilesystemencoding = gfe


class test_RetryTaskError(unittest.TestCase):

    def test_retry_task_error(self):
        try:
            raise Exception("foo")
        except Exception, exc:
            ret = RetryTaskError("Retrying task", exc)
            self.assertEqual(ret.exc, exc)


class test_WorkerTaskTrace(unittest.TestCase):

    def test_process_cleanup_fails(self):
        backend = mytask.backend
        mytask.backend = Mock()
        mytask.backend.process_cleanup = Mock(side_effect=KeyError())
        try:

            logger = mytask.app.log.get_default_logger()
            with wrap_logger(logger) as sio:
                tid = uuid()
                ret = jail(tid, mytask.name, [2], {})
                self.assertEqual(ret, 4)
                mytask.backend.mark_as_done.assert_called_with(tid, 4)
                logs = sio.getvalue().strip()
                self.assertIn("Process cleanup failed", logs)
        finally:
            mytask.backend = backend

    def test_process_cleanup_BaseException(self):
        backend = mytask.backend
        mytask.backend = Mock()
        mytask.backend.process_cleanup = Mock(side_effect=SystemExit())
        try:
            with self.assertRaises(SystemExit):
                jail(uuid(), mytask.name, [2], {})
        finally:
            mytask.backend = backend

    def test_execute_jail_success(self):
        ret = jail(uuid(), mytask.name, [2], {})
        self.assertEqual(ret, 4)

    def test_marked_as_started(self):
        mytask.track_started = True

        class Backend(mytask.backend.__class__):
            _started = []

            def mark_as_started(self, tid, *args, **kwargs):
                self._started.append(tid)

        prev, mytask.backend = mytask.backend, Backend()

        try:
            tid = uuid()
            jail(tid, mytask.name, [2], {})
            self.assertIn(tid, Backend._started)

            mytask.ignore_result = True
            tid = uuid()
            jail(tid, mytask.name, [2], {})
            self.assertNotIn(tid, Backend._started)
        finally:
            mytask.backend = prev
            mytask.track_started = False
            mytask.ignore_result = False

    def test_execute_jail_failure(self):
        ret = jail(uuid(), mytask_raising.name,
                   [4], {})
        self.assertIsInstance(ret, ExceptionInfo)
        self.assertTupleEqual(ret.exception.args, (4, ))

    def test_execute_ignore_result(self):
        task_id = uuid()
        ret = jail(id, MyTaskIgnoreResult.name,
                   [4], {})
        self.assertEqual(ret, 256)
        self.assertFalse(AsyncResult(task_id).ready())


class MockEventDispatcher(object):

    def __init__(self):
        self.sent = []

    def send(self, event, **fields):
        self.sent.append(event)


class test_TaskRequest(unittest.TestCase):

    def test_task_wrapper_repr(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        self.assertTrue(repr(tw))

    def test_sets_store_errors(self):
        mytask.ignore_result = True
        try:
            tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
            self.assertFalse(tw._store_errors)
            mytask.store_errors_even_if_ignored = True
            tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
            self.assertTrue(tw._store_errors)
        finally:
            mytask.ignore_result = False
            mytask.store_errors_even_if_ignored = False

    def test_send_event(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.eventer = MockEventDispatcher()
        tw.send_event("task-frobulated")
        self.assertIn("task-frobulated", tw.eventer.sent)

    def test_on_retry(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.eventer = MockEventDispatcher()
        try:
            raise RetryTaskError("foo", KeyError("moofoobar"))
        except:
            einfo = ExceptionInfo(sys.exc_info())
            tw.on_failure(einfo)
            self.assertIn("task-retried", tw.eventer.sent)

    def test_terminate__task_started(self):
        pool = Mock()
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.time_start = time.time()
        tw.worker_pid = 313
        tw.terminate(pool, signal="KILL")
        pool.terminate_job.assert_called_with(tw.worker_pid, "KILL")

    def test_terminate__task_reserved(self):
        pool = Mock()
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.time_start = None
        tw.terminate(pool, signal="KILL")
        self.assertFalse(pool.terminate_job.call_count)
        self.assertTupleEqual(tw._terminate_on_ack, (True, pool, "KILL"))
        tw.terminate(pool, signal="KILL")

    def test_revoked_expires_expired(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.expires = datetime.now() - timedelta(days=1)
        tw.revoked()
        self.assertIn(tw.task_id, revoked)
        self.assertEqual(mytask.backend.get_status(tw.task_id),
                         states.REVOKED)

    def test_revoked_expires_not_expired(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.expires = datetime.now() + timedelta(days=1)
        tw.revoked()
        self.assertNotIn(tw.task_id, revoked)
        self.assertNotEqual(mytask.backend.get_status(tw.task_id),
                         states.REVOKED)

    def test_revoked_expires_ignore_result(self):
        mytask.ignore_result = True
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        try:
            tw.expires = datetime.now() - timedelta(days=1)
            tw.revoked()
            self.assertIn(tw.task_id, revoked)
            self.assertNotEqual(mytask.backend.get_status(tw.task_id),
                                states.REVOKED)

        finally:
            mytask.ignore_result = False

    def test_send_email(self):
        app = app_or_default()
        old_mail_admins = app.mail_admins
        old_enable_mails = mytask.send_error_emails
        mail_sent = [False]

        def mock_mail_admins(*args, **kwargs):
            mail_sent[0] = True

        app.mail_admins = mock_mail_admins
        mytask.send_error_emails = True
        try:
            tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
            try:
                raise KeyError("moofoobar")
            except:
                einfo = ExceptionInfo(sys.exc_info())

            tw.on_failure(einfo)
            self.assertTrue(mail_sent[0])

            mail_sent[0] = False
            mytask.send_error_emails = False
            tw.on_failure(einfo)
            self.assertFalse(mail_sent[0])

            mail_sent[0] = False
            mytask.send_error_emails = True
            mytask.error_whitelist = [KeyError]
            tw.on_failure(einfo)
            self.assertTrue(mail_sent[0])

            mail_sent[0] = False
            mytask.send_error_emails = True
            mytask.error_whitelist = [SyntaxError]
            tw.on_failure(einfo)
            self.assertFalse(mail_sent[0])

        finally:
            app.mail_admins = old_mail_admins
            mytask.send_error_emails = old_enable_mails
            mytask.error_whitelist = ()

    def test_already_revoked(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw._already_revoked = True
        self.assertTrue(tw.revoked())

    def test_revoked(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        revoked.add(tw.task_id)
        self.assertTrue(tw.revoked())
        self.assertTrue(tw._already_revoked)
        self.assertTrue(tw.acknowledged)

    def test_execute_does_not_execute_revoked(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        revoked.add(tw.task_id)
        tw.execute()

    def test_execute_acks_late(self):
        mytask_raising.acks_late = True
        tw = TaskRequest(mytask_raising.name, uuid(), [1], {"f": "x"})
        try:
            tw.execute()
            self.assertTrue(tw.acknowledged)
        finally:
            mytask_raising.acks_late = False

    def test_execute_using_pool_does_not_execute_revoked(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        revoked.add(tw.task_id)
        tw.execute_using_pool(None)

    def test_on_accepted_acks_early(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.on_accepted(pid=os.getpid(), time_accepted=time.time())
        self.assertTrue(tw.acknowledged)

    def test_on_accepted_acks_late(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        mytask.acks_late = True
        try:
            tw.on_accepted(pid=os.getpid(), time_accepted=time.time())
            self.assertFalse(tw.acknowledged)
        finally:
            mytask.acks_late = False

    def test_on_accepted_terminates(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        pool = Mock()
        tw.terminate(pool, signal="KILL")
        self.assertFalse(pool.terminate_job.call_count)
        tw.on_accepted(pid=314, time_accepted=time.time())
        pool.terminate_job.assert_called_with(314, "KILL")

    def test_on_success_acks_early(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.time_start = 1
        tw.on_success(42)
        self.assertFalse(tw.acknowledged)

    def test_on_success_acks_late(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.time_start = 1
        mytask.acks_late = True
        try:
            tw.on_success(42)
            self.assertTrue(tw.acknowledged)
        finally:
            mytask.acks_late = False

    def test_on_failure_WorkerLostError(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        try:
            raise WorkerLostError("do re mi")
        except WorkerLostError:
            exc_info = ExceptionInfo(sys.exc_info())
            tw.on_failure(exc_info)
            self.assertEqual(mytask.backend.get_status(tw.task_id),
                             states.FAILURE)

        mytask.ignore_result = True
        try:
            tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
            tw.on_failure(exc_info)
            self.assertEqual(mytask.backend.get_status(tw.task_id),
                             states.PENDING)
        finally:
            mytask.ignore_result = False

    def test_on_failure_acks_late(self):
        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.time_start = 1
        mytask.acks_late = True
        try:
            try:
                raise KeyError("foo")
            except KeyError:
                exc_info = ExceptionInfo(sys.exc_info())
                tw.on_failure(exc_info)
                self.assertTrue(tw.acknowledged)
        finally:
            mytask.acks_late = False

    def test_from_message_invalid_kwargs(self):
        body = dict(task="foo", id=1, args=(), kwargs="foo")
        with self.assertRaises(InvalidTaskError):
            TaskRequest.from_message(None, body)

    def test_on_timeout(self):

        class MockLogger(object):

            def __init__(self):
                self.warnings = []
                self.errors = []

            def warning(self, msg, *args, **kwargs):
                self.warnings.append(msg % args)

            def error(self, msg, *args, **kwargs):
                self.errors.append(msg % args)

        tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
        tw.logger = MockLogger()
        tw.on_timeout(soft=True, timeout=1337)
        self.assertIn("Soft time limit (1337s) exceeded",
                      tw.logger.warnings[0])
        tw.on_timeout(soft=False, timeout=1337)
        self.assertIn("Hard time limit (1337s) exceeded", tw.logger.errors[0])
        self.assertEqual(mytask.backend.get_status(tw.task_id),
                         states.FAILURE)

        mytask.ignore_result = True
        try:
            tw = TaskRequest(mytask.name, uuid(), [1], {"f": "x"})
            tw.logger = MockLogger()
        finally:
            mytask.ignore_result = False
            tw.on_timeout(soft=True, timeout=1336)
            self.assertEqual(mytask.backend.get_status(tw.task_id),
                             states.PENDING)

    def test_execute_and_trace(self):
        res = execute_and_trace(mytask.name, uuid(), [4], {})
        self.assertEqual(res, 4 ** 4)

    def test_execute_safe_catches_exception(self):
        old_exec = WorkerTaskTrace.execute

        def _error_exec(self, *args, **kwargs):
            raise KeyError("baz")

        WorkerTaskTrace.execute = _error_exec
        try:
            with catch_warnings(record=True) as log:
                res = execute_and_trace(mytask.name, uuid(),
                                        [4], {})
                self.assertIsInstance(res, ExceptionInfo)
                self.assertTrue(log)
                self.assertIn("Exception outside", log[0].message.args[0])
                self.assertIn("KeyError", log[0].message.args[0])
        finally:
            WorkerTaskTrace.execute = old_exec

    def create_exception(self, exc):
        try:
            raise exc
        except exc.__class__:
            return sys.exc_info()

    def test_worker_task_trace_handle_retry(self):
        from celery.exceptions import RetryTaskError
        tid = uuid()
        w = WorkerTaskTrace(mytask.name, tid, [4], {})
        type_, value_, tb_ = self.create_exception(ValueError("foo"))
        type_, value_, tb_ = self.create_exception(RetryTaskError(str(value_),
                                                                  exc=value_))
        w._store_errors = False
        w.handle_retry(value_, type_, tb_, "")
        self.assertEqual(mytask.backend.get_status(tid), states.PENDING)
        w._store_errors = True
        w.handle_retry(value_, type_, tb_, "")
        self.assertEqual(mytask.backend.get_status(tid), states.RETRY)

    def test_worker_task_trace_handle_failure(self):
        tid = uuid()
        w = WorkerTaskTrace(mytask.name, tid, [4], {})
        type_, value_, tb_ = self.create_exception(ValueError("foo"))
        w._store_errors = False
        w.handle_failure(value_, type_, tb_, "")
        self.assertEqual(mytask.backend.get_status(tid), states.PENDING)
        w._store_errors = True
        w.handle_failure(value_, type_, tb_, "")
        self.assertEqual(mytask.backend.get_status(tid), states.FAILURE)

    def test_task_wrapper_mail_attrs(self):
        tw = TaskRequest(mytask.name, uuid(), [], {})
        x = tw.success_msg % {"name": tw.task_name,
                              "id": tw.task_id,
                              "return_value": 10,
                              "runtime": 0.3641}
        self.assertTrue(x)
        x = tw.error_msg % {"name": tw.task_name,
                           "id": tw.task_id,
                           "exc": "FOOBARBAZ",
                           "traceback": "foobarbaz"}
        self.assertTrue(x)

    def test_from_message(self):
        us = u"æØåveéðƒeæ"
        body = {"task": mytask.name, "id": uuid(),
                "args": [2], "kwargs": {us: "bar"}}
        m = Message(None, body=anyjson.serialize(body), backend="foo",
                          content_type="application/json",
                          content_encoding="utf-8")
        tw = TaskRequest.from_message(m, m.decode())
        self.assertIsInstance(tw, TaskRequest)
        self.assertEqual(tw.task_name, body["task"])
        self.assertEqual(tw.task_id, body["id"])
        self.assertEqual(tw.args, body["args"])
        us = from_utf8(us)
        self.assertEqual(tw.kwargs.keys()[0], us)
        self.assertIsInstance(tw.kwargs.keys()[0], str)
        self.assertTrue(tw.logger)

    def test_from_message_empty_args(self):
        body = {"task": mytask.name, "id": uuid()}
        m = Message(None, body=anyjson.serialize(body), backend="foo",
                          content_type="application/json",
                          content_encoding="utf-8")
        tw = TaskRequest.from_message(m, m.decode())
        self.assertIsInstance(tw, TaskRequest)
        self.assertEquals(tw.args, [])
        self.assertEquals(tw.kwargs, {})

    def test_from_message_missing_required_fields(self):
        body = {}
        m = Message(None, body=anyjson.serialize(body), backend="foo",
                          content_type="application/json",
                          content_encoding="utf-8")
        with self.assertRaises(InvalidTaskError):
            TaskRequest.from_message(m, m.decode())

    def test_from_message_nonexistant_task(self):
        body = {"task": "cu.mytask.doesnotexist", "id": uuid(),
                "args": [2], "kwargs": {u"æØåveéðƒeæ": "bar"}}
        m = Message(None, body=anyjson.serialize(body), backend="foo",
                          content_type="application/json",
                          content_encoding="utf-8")
        with self.assertRaises(NotRegistered):
            TaskRequest.from_message(m, m.decode())

    def test_execute(self):
        tid = uuid()
        tw = TaskRequest(mytask.name, tid, [4], {"f": "x"})
        self.assertEqual(tw.execute(), 256)
        meta = mytask.backend.get_task_meta(tid)
        self.assertEqual(meta["result"], 256)
        self.assertEqual(meta["status"], states.SUCCESS)

    def test_execute_success_no_kwargs(self):
        tid = uuid()
        tw = TaskRequest(mytask_no_kwargs.name, tid, [4], {})
        self.assertEqual(tw.execute(), 256)
        meta = mytask_no_kwargs.backend.get_task_meta(tid)
        self.assertEqual(meta["result"], 256)
        self.assertEqual(meta["status"], states.SUCCESS)

    def test_execute_success_some_kwargs(self):
        tid = uuid()
        tw = TaskRequest(mytask_some_kwargs.name, tid, [4], {})
        self.assertEqual(tw.execute(logfile="foobaz.log"), 256)
        meta = mytask_some_kwargs.backend.get_task_meta(tid)
        self.assertEqual(some_kwargs_scratchpad.get("logfile"), "foobaz.log")
        self.assertEqual(meta["result"], 256)
        self.assertEqual(meta["status"], states.SUCCESS)

    def test_execute_ack(self):
        tid = uuid()
        tw = TaskRequest(mytask.name, tid, [4], {"f": "x"},
                        on_ack=on_ack)
        self.assertEqual(tw.execute(), 256)
        meta = mytask.backend.get_task_meta(tid)
        self.assertTrue(scratch["ACK"])
        self.assertEqual(meta["result"], 256)
        self.assertEqual(meta["status"], states.SUCCESS)

    def test_execute_fail(self):
        tid = uuid()
        tw = TaskRequest(mytask_raising.name, tid, [4], {"f": "x"})
        self.assertIsInstance(tw.execute(), ExceptionInfo)
        meta = mytask_raising.backend.get_task_meta(tid)
        self.assertEqual(meta["status"], states.FAILURE)
        self.assertIsInstance(meta["result"], KeyError)

    def test_execute_using_pool(self):
        tid = uuid()
        tw = TaskRequest(mytask.name, tid, [4], {"f": "x"})

        class MockPool(BasePool):
            target = None
            args = None
            kwargs = None

            def __init__(self, *args, **kwargs):
                pass

            def apply_async(self, target, args=None, kwargs=None,
                    *margs, **mkwargs):
                self.target = target
                self.args = args
                self.kwargs = kwargs

        p = MockPool()
        tw.execute_using_pool(p)
        self.assertTrue(p.target)
        self.assertEqual(p.args[0], mytask.name)
        self.assertEqual(p.args[1], tid)
        self.assertEqual(p.args[2], [4])
        self.assertIn("f", p.args[3])
        self.assertIn([4], p.args)

    def test_default_kwargs(self):
        tid = uuid()
        tw = TaskRequest(mytask.name, tid, [4], {"f": "x"})
        self.assertDictEqual(
                tw.extend_with_default_kwargs(10, "some_logfile"), {
                    "f": "x",
                    "logfile": "some_logfile",
                    "loglevel": 10,
                    "task_id": tw.task_id,
                    "task_retries": 0,
                    "task_is_eager": False,
                    "delivery_info": {},
                    "task_name": tw.task_name})

    def _test_on_failure(self, exception):
        app = app_or_default()
        tid = uuid()
        tw = TaskRequest(mytask.name, tid, [4], {"f": "x"})
        try:
            raise exception
        except Exception:
            exc_info = ExceptionInfo(sys.exc_info())

            logfh = WhateverIO()
            tw.logger.handlers = []
            tw.logger = setup_logger(logfile=logfh, loglevel=logging.INFO,
                                     root=False)

            app.conf.CELERY_SEND_TASK_ERROR_EMAILS = True

            tw.on_failure(exc_info)
            logvalue = logfh.getvalue()
            self.assertIn(mytask.name, logvalue)
            self.assertIn(tid, logvalue)
            self.assertIn("ERROR", logvalue)

            app.conf.CELERY_SEND_TASK_ERROR_EMAILS = False

    def test_on_failure(self):
        self._test_on_failure(Exception("Inside unit tests"))

    def test_on_failure_unicode_exception(self):
        self._test_on_failure(Exception(u"Бобры атакуют"))

    def test_on_failure_utf8_exception(self):
        self._test_on_failure(Exception(
            from_utf8(u"Бобры атакуют")))
