# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import numbers
import os
import signal
import socket
import sys

from datetime import datetime, timedelta

from billiard.einfo import ExceptionInfo
from kombu.utils import uuid
from kombu.utils.encoding import default_encode, from_utf8, safe_str, safe_repr

from celery import states
from celery.app.trace import (
    trace_task,
    _trace_task_ret,
    TraceInfo,
    mro_lookup,
    build_tracer,
    setup_worker_optimizations,
    reset_worker_optimizations,
)
from celery.concurrency.base import BasePool
from celery.exceptions import (
    Ignore,
    InvalidTaskError,
    Reject,
    Retry,
    TaskRevokedError,
    Terminated,
    WorkerLostError,
)
from celery.five import monotonic
from celery.signals import task_revoked
from celery.worker import request as module
from celery.worker.request import (
    Request, create_request_cls, logger as req_logger,
)
from celery.worker.state import revoked

from celery.tests.case import (
    AppCase,
    Case,
    Mock,
    TaskMessage,
    task_message_from_sig,
    patch,
    skip,
)


class RequestCase(AppCase):

    def setup(self):
        self.app.conf.result_serializer = 'pickle'

        @self.app.task(shared=False)
        def add(x, y, **kw_):
            return x + y
        self.add = add

        @self.app.task(shared=False)
        def mytask(i, **kwargs):
            return i ** i
        self.mytask = mytask

        @self.app.task(shared=False)
        def mytask_raising(i):
            raise KeyError(i)
        self.mytask_raising = mytask_raising

    def xRequest(self, name=None, id=None, args=None, kwargs=None,
                 on_ack=None, on_reject=None, Request=Request, **head):
        args = [1] if args is None else args
        kwargs = {'f': 'x'} if kwargs is None else kwargs
        on_ack = on_ack or Mock(name='on_ack')
        on_reject = on_reject or Mock(name='on_reject')
        message = TaskMessage(
            name or self.mytask.name, id, args=args, kwargs=kwargs, **head
        )
        return Request(message, app=self.app,
                       on_ack=on_ack, on_reject=on_reject)


class test_mro_lookup(Case):

    def test_order(self):

        class A(object):
            pass

        class B(A):
            pass

        class C(B):
            pass

        class D(C):

            @classmethod
            def mro(cls):
                return ()

        A.x = 10
        self.assertEqual(mro_lookup(C, 'x'), A)
        self.assertIsNone(mro_lookup(C, 'x', stop={A}))
        B.x = 10
        self.assertEqual(mro_lookup(C, 'x'), B)
        C.x = 10
        self.assertEqual(mro_lookup(C, 'x'), C)
        self.assertIsNone(mro_lookup(D, 'x'))


def jail(app, task_id, name, args, kwargs):
    request = {'id': task_id}
    task = app.tasks[name]
    task.__trace__ = None  # rebuild
    return trace_task(
        task, task_id, args, kwargs, request=request, eager=False, app=app,
    ).retval


@skip.if_python3()
class test_default_encode(AppCase):

    def test_jython(self):
        prev, sys.platform = sys.platform, 'java 1.6.1'
        try:
            self.assertEqual(default_encode(bytes('foo')), 'foo')
        finally:
            sys.platform = prev

    def test_cpython(self):
        prev, sys.platform = sys.platform, 'darwin'
        gfe, sys.getfilesystemencoding = (
            sys.getfilesystemencoding,
            lambda: 'utf-8',
        )
        try:
            self.assertEqual(default_encode(bytes('foo')), 'foo')
        finally:
            sys.platform = prev
            sys.getfilesystemencoding = gfe


class test_Retry(AppCase):

    def test_retry_semipredicate(self):
        try:
            raise Exception('foo')
        except Exception as exc:
            ret = Retry('Retrying task', exc)
            self.assertEqual(ret.exc, exc)


class test_trace_task(RequestCase):

    def setup(self):

        @self.app.task(shared=False)
        def mytask(i, **kwargs):
            return i ** i
        self.mytask = mytask

        @self.app.task(shared=False)
        def mytask_raising(i):
            raise KeyError(i)
        self.mytask_raising = mytask_raising

    @patch('celery.app.trace.logger')
    def test_process_cleanup_fails(self, _logger):
        self.mytask.backend = Mock()
        self.mytask.backend.process_cleanup = Mock(side_effect=KeyError())
        tid = uuid()
        ret = jail(self.app, tid, self.mytask.name, [2], {})
        self.assertEqual(ret, 4)
        self.mytask.backend.mark_as_done.assert_called()
        self.assertIn('Process cleanup failed', _logger.error.call_args[0][0])

    def test_process_cleanup_BaseException(self):
        self.mytask.backend = Mock()
        self.mytask.backend.process_cleanup = Mock(side_effect=SystemExit())
        with self.assertRaises(SystemExit):
            jail(self.app, uuid(), self.mytask.name, [2], {})

    def test_execute_jail_success(self):
        ret = jail(self.app, uuid(), self.mytask.name, [2], {})
        self.assertEqual(ret, 4)

    def test_marked_as_started(self):
        _started = []

        def store_result(tid, meta, state, **kwargs):
            if state == states.STARTED:
                _started.append(tid)
        self.mytask.backend.store_result = Mock(name='store_result')
        self.mytask.backend.store_result.side_effect = store_result
        self.mytask.track_started = True

        tid = uuid()
        jail(self.app, tid, self.mytask.name, [2], {})
        self.assertIn(tid, _started)

        self.mytask.ignore_result = True
        tid = uuid()
        jail(self.app, tid, self.mytask.name, [2], {})
        self.assertNotIn(tid, _started)

    def test_execute_jail_failure(self):
        ret = jail(
            self.app, uuid(), self.mytask_raising.name, [4], {},
        )
        self.assertIsInstance(ret, ExceptionInfo)
        self.assertTupleEqual(ret.exception.args, (4,))

    def test_execute_ignore_result(self):

        @self.app.task(shared=False, ignore_result=True)
        def ignores_result(i):
            return i ** i

        task_id = uuid()
        ret = jail(self.app, task_id, ignores_result.name, [4], {})
        self.assertEqual(ret, 256)
        self.assertFalse(self.app.AsyncResult(task_id).ready())


class test_Request(RequestCase):

    def get_request(self, sig, Request=Request, **kwargs):
        return Request(
            task_message_from_sig(self.app, sig),
            on_ack=Mock(name='on_ack'),
            on_reject=Mock(name='on_reject'),
            eventer=Mock(name='eventer'),
            app=self.app,
            connection_errors=(socket.error,),
            task=sig.type,
            **kwargs
        )

    def test_shadow(self):
        self.assertEqual(
            self.get_request(self.add.s(2, 2).set(shadow='fooxyz')).name,
            'fooxyz',
        )

    def test_invalid_eta_raises_InvalidTaskError(self):
        with self.assertRaises(InvalidTaskError):
            self.get_request(self.add.s(2, 2).set(eta='12345'))

    def test_invalid_expires_raises_InvalidTaskError(self):
        with self.assertRaises(InvalidTaskError):
            self.get_request(self.add.s(2, 2).set(expires='12345'))

    def test_valid_expires_with_utc_makes_aware(self):
        with patch('celery.worker.request.maybe_make_aware') as mma:
            self.get_request(self.add.s(2, 2).set(expires=10),
                             maybe_make_aware=mma)
            mma.assert_called()

    def test_maybe_expire_when_expires_is_None(self):
        req = self.get_request(self.add.s(2, 2))
        self.assertFalse(req.maybe_expire())

    def test_on_retry_acks_if_late(self):
        self.add.acks_late = True
        req = self.get_request(self.add.s(2, 2))
        req.on_retry(Mock())
        req.on_ack.assert_called_with(req_logger, req.connection_errors)

    def test_on_failure_Termianted(self):
        einfo = None
        try:
            raise Terminated('9')
        except Terminated:
            einfo = ExceptionInfo()
        self.assertIsNotNone(einfo)
        req = self.get_request(self.add.s(2, 2))
        req.on_failure(einfo)
        req.eventer.send.assert_called_with(
            'task-revoked',
            uuid=req.id, terminated=True, signum='9', expired=False,
        )

    def test_on_failure_propagates_MemoryError(self):
        einfo = None
        try:
            raise MemoryError()
        except MemoryError:
            einfo = ExceptionInfo(internal=True)
        self.assertIsNotNone(einfo)
        req = self.get_request(self.add.s(2, 2))
        with self.assertRaises(MemoryError):
            req.on_failure(einfo)

    def test_on_failure_Ignore_acknowledges(self):
        einfo = None
        try:
            raise Ignore()
        except Ignore:
            einfo = ExceptionInfo(internal=True)
        self.assertIsNotNone(einfo)
        req = self.get_request(self.add.s(2, 2))
        req.on_failure(einfo)
        req.on_ack.assert_called_with(req_logger, req.connection_errors)

    def test_on_failure_Reject_rejects(self):
        einfo = None
        try:
            raise Reject()
        except Reject:
            einfo = ExceptionInfo(internal=True)
        self.assertIsNotNone(einfo)
        req = self.get_request(self.add.s(2, 2))
        req.on_failure(einfo)
        req.on_reject.assert_called_with(
            req_logger, req.connection_errors, False,
        )

    def test_on_failure_Reject_rejects_with_requeue(self):
        einfo = None
        try:
            raise Reject(requeue=True)
        except Reject:
            einfo = ExceptionInfo(internal=True)
        self.assertIsNotNone(einfo)
        req = self.get_request(self.add.s(2, 2))
        req.on_failure(einfo)
        req.on_reject.assert_called_with(
            req_logger, req.connection_errors, True,
        )

    def test_on_failure_WorkerLostError_rejects_with_requeue(self):
        einfo = None
        try:
            raise WorkerLostError()
        except:
            einfo = ExceptionInfo(internal=True)
        req = self.get_request(self.add.s(2, 2))
        req.task.acks_late = True
        req.task.reject_on_worker_lost = True
        req.delivery_info['redelivered'] = False
        req.on_failure(einfo)
        req.on_reject.assert_called_with(
            req_logger, req.connection_errors, True)

    def test_on_failure_WorkerLostError_redelivered_None(self):
        einfo = None
        try:
            raise WorkerLostError()
        except:
            einfo = ExceptionInfo(internal=True)
        req = self.get_request(self.add.s(2, 2))
        req.task.acks_late = True
        req.task.reject_on_worker_lost = True
        req.delivery_info['redelivered'] = None
        req.on_failure(einfo)
        req.on_reject.assert_called_with(
            req_logger, req.connection_errors, False)

    def test_tzlocal_is_cached(self):
        req = self.get_request(self.add.s(2, 2))
        req._tzlocal = 'foo'
        self.assertEqual(req.tzlocal, 'foo')

    def test_task_wrapper_repr(self):
        self.assertTrue(repr(self.xRequest()))

    def test_sets_store_errors(self):
        self.mytask.ignore_result = True
        job = self.xRequest()
        self.assertFalse(job.store_errors)

        self.mytask.store_errors_even_if_ignored = True
        job = self.xRequest()
        self.assertTrue(job.store_errors)

    def test_send_event(self):
        job = self.xRequest()
        job.eventer = Mock(name='.eventer')
        job.send_event('task-frobulated')
        job.eventer.send.assert_called_with('task-frobulated', uuid=job.id)

    def test_on_retry(self):
        job = self.get_request(self.mytask.s(1, f='x'))
        job.eventer = Mock(name='.eventer')
        try:
            raise Retry('foo', KeyError('moofoobar'))
        except:
            einfo = ExceptionInfo()
            job.on_failure(einfo)
            job.eventer.send.assert_called_with(
                'task-retried',
                uuid=job.id,
                exception=safe_repr(einfo.exception.exc),
                traceback=safe_str(einfo.traceback),
            )
            prev, module._does_info = module._does_info, False
            try:
                job.on_failure(einfo)
            finally:
                module._does_info = prev
            einfo.internal = True
            job.on_failure(einfo)

    def test_compat_properties(self):
        job = self.xRequest()
        self.assertEqual(job.task_id, job.id)
        self.assertEqual(job.task_name, job.name)
        job.task_id = 'ID'
        self.assertEqual(job.id, 'ID')
        job.task_name = 'NAME'
        self.assertEqual(job.name, 'NAME')

    def test_terminate__pool_ref(self):
        pool = Mock()
        signum = signal.SIGTERM
        job = self.get_request(self.mytask.s(1, f='x'))
        job._apply_result = Mock(name='_apply_result')
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job,
                terminated=True, expired=False, signum=signum):
            job.time_start = monotonic()
            job.worker_pid = 314
            job.terminate(pool, signal='TERM')
            job._apply_result().terminate.assert_called_with(signum)

            job._apply_result = Mock(name='_apply_result2')
            job._apply_result.return_value = None
            job.terminate(pool, signal='TERM')

    def test_terminate__task_started(self):
        pool = Mock()
        signum = signal.SIGTERM
        job = self.get_request(self.mytask.s(1, f='x'))
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job,
                terminated=True, expired=False, signum=signum):
            job.time_start = monotonic()
            job.worker_pid = 313
            job.terminate(pool, signal='TERM')
            pool.terminate_job.assert_called_with(job.worker_pid, signum)

    def test_terminate__task_reserved(self):
        pool = Mock()
        job = self.get_request(self.mytask.s(1, f='x'))
        job.time_start = None
        job.terminate(pool, signal='TERM')
        pool.terminate_job.assert_not_called()
        self.assertTupleEqual(job._terminate_on_ack, (pool, 15))
        job.terminate(pool, signal='TERM')

    def test_revoked_expires_expired(self):
        job = self.get_request(self.mytask.s(1, f='x').set(
            expires=datetime.utcnow() - timedelta(days=1)
        ))
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job,
                terminated=False, expired=True, signum=None):
            job.revoked()
            self.assertIn(job.id, revoked)
            self.assertEqual(
                self.mytask.backend.get_status(job.id),
                states.REVOKED,
            )

    def test_revoked_expires_not_expired(self):
        job = self.xRequest(
            expires=datetime.utcnow() + timedelta(days=1),
        )
        job.revoked()
        self.assertNotIn(job.id, revoked)
        self.assertNotEqual(
            self.mytask.backend.get_status(job.id),
            states.REVOKED,
        )

    def test_revoked_expires_ignore_result(self):
        self.mytask.ignore_result = True
        job = self.xRequest(
            expires=datetime.utcnow() - timedelta(days=1),
        )
        job.revoked()
        self.assertIn(job.id, revoked)
        self.assertNotEqual(
            self.mytask.backend.get_status(job.id), states.REVOKED,
        )

    def test_already_revoked(self):
        job = self.xRequest()
        job._already_revoked = True
        self.assertTrue(job.revoked())

    def test_revoked(self):
        job = self.xRequest()
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job,
                terminated=False, expired=False, signum=None):
            revoked.add(job.id)
            self.assertTrue(job.revoked())
            self.assertTrue(job._already_revoked)
            self.assertTrue(job.acknowledged)

    def test_execute_does_not_execute_revoked(self):
        job = self.xRequest()
        revoked.add(job.id)
        job.execute()

    def test_execute_acks_late(self):
        self.mytask_raising.acks_late = True
        job = self.xRequest(
            name=self.mytask_raising.name,
            kwargs={},
        )
        job.execute()
        self.assertTrue(job.acknowledged)
        job.execute()

    def test_execute_using_pool_does_not_execute_revoked(self):
        job = self.xRequest()
        revoked.add(job.id)
        with self.assertRaises(TaskRevokedError):
            job.execute_using_pool(None)

    def test_on_accepted_acks_early(self):
        job = self.xRequest()
        job.on_accepted(pid=os.getpid(), time_accepted=monotonic())
        self.assertTrue(job.acknowledged)
        prev, module._does_debug = module._does_debug, False
        try:
            job.on_accepted(pid=os.getpid(), time_accepted=monotonic())
        finally:
            module._does_debug = prev

    def test_on_accepted_acks_late(self):
        job = self.xRequest()
        self.mytask.acks_late = True
        job.on_accepted(pid=os.getpid(), time_accepted=monotonic())
        self.assertFalse(job.acknowledged)

    def test_on_accepted_terminates(self):
        signum = signal.SIGTERM
        pool = Mock()
        job = self.xRequest()
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job,
                terminated=True, expired=False, signum=signum):
            job.terminate(pool, signal='TERM')
            self.assertFalse(pool.terminate_job.call_count)
            job.on_accepted(pid=314, time_accepted=monotonic())
            pool.terminate_job.assert_called_with(314, signum)

    def test_on_success_acks_early(self):
        job = self.xRequest()
        job.time_start = 1
        job.on_success((0, 42, 0.001))
        prev, module._does_info = module._does_info, False
        try:
            job.on_success((0, 42, 0.001))
            self.assertFalse(job.acknowledged)
        finally:
            module._does_info = prev

    def test_on_success_BaseException(self):
        job = self.xRequest()
        job.time_start = 1
        with self.assertRaises(SystemExit):
            try:
                raise SystemExit()
            except SystemExit:
                job.on_success((1, ExceptionInfo(), 0.01))
            else:
                assert False

    def test_on_success_eventer(self):
        job = self.xRequest()
        job.time_start = 1
        job.eventer = Mock()
        job.eventer.send = Mock()
        job.on_success((0, 42, 0.001))
        job.eventer.send.assert_called()

    def test_on_success_when_failure(self):
        job = self.xRequest()
        job.time_start = 1
        job.on_failure = Mock()
        try:
            raise KeyError('foo')
        except Exception:
            job.on_success((1, ExceptionInfo(), 0.001))
            job.on_failure.assert_called()

    def test_on_success_acks_late(self):
        job = self.xRequest()
        job.time_start = 1
        self.mytask.acks_late = True
        job.on_success((0, 42, 0.001))
        self.assertTrue(job.acknowledged)

    def test_on_failure_WorkerLostError(self):

        def get_ei():
            try:
                raise WorkerLostError('do re mi')
            except WorkerLostError:
                return ExceptionInfo()

        job = self.xRequest()
        exc_info = get_ei()
        job.on_failure(exc_info)
        self.assertEqual(
            self.mytask.backend.get_status(job.id), states.FAILURE,
        )

        self.mytask.ignore_result = True
        exc_info = get_ei()
        job = self.xRequest()
        job.on_failure(exc_info)
        self.assertEqual(
            self.mytask.backend.get_status(job.id), states.PENDING,
        )

    def test_on_failure_acks_late(self):
        job = self.xRequest()
        job.time_start = 1
        self.mytask.acks_late = True
        try:
            raise KeyError('foo')
        except KeyError:
            exc_info = ExceptionInfo()
            job.on_failure(exc_info)
            self.assertTrue(job.acknowledged)

    def test_from_message_invalid_kwargs(self):
        m = TaskMessage(self.mytask.name, args=(), kwargs='foo')
        req = Request(m, app=self.app)
        with self.assertRaises(InvalidTaskError):
            raise req.execute().exception

    @patch('celery.worker.request.error')
    @patch('celery.worker.request.warn')
    def test_on_timeout(self, warn, error):

        job = self.xRequest()
        job.acknowledge = Mock(name='ack')
        job.task.acks_late = True
        job.on_timeout(soft=True, timeout=1337)
        self.assertIn('Soft time limit', warn.call_args[0][0])
        job.on_timeout(soft=False, timeout=1337)
        self.assertIn('Hard time limit', error.call_args[0][0])
        self.assertEqual(
            self.mytask.backend.get_status(job.id), states.FAILURE,
        )
        job.acknowledge.assert_called_with()

        self.mytask.ignore_result = True
        job = self.xRequest()
        job.on_timeout(soft=True, timeout=1336)
        self.assertEqual(
            self.mytask.backend.get_status(job.id), states.PENDING,
        )

        job = self.xRequest()
        job.acknowledge = Mock(name='ack')
        job.task.acks_late = False
        job.on_timeout(soft=True, timeout=1335)
        job.acknowledge.assert_not_called()

    def test_fast_trace_task(self):
        from celery.app import trace
        setup_worker_optimizations(self.app)
        self.assertIs(trace.trace_task_ret, trace._fast_trace_task)
        tid = uuid()
        message = TaskMessage(self.mytask.name, tid, args=[4])
        assert len(message.payload) == 3
        try:
            self.mytask.__trace__ = build_tracer(
                self.mytask.name, self.mytask, self.app.loader, 'test',
                app=self.app,
            )
            failed, res, runtime = trace.trace_task_ret(
                self.mytask.name, tid, message.headers, message.body,
                message.content_type, message.content_encoding)
            self.assertFalse(failed)
            self.assertEqual(res, repr(4 ** 4))
            self.assertIsNotNone(runtime)
            self.assertIsInstance(runtime, numbers.Real)
        finally:
            reset_worker_optimizations()
            self.assertIs(trace.trace_task_ret, trace._trace_task_ret)
        delattr(self.mytask, '__trace__')
        failed, res, runtime = trace.trace_task_ret(
            self.mytask.name, tid, message.headers, message.body,
            message.content_type, message.content_encoding, app=self.app,
        )
        self.assertFalse(failed)
        self.assertEqual(res, repr(4 ** 4))
        self.assertIsNotNone(runtime)
        self.assertIsInstance(runtime, numbers.Real)

    def test_trace_task_ret(self):
        self.mytask.__trace__ = build_tracer(
            self.mytask.name, self.mytask, self.app.loader, 'test',
            app=self.app,
        )
        tid = uuid()
        message = TaskMessage(self.mytask.name, tid, args=[4])
        _, R, _ = _trace_task_ret(
            self.mytask.name, tid, message.headers,
            message.body, message.content_type,
            message.content_encoding, app=self.app,
        )
        self.assertEqual(R, repr(4 ** 4))

    def test_trace_task_ret__no_trace(self):
        try:
            delattr(self.mytask, '__trace__')
        except AttributeError:
            pass
        tid = uuid()
        message = TaskMessage(self.mytask.name, tid, args=[4])
        _, R, _ = _trace_task_ret(
            self.mytask.name, tid, message.headers,
            message.body, message.content_type,
            message.content_encoding, app=self.app,
        )
        self.assertEqual(R, repr(4 ** 4))

    def test_trace_catches_exception(self):

        @self.app.task(request=None, shared=False)
        def raising():
            raise KeyError('baz')

        with self.assertWarnsRegex(RuntimeWarning,
                                   r'Exception raised outside'):
            res = trace_task(raising, uuid(), [], {}, app=self.app)[0]
            self.assertIsInstance(res, ExceptionInfo)

    def test_worker_task_trace_handle_retry(self):
        tid = uuid()
        self.mytask.push_request(id=tid)
        try:
            raise ValueError('foo')
        except Exception as exc:
            try:
                raise Retry(str(exc), exc=exc)
            except Retry as exc:
                w = TraceInfo(states.RETRY, exc)
                w.handle_retry(
                    self.mytask, self.mytask.request, store_errors=False,
                )
                self.assertEqual(
                    self.mytask.backend.get_status(tid), states.PENDING,
                )
                w.handle_retry(
                    self.mytask, self.mytask.request, store_errors=True,
                )
                self.assertEqual(
                    self.mytask.backend.get_status(tid), states.RETRY,
                )
        finally:
            self.mytask.pop_request()

    def test_worker_task_trace_handle_failure(self):
        tid = uuid()
        self.mytask.push_request()
        try:
            self.mytask.request.id = tid
            try:
                raise ValueError('foo')
            except Exception as exc:
                w = TraceInfo(states.FAILURE, exc)
                w.handle_failure(
                    self.mytask, self.mytask.request, store_errors=False,
                )
                self.assertEqual(
                    self.mytask.backend.get_status(tid), states.PENDING,
                )
                w.handle_failure(
                    self.mytask, self.mytask.request, store_errors=True,
                )
                self.assertEqual(
                    self.mytask.backend.get_status(tid), states.FAILURE,
                )
        finally:
            self.mytask.pop_request()

    def test_from_message(self):
        us = 'æØåveéðƒeæ'
        tid = uuid()
        m = TaskMessage(self.mytask.name, tid, args=[2], kwargs={us: 'bar'})
        job = Request(m, app=self.app)
        self.assertIsInstance(job, Request)
        self.assertEqual(job.name, self.mytask.name)
        self.assertEqual(job.id, tid)
        self.assertIs(job.message, m)

    def test_from_message_empty_args(self):
        tid = uuid()
        m = TaskMessage(self.mytask.name, tid, args=[], kwargs={})
        job = Request(m, app=self.app)
        self.assertIsInstance(job, Request)

    def test_from_message_missing_required_fields(self):
        m = TaskMessage(self.mytask.name)
        m.headers.clear()
        with self.assertRaises(KeyError):
            Request(m, app=self.app)

    def test_from_message_nonexistant_task(self):
        m = TaskMessage(
            'cu.mytask.doesnotexist',
            args=[2], kwargs={'æØåveéðƒeæ': 'bar'},
        )
        with self.assertRaises(KeyError):
            Request(m, app=self.app)

    def test_execute(self):
        tid = uuid()
        job = self.xRequest(id=tid, args=[4], kwargs={})
        self.assertEqual(job.execute(), 256)
        meta = self.mytask.backend.get_task_meta(tid)
        self.assertEqual(meta['status'], states.SUCCESS)
        self.assertEqual(meta['result'], 256)

    def test_execute_success_no_kwargs(self):

        @self.app.task  # traverses coverage for decorator without parens
        def mytask_no_kwargs(i):
            return i ** i

        tid = uuid()
        job = self.xRequest(
            name=mytask_no_kwargs.name,
            id=tid,
            args=[4],
            kwargs={},
        )
        self.assertEqual(job.execute(), 256)
        meta = mytask_no_kwargs.backend.get_task_meta(tid)
        self.assertEqual(meta['result'], 256)
        self.assertEqual(meta['status'], states.SUCCESS)

    def test_execute_ack(self):
        scratch = {'ACK': False}

        def on_ack(*args, **kwargs):
            scratch['ACK'] = True

        tid = uuid()
        job = self.xRequest(id=tid, args=[4], on_ack=on_ack)
        self.assertEqual(job.execute(), 256)
        meta = self.mytask.backend.get_task_meta(tid)
        self.assertTrue(scratch['ACK'])
        self.assertEqual(meta['result'], 256)
        self.assertEqual(meta['status'], states.SUCCESS)

    def test_execute_fail(self):
        tid = uuid()
        job = self.xRequest(
            name=self.mytask_raising.name,
            id=tid,
            args=[4],
            kwargs={},
        )
        self.assertIsInstance(job.execute(), ExceptionInfo)
        assert self.mytask_raising.backend.serializer == 'pickle'
        meta = self.mytask_raising.backend.get_task_meta(tid)
        self.assertEqual(meta['status'], states.FAILURE)
        self.assertIsInstance(meta['result'], KeyError)

    def test_execute_using_pool(self):
        tid = uuid()
        job = self.xRequest(id=tid, args=[4])

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
        job.execute_using_pool(p)
        self.assertTrue(p.target)
        self.assertEqual(p.args[0], self.mytask.name)
        self.assertEqual(p.args[1], tid)
        self.assertEqual(p.args[3], job.message.body)

    def _test_on_failure(self, exception, **kwargs):
        tid = uuid()
        job = self.xRequest(id=tid, args=[4])
        job.send_event = Mock(name='send_event')
        job.task.backend.mark_as_failure = Mock(name='mark_as_failure')
        try:
            raise exception
        except type(exception):
            exc_info = ExceptionInfo()
            job.on_failure(exc_info, **kwargs)
            job.send_event.assert_called()
        return job

    def test_on_failure(self):
        self._test_on_failure(Exception('Inside unit tests'))

    def test_on_failure__unicode_exception(self):
        self._test_on_failure(Exception('Бобры атакуют'))

    def test_on_failure__utf8_exception(self):
        self._test_on_failure(Exception(
            from_utf8('Бобры атакуют')))

    def test_on_failure__WorkerLostError(self):
        exc = WorkerLostError()
        job = self._test_on_failure(exc)
        job.task.backend.mark_as_failure.assert_called_with(
            job.id, exc, request=job, store_result=True,
        )

    def test_on_failure__return_ok(self):
        self._test_on_failure(KeyError(), return_ok=True)

    def test_reject(self):
        job = self.xRequest(id=uuid())
        job.on_reject = Mock(name='on_reject')
        job.reject(requeue=True)
        job.on_reject.assert_called_with(
            req_logger, job.connection_errors, True,
        )
        self.assertTrue(job.acknowledged)
        job.on_reject.reset_mock()
        job.reject(requeue=True)
        job.on_reject.assert_not_called()

    def test_group(self):
        gid = uuid()
        job = self.xRequest(id=uuid(), group=gid)
        self.assertEqual(job.group, gid)


class test_create_request_class(RequestCase):

    def setup(self):
        RequestCase.setup(self)
        self.task = Mock(name='task')
        self.pool = Mock(name='pool')
        self.eventer = Mock(name='eventer')

    def create_request_cls(self, **kwargs):
        return create_request_cls(
            Request, self.task, self.pool, 'foo', self.eventer, **kwargs
        )

    def zRequest(self, Request=None, revoked_tasks=None, ref=None, **kwargs):
        return self.xRequest(
            Request=Request or self.create_request_cls(
                ref=ref,
                revoked_tasks=revoked_tasks,
            ),
            **kwargs)

    def test_on_success(self):
        self.zRequest(id=uuid()).on_success((False, 'hey', 3.1222))

    def test_on_success__SystemExit(self,
                                    errors=(SystemExit, KeyboardInterrupt)):
        for exc in errors:
            einfo = None
            try:
                raise exc()
            except exc:
                einfo = ExceptionInfo()
            with self.assertRaises(exc):
                self.zRequest(id=uuid()).on_success((True, einfo, 1.0))

    def test_on_success__calls_failure(self):
        job = self.zRequest(id=uuid())
        einfo = Mock(name='einfo')
        job.on_failure = Mock(name='on_failure')
        job.on_success((True, einfo, 1.0))
        job.on_failure.assert_called_with(einfo, return_ok=True)

    def test_on_success__acks_late_enabled(self):
        self.task.acks_late = True
        job = self.zRequest(id=uuid())
        job.acknowledge = Mock(name='ack')
        job.on_success((False, 'foo', 1.0))
        job.acknowledge.assert_called_with()

    def test_on_success__acks_late_disabled(self):
        self.task.acks_late = False
        job = self.zRequest(id=uuid())
        job.acknowledge = Mock(name='ack')
        job.on_success((False, 'foo', 1.0))
        job.acknowledge.assert_not_called()

    def test_on_success__no_events(self):
        self.eventer = None
        job = self.zRequest(id=uuid())
        job.send_event = Mock(name='send_event')
        job.on_success((False, 'foo', 1.0))
        job.send_event.assert_not_called()

    def test_on_success__with_events(self):
        job = self.zRequest(id=uuid())
        job.send_event = Mock(name='send_event')
        job.on_success((False, 'foo', 1.0))
        job.send_event.assert_called_with(
            'task-succeeded', result='foo', runtime=1.0,
        )

    def test_execute_using_pool__revoked(self):
        tid = uuid()
        job = self.zRequest(id=tid, revoked_tasks={tid})
        job.revoked = Mock()
        job.revoked.return_value = True
        with self.assertRaises(TaskRevokedError):
            job.execute_using_pool(self.pool)

    def test_execute_using_pool__expired(self):
        tid = uuid()
        job = self.zRequest(id=tid, revoked_tasks=set())
        job.expires = 1232133
        job.revoked = Mock()
        job.revoked.return_value = True
        with self.assertRaises(TaskRevokedError):
            job.execute_using_pool(self.pool)

    def test_execute_using_pool(self):
        from celery.app.trace import trace_task_ret as trace
        weakref_ref = Mock(name='weakref.ref')
        job = self.zRequest(id=uuid(), revoked_tasks=set(), ref=weakref_ref)
        job.execute_using_pool(self.pool)
        self.pool.apply_async.assert_called_with(
            trace,
            args=(job.type, job.id, job.request_dict, job.body,
                  job.content_type, job.content_encoding),
            accept_callback=job.on_accepted,
            timeout_callback=job.on_timeout,
            callback=job.on_success,
            error_callback=job.on_failure,
            soft_timeout=self.task.soft_time_limit,
            timeout=self.task.time_limit,
            correlation_id=job.id,
        )
        self.assertTrue(job._apply_result)
        weakref_ref.assert_called_with(self.pool.apply_async())
        self.assertIs(job._apply_result, weakref_ref())
