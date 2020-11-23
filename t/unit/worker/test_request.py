import numbers
import os
import signal
import socket
from datetime import datetime, timedelta
from time import monotonic, time
from unittest.mock import Mock, patch

import pytest
from billiard.einfo import ExceptionInfo
from kombu.utils.encoding import from_utf8, safe_repr, safe_str
from kombu.utils.uuid import uuid

from celery import states
from celery.app.trace import (TraceInfo, _trace_task_ret, build_tracer,
                              mro_lookup, reset_worker_optimizations,
                              setup_worker_optimizations, trace_task)
from celery.backends.base import BaseDictBackend
from celery.exceptions import (Ignore, InvalidTaskError, Reject, Retry,
                               TaskRevokedError, Terminated, WorkerLostError)
from celery.signals import task_revoked
from celery.worker import request as module
from celery.worker import strategy
from celery.worker.request import Request, create_request_cls
from celery.worker.request import logger as req_logger
from celery.worker.state import revoked


class RequestCase:

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
        message = self.TaskMessage(
            name or self.mytask.name, id, args=args, kwargs=kwargs, **head
        )
        return Request(message, app=self.app,
                       on_ack=on_ack, on_reject=on_reject)


class test_mro_lookup:

    def test_order(self):

        class A:
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
        assert mro_lookup(C, 'x') == A
        assert mro_lookup(C, 'x', stop={A}) is None
        B.x = 10
        assert mro_lookup(C, 'x') == B
        C.x = 10
        assert mro_lookup(C, 'x') == C
        assert mro_lookup(D, 'x') is None


def jail(app, task_id, name, args, kwargs):
    request = {'id': task_id}
    task = app.tasks[name]
    task.__trace__ = None  # rebuild
    return trace_task(
        task, task_id, args, kwargs, request=request, eager=False, app=app,
    ).retval


class test_Retry:

    def test_retry_semipredicate(self):
        try:
            raise Exception('foo')
        except Exception as exc:
            ret = Retry('Retrying task', exc)
            assert ret.exc == exc


class test_trace_task(RequestCase):

    def test_process_cleanup_fails(self, patching):
        _logger = patching('celery.app.trace.logger')
        self.mytask.backend = Mock()
        self.mytask.backend.process_cleanup = Mock(side_effect=KeyError())
        tid = uuid()
        ret = jail(self.app, tid, self.mytask.name, [2], {})
        assert ret == 4
        self.mytask.backend.mark_as_done.assert_called()
        assert 'Process cleanup failed' in _logger.error.call_args[0][0]

    def test_process_cleanup_BaseException(self):
        self.mytask.backend = Mock()
        self.mytask.backend.process_cleanup = Mock(side_effect=SystemExit())
        with pytest.raises(SystemExit):
            jail(self.app, uuid(), self.mytask.name, [2], {})

    def test_execute_jail_success(self):
        ret = jail(self.app, uuid(), self.mytask.name, [2], {})
        assert ret == 4

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
        assert tid in _started

        self.mytask.ignore_result = True
        tid = uuid()
        jail(self.app, tid, self.mytask.name, [2], {})
        assert tid not in _started

    def test_execute_jail_failure(self):
        ret = jail(
            self.app, uuid(), self.mytask_raising.name, [4], {},
        )
        assert isinstance(ret, ExceptionInfo)
        assert ret.exception.args == (4,)

    def test_execute_ignore_result(self):

        @self.app.task(shared=False, ignore_result=True)
        def ignores_result(i):
            return i ** i

        task_id = uuid()
        ret = jail(self.app, task_id, ignores_result.name, [4], {})
        assert ret == 256
        assert not self.app.AsyncResult(task_id).ready()


class test_Request(RequestCase):

    def get_request(self,
                    sig,
                    Request=Request,
                    exclude_headers=None,
                    **kwargs):
        msg = self.task_message_from_sig(self.app, sig)
        headers = None
        if exclude_headers:
            headers = msg.headers
            for header in exclude_headers:
                headers.pop(header)
        return Request(
            msg,
            on_ack=Mock(name='on_ack'),
            on_reject=Mock(name='on_reject'),
            eventer=Mock(name='eventer'),
            app=self.app,
            connection_errors=(socket.error,),
            task=sig.type,
            headers=headers,
            **kwargs
        )

    def test_shadow(self):
        assert self.get_request(
            self.add.s(2, 2).set(shadow='fooxyz')).name == 'fooxyz'

    def test_args(self):
        args = (2, 2)
        assert self.get_request(
            self.add.s(*args)).args == args

    def test_kwargs(self):
        kwargs = {'1': '2', '3': '4'}
        assert self.get_request(
            self.add.s(**kwargs)).kwargs == kwargs

    def test_info_function(self):
        import random
        import string
        kwargs = {}
        for i in range(0, 2):
            kwargs[str(i)] = ''.join(random.choice(string.ascii_lowercase) for i in range(1000))
        assert self.get_request(
            self.add.s(**kwargs)).info(safe=True).get('kwargs') == kwargs
        assert self.get_request(
            self.add.s(**kwargs)).info(safe=False).get('kwargs') == kwargs
        args = []
        for i in range(0, 2):
            args.append(''.join(random.choice(string.ascii_lowercase) for i in range(1000)))
        assert list(self.get_request(
            self.add.s(*args)).info(safe=True).get('args')) == args
        assert list(self.get_request(
            self.add.s(*args)).info(safe=False).get('args')) == args

    def test_no_shadow_header(self):
        request = self.get_request(self.add.s(2, 2),
                                   exclude_headers=['shadow'])
        assert request.name == 't.unit.worker.test_request.add'

    def test_invalid_eta_raises_InvalidTaskError(self):
        with pytest.raises(InvalidTaskError):
            self.get_request(self.add.s(2, 2).set(eta='12345'))

    def test_invalid_expires_raises_InvalidTaskError(self):
        with pytest.raises(InvalidTaskError):
            self.get_request(self.add.s(2, 2).set(expires='12345'))

    def test_valid_expires_with_utc_makes_aware(self):
        with patch('celery.worker.request.maybe_make_aware') as mma:
            self.get_request(self.add.s(2, 2).set(expires=10),
                             maybe_make_aware=mma)
            mma.assert_called()

    def test_maybe_expire_when_expires_is_None(self):
        req = self.get_request(self.add.s(2, 2))
        assert not req.maybe_expire()

    def test_on_retry_acks_if_late(self):
        self.add.acks_late = True
        req = self.get_request(self.add.s(2, 2))
        req.on_retry(Mock())
        req.on_ack.assert_called_with(req_logger, req.connection_errors)

    def test_on_failure_Terminated(self):
        einfo = None
        try:
            raise Terminated('9')
        except Terminated:
            einfo = ExceptionInfo()
        assert einfo is not None
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
        assert einfo is not None
        req = self.get_request(self.add.s(2, 2))
        with pytest.raises(MemoryError):
            req.on_failure(einfo)

    def test_on_failure_Ignore_acknowledges(self):
        einfo = None
        try:
            raise Ignore()
        except Ignore:
            einfo = ExceptionInfo(internal=True)
        assert einfo is not None
        req = self.get_request(self.add.s(2, 2))
        req.on_failure(einfo)
        req.on_ack.assert_called_with(req_logger, req.connection_errors)

    def test_on_failure_Reject_rejects(self):
        einfo = None
        try:
            raise Reject()
        except Reject:
            einfo = ExceptionInfo(internal=True)
        assert einfo is not None
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
        assert einfo is not None
        req = self.get_request(self.add.s(2, 2))
        req.on_failure(einfo)
        req.on_reject.assert_called_with(
            req_logger, req.connection_errors, True,
        )

    def test_on_failure_WorkerLostError_rejects_with_requeue(self):
        einfo = None
        try:
            raise WorkerLostError()
        except WorkerLostError:
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
        except WorkerLostError:
            einfo = ExceptionInfo(internal=True)
        req = self.get_request(self.add.s(2, 2))
        req.task.acks_late = True
        req.task.reject_on_worker_lost = True
        req.delivery_info['redelivered'] = None
        req.on_failure(einfo)
        req.on_reject.assert_called_with(
            req_logger, req.connection_errors, True)

    def test_tzlocal_is_cached(self):
        req = self.get_request(self.add.s(2, 2))
        req._tzlocal = 'foo'
        assert req.tzlocal == 'foo'

    def test_task_wrapper_repr(self):
        assert repr(self.xRequest())

    def test_sets_store_errors(self):
        self.mytask.ignore_result = True
        job = self.xRequest()
        assert not job.store_errors

        self.mytask.store_errors_even_if_ignored = True
        job = self.xRequest()
        assert job.store_errors

    def test_send_event(self):
        job = self.xRequest()
        job.eventer = Mock(name='.eventer')
        job.send_event('task-frobulated')
        job.eventer.send.assert_called_with('task-frobulated', uuid=job.id)

    def test_send_events__disabled_at_task_level(self):
        job = self.xRequest()
        job.task.send_events = False
        job.eventer = Mock(name='.eventer')
        job.send_event('task-frobulated')
        job.eventer.send.assert_not_called()

    def test_on_retry(self):
        job = self.get_request(self.mytask.s(1, f='x'))
        job.eventer = Mock(name='.eventer')
        try:
            raise Retry('foo', KeyError('moofoobar'))
        except Retry:
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
        assert job.task_id == job.id
        assert job.task_name == job.name
        job.task_id = 'ID'
        assert job.id == 'ID'
        job.task_name = 'NAME'
        assert job.name == 'NAME'

    def test_terminate__pool_ref(self):
        pool = Mock()
        signum = signal.SIGTERM
        job = self.get_request(self.mytask.s(1, f='x'))
        job._apply_result = Mock(name='_apply_result')
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job._context,
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
                task_revoked, sender=job.task, request=job._context,
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
        assert job._terminate_on_ack == (pool, 15)
        job.terminate(pool, signal='TERM')

    def test_revoked_expires_expired(self):
        job = self.get_request(self.mytask.s(1, f='x').set(
            expires=datetime.utcnow() - timedelta(days=1)
        ))
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job._context,
                terminated=False, expired=True, signum=None):
            job.revoked()
            assert job.id in revoked
            self.app.set_current()
            assert self.mytask.backend.get_status(job.id) == states.REVOKED

    def test_revoked_expires_not_expired(self):
        job = self.xRequest(
            expires=datetime.utcnow() + timedelta(days=1),
        )
        job.revoked()
        assert job.id not in revoked
        assert self.mytask.backend.get_status(job.id) != states.REVOKED

    def test_revoked_expires_ignore_result(self):
        self.mytask.ignore_result = True
        job = self.xRequest(
            expires=datetime.utcnow() - timedelta(days=1),
        )
        job.revoked()
        assert job.id in revoked
        assert self.mytask.backend.get_status(job.id) != states.REVOKED

    def test_already_revoked(self):
        job = self.xRequest()
        job._already_revoked = True
        assert job.revoked()

    def test_revoked(self):
        job = self.xRequest()
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job._context,
                terminated=False, expired=False, signum=None):
            revoked.add(job.id)
            assert job.revoked()
            assert job._already_revoked
            assert job.acknowledged

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
        assert job.acknowledged
        job.execute()

    def test_execute_using_pool_does_not_execute_revoked(self):
        job = self.xRequest()
        revoked.add(job.id)
        with pytest.raises(TaskRevokedError):
            job.execute_using_pool(None)

    def test_on_accepted_acks_early(self):
        job = self.xRequest()
        job.on_accepted(pid=os.getpid(), time_accepted=monotonic())
        assert job.acknowledged
        prev, module._does_debug = module._does_debug, False
        try:
            job.on_accepted(pid=os.getpid(), time_accepted=monotonic())
        finally:
            module._does_debug = prev

    def test_on_accepted_acks_late(self):
        job = self.xRequest()
        self.mytask.acks_late = True
        job.on_accepted(pid=os.getpid(), time_accepted=monotonic())
        assert not job.acknowledged

    def test_on_accepted_terminates(self):
        signum = signal.SIGTERM
        pool = Mock()
        job = self.xRequest()
        with self.assert_signal_called(
                task_revoked, sender=job.task, request=job._context,
                terminated=True, expired=False, signum=signum):
            job.terminate(pool, signal='TERM')
            assert not pool.terminate_job.call_count
            job.on_accepted(pid=314, time_accepted=monotonic())
            pool.terminate_job.assert_called_with(314, signum)

    def test_on_accepted_time_start(self):
        job = self.xRequest()
        job.on_accepted(pid=os.getpid(), time_accepted=monotonic())
        assert time() - job.time_start < 1

    def test_on_success_acks_early(self):
        job = self.xRequest()
        job.time_start = 1
        job.on_success((0, 42, 0.001))
        prev, module._does_info = module._does_info, False
        try:
            job.on_success((0, 42, 0.001))
            assert not job.acknowledged
        finally:
            module._does_info = prev

    def test_on_success_BaseException(self):
        job = self.xRequest()
        job.time_start = 1
        with pytest.raises(SystemExit):
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
        assert job.acknowledged

    def test_on_failure_WorkerLostError(self):

        def get_ei():
            try:
                raise WorkerLostError('do re mi')
            except WorkerLostError:
                return ExceptionInfo()

        job = self.xRequest()
        exc_info = get_ei()
        job.on_failure(exc_info)
        self.app.set_current()
        assert self.mytask.backend.get_status(job.id) == states.FAILURE

        self.mytask.ignore_result = True
        exc_info = get_ei()
        job = self.xRequest()
        job.on_failure(exc_info)
        assert self.mytask.backend.get_status(job.id) == states.PENDING

    def test_on_failure_acks_late_reject_on_worker_lost_enabled(self):
        try:
            raise WorkerLostError()
        except WorkerLostError:
            exc_info = ExceptionInfo()
        self.mytask.acks_late = True
        self.mytask.reject_on_worker_lost = True

        job = self.xRequest()
        job.delivery_info['redelivered'] = False
        job.on_failure(exc_info)

        assert self.mytask.backend.get_status(job.id) == states.PENDING

        job = self.xRequest()
        job.delivery_info['redelivered'] = True
        job.on_failure(exc_info)

        assert self.mytask.backend.get_status(job.id) == states.PENDING

    def test_on_failure_acks_late(self):
        job = self.xRequest()
        job.time_start = 1
        self.mytask.acks_late = True
        try:
            raise KeyError('foo')
        except KeyError:
            exc_info = ExceptionInfo()
            job.on_failure(exc_info)
        assert job.acknowledged

    def test_on_failure_acks_on_failure_or_timeout_disabled_for_task(self):
        job = self.xRequest()
        job.time_start = 1
        job._on_reject = Mock()
        self.mytask.acks_late = True
        self.mytask.acks_on_failure_or_timeout = False
        try:
            raise KeyError('foo')
        except KeyError:
            exc_info = ExceptionInfo()
            job.on_failure(exc_info)

        assert job.acknowledged is True
        job._on_reject.assert_called_with(req_logger, job.connection_errors, False)

    def test_on_failure_acks_on_failure_or_timeout_enabled_for_task(self):
        job = self.xRequest()
        job.time_start = 1
        self.mytask.acks_late = True
        self.mytask.acks_on_failure_or_timeout = True
        try:
            raise KeyError('foo')
        except KeyError:
            exc_info = ExceptionInfo()
            job.on_failure(exc_info)
        assert job.acknowledged is True

    def test_on_failure_acks_on_failure_or_timeout_disabled(self):
        self.app.conf.acks_on_failure_or_timeout = False
        job = self.xRequest()
        job.time_start = 1
        self.mytask.acks_late = True
        self.mytask.acks_on_failure_or_timeout = False
        try:
            raise KeyError('foo')
        except KeyError:
            exc_info = ExceptionInfo()
            job.on_failure(exc_info)
        assert job.acknowledged is True
        job._on_reject.assert_called_with(req_logger, job.connection_errors,
                                          False)
        self.app.conf.acks_on_failure_or_timeout = True

    def test_on_failure_acks_on_failure_or_timeout_enabled(self):
        self.app.conf.acks_on_failure_or_timeout = True
        job = self.xRequest()
        job.time_start = 1
        self.mytask.acks_late = True
        try:
            raise KeyError('foo')
        except KeyError:
            exc_info = ExceptionInfo()
            job.on_failure(exc_info)
        assert job.acknowledged is True

    def test_from_message_invalid_kwargs(self):
        m = self.TaskMessage(self.mytask.name, args=(), kwargs='foo')
        req = Request(m, app=self.app)
        with pytest.raises(InvalidTaskError):
            raise req.execute().exception

    def test_on_hard_timeout_acks_late(self, patching):
        error = patching('celery.worker.request.error')

        job = self.xRequest()
        job.acknowledge = Mock(name='ack')
        job.task.acks_late = True
        job.on_timeout(soft=False, timeout=1337)
        assert 'Hard time limit' in error.call_args[0][0]
        assert self.mytask.backend.get_status(job.id) == states.FAILURE
        job.acknowledge.assert_called_with()

        job = self.xRequest()
        job.acknowledge = Mock(name='ack')
        job.task.acks_late = False
        job.on_timeout(soft=False, timeout=1335)
        job.acknowledge.assert_not_called()

    def test_on_hard_timeout_acks_on_failure_or_timeout(self, patching):
        error = patching('celery.worker.request.error')

        job = self.xRequest()
        job.acknowledge = Mock(name='ack')
        job.task.acks_late = True
        job.task.acks_on_failure_or_timeout = True
        job.on_timeout(soft=False, timeout=1337)
        assert 'Hard time limit' in error.call_args[0][0]
        assert self.mytask.backend.get_status(job.id) == states.FAILURE
        job.acknowledge.assert_called_with()

        job = self.xRequest()
        job.acknowledge = Mock(name='ack')
        job.task.acks_late = True
        job.task.acks_on_failure_or_timeout = False
        job.on_timeout(soft=False, timeout=1337)
        assert 'Hard time limit' in error.call_args[0][0]
        assert self.mytask.backend.get_status(job.id) == states.FAILURE
        job.acknowledge.assert_not_called()

        job = self.xRequest()
        job.acknowledge = Mock(name='ack')
        job.task.acks_late = False
        job.task.acks_on_failure_or_timeout = True
        job.on_timeout(soft=False, timeout=1335)
        job.acknowledge.assert_not_called()

    def test_on_soft_timeout(self, patching):
        warn = patching('celery.worker.request.warn')

        job = self.xRequest()
        job.acknowledge = Mock(name='ack')
        job.task.acks_late = True
        job.on_timeout(soft=True, timeout=1337)
        assert 'Soft time limit' in warn.call_args[0][0]
        assert self.mytask.backend.get_status(job.id) == states.PENDING
        job.acknowledge.assert_not_called()

        self.mytask.ignore_result = True
        job = self.xRequest()
        job.on_timeout(soft=True, timeout=1336)
        assert self.mytask.backend.get_status(job.id) == states.PENDING

    def test_fast_trace_task(self):
        from celery.app import trace
        setup_worker_optimizations(self.app)
        assert trace.trace_task_ret is trace._fast_trace_task
        tid = uuid()
        message = self.TaskMessage(self.mytask.name, tid, args=[4])
        assert len(message.payload) == 3
        try:
            self.mytask.__trace__ = build_tracer(
                self.mytask.name, self.mytask, self.app.loader, 'test',
                app=self.app,
            )
            failed, res, runtime = trace.trace_task_ret(
                self.mytask.name, tid, message.headers, message.body,
                message.content_type, message.content_encoding)
            assert not failed
            assert res == repr(4 ** 4)
            assert runtime is not None
            assert isinstance(runtime, numbers.Real)
        finally:
            reset_worker_optimizations()
            assert trace.trace_task_ret is trace._trace_task_ret
        delattr(self.mytask, '__trace__')
        failed, res, runtime = trace.trace_task_ret(
            self.mytask.name, tid, message.headers, message.body,
            message.content_type, message.content_encoding, app=self.app,
        )
        assert not failed
        assert res == repr(4 ** 4)
        assert runtime is not None
        assert isinstance(runtime, numbers.Real)

    def test_trace_task_ret(self):
        self.mytask.__trace__ = build_tracer(
            self.mytask.name, self.mytask, self.app.loader, 'test',
            app=self.app,
        )
        tid = uuid()
        message = self.TaskMessage(self.mytask.name, tid, args=[4])
        _, R, _ = _trace_task_ret(
            self.mytask.name, tid, message.headers,
            message.body, message.content_type,
            message.content_encoding, app=self.app,
        )
        assert R == repr(4 ** 4)

    def test_trace_task_ret__no_trace(self):
        try:
            delattr(self.mytask, '__trace__')
        except AttributeError:
            pass
        tid = uuid()
        message = self.TaskMessage(self.mytask.name, tid, args=[4])
        _, R, _ = _trace_task_ret(
            self.mytask.name, tid, message.headers,
            message.body, message.content_type,
            message.content_encoding, app=self.app,
        )
        assert R == repr(4 ** 4)

    def test_trace_catches_exception(self):

        @self.app.task(request=None, shared=False)
        def raising():
            raise KeyError('baz')

        with pytest.warns(RuntimeWarning):
            res = trace_task(raising, uuid(), [], {}, app=self.app)[0]
            assert isinstance(res, ExceptionInfo)

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
                assert self.mytask.backend.get_status(tid) == states.PENDING
                w.handle_retry(
                    self.mytask, self.mytask.request, store_errors=True,
                )
                assert self.mytask.backend.get_status(tid) == states.RETRY
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
                assert self.mytask.backend.get_status(tid) == states.PENDING
                w.handle_failure(
                    self.mytask, self.mytask.request, store_errors=True,
                )
                assert self.mytask.backend.get_status(tid) == states.FAILURE
        finally:
            self.mytask.pop_request()

    def test_from_message(self):
        us = 'æØåveéðƒeæ'
        tid = uuid()
        m = self.TaskMessage(
            self.mytask.name, tid, args=[2], kwargs={us: 'bar'},
        )
        job = Request(m, app=self.app)
        assert isinstance(job, Request)
        assert job.name == self.mytask.name
        assert job.id == tid
        assert job.message is m

    def test_from_message_empty_args(self):
        tid = uuid()
        m = self.TaskMessage(self.mytask.name, tid, args=[], kwargs={})
        job = Request(m, app=self.app)
        assert isinstance(job, Request)

    def test_from_message_missing_required_fields(self):
        m = self.TaskMessage(self.mytask.name)
        m.headers.clear()
        with pytest.raises(KeyError):
            Request(m, app=self.app)

    def test_from_message_nonexistant_task(self):
        m = self.TaskMessage(
            'cu.mytask.doesnotexist',
            args=[2], kwargs={'æØåveéðƒeæ': 'bar'},
        )
        with pytest.raises(KeyError):
            Request(m, app=self.app)

    def test_execute(self):
        tid = uuid()
        job = self.xRequest(id=tid, args=[4], kwargs={})
        assert job.execute() == 256
        meta = self.mytask.backend.get_task_meta(tid)
        assert meta['status'] == states.SUCCESS
        assert meta['result'] == 256

    def test_execute_backend_error_acks_late(self):
        """direct call to execute should reject task in case of internal failure."""
        tid = uuid()
        self.mytask.acks_late = True
        job = self.xRequest(id=tid, args=[4], kwargs={})
        job._on_reject = Mock()
        job._on_ack = Mock()
        self.mytask.backend = BaseDictBackend(app=self.app)
        self.mytask.backend.mark_as_done = Mock()
        self.mytask.backend.mark_as_done.side_effect = Exception()
        self.mytask.backend.mark_as_failure = Mock()
        self.mytask.backend.mark_as_failure.side_effect = Exception()

        job.execute()

        assert job.acknowledged
        job._on_reject.assert_called_once()
        job._on_ack.assert_not_called()

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
        assert job.execute() == 256
        meta = mytask_no_kwargs.backend.get_task_meta(tid)
        assert meta['result'] == 256
        assert meta['status'] == states.SUCCESS

    def test_execute_ack(self):
        scratch = {'ACK': False}

        def on_ack(*args, **kwargs):
            scratch['ACK'] = True

        tid = uuid()
        job = self.xRequest(id=tid, args=[4], on_ack=on_ack)
        assert job.execute() == 256
        meta = self.mytask.backend.get_task_meta(tid)
        assert scratch['ACK']
        assert meta['result'] == 256
        assert meta['status'] == states.SUCCESS

    def test_execute_fail(self):
        tid = uuid()
        job = self.xRequest(
            name=self.mytask_raising.name,
            id=tid,
            args=[4],
            kwargs={},
        )
        assert isinstance(job.execute(), ExceptionInfo)
        assert self.mytask_raising.backend.serializer == 'pickle'
        meta = self.mytask_raising.backend.get_task_meta(tid)
        assert meta['status'] == states.FAILURE
        assert isinstance(meta['result'], KeyError)

    def test_execute_using_pool(self):
        tid = uuid()
        job = self.xRequest(id=tid, args=[4])
        p = Mock()
        job.execute_using_pool(p)
        p.apply_async.assert_called_once()
        args = p.apply_async.call_args[1]['args']
        assert args[0] == self.mytask.name
        assert args[1] == tid
        assert args[2] == job.request_dict
        assert args[3] == job.message.body

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
            job.id, exc, request=job._context, store_result=True,
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
        assert job.acknowledged
        job.on_reject.reset_mock()
        job.reject(requeue=True)
        job.on_reject.assert_not_called()

    def test_group(self):
        gid = uuid()
        job = self.xRequest(id=uuid(), group=gid)
        assert job.group == gid

    def test_group_index(self):
        group_index = 42
        job = self.xRequest(id=uuid(), group_index=group_index)
        assert job.group_index == group_index


class test_create_request_class(RequestCase):

    def setup(self):
        self.task = Mock(name='task')
        self.pool = Mock(name='pool')
        self.eventer = Mock(name='eventer')
        RequestCase.setup(self)

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
            with pytest.raises(exc):
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
        with pytest.raises(TaskRevokedError):
            job.execute_using_pool(self.pool)

    def test_execute_using_pool__expired(self):
        tid = uuid()
        job = self.zRequest(id=tid, revoked_tasks=set())
        job.expires = 1232133
        job.revoked = Mock()
        job.revoked.return_value = True
        with pytest.raises(TaskRevokedError):
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
        assert job._apply_result
        weakref_ref.assert_called_with(self.pool.apply_async())
        assert job._apply_result is weakref_ref()

    def test_execute_using_pool_with_none_timelimit_header(self):
        from celery.app.trace import trace_task_ret as trace
        weakref_ref = Mock(name='weakref.ref')
        job = self.zRequest(id=uuid(),
                            revoked_tasks=set(),
                            ref=weakref_ref,
                            headers={'timelimit': None})
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
        assert job._apply_result
        weakref_ref.assert_called_with(self.pool.apply_async())
        assert job._apply_result is weakref_ref()

    def test_execute_using_pool__defaults_of_hybrid_to_proto2(self):
        weakref_ref = Mock(name='weakref.ref')
        headers = strategy.hybrid_to_proto2(Mock(headers=None), {'id': uuid(),
                                                                 'task': self.mytask.name})[1]
        job = self.zRequest(revoked_tasks=set(), ref=weakref_ref, **headers)
        job.execute_using_pool(self.pool)
        assert job._apply_result
        weakref_ref.assert_called_with(self.pool.apply_async())
        assert job._apply_result is weakref_ref()
