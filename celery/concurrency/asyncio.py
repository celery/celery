"""Asyncio execution pool.

Runs every ``async def`` task body on a single event loop that is shared by
the whole worker process, so loop-bound resources (database connection pools,
HTTP client sessions) can be created once and reused by every task.

The surrounding task tracing -- decoding the message, firing signals, storing
the result, acknowledging -- is synchronous in Celery, so it runs in a bounded
thread pool of ``--concurrency`` threads.  Each thread blocks on its own
coroutine while that coroutine runs, together with every other in-flight
coroutine, on the one loop.
"""
from __future__ import annotations

import asyncio
import os
import threading
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import Future, ThreadPoolExecutor, wait
from itertools import count
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from celery import signals
from celery._state import _task_stack
from celery.exceptions import ImproperlyConfigured, SoftTimeLimitExceeded, Terminated, TimeLimitExceeded
from celery.utils.coroutines import pop_coroutine_runner, push_coroutine_runner
from celery.utils.log import get_logger
from celery.utils.threads import (LocalStack, _FastLocalStack, get_ident, release_local_ident, reset_local_ident,
                                  use_local_ident)

from .base import BasePool, apply_target

__all__ = ('TaskPool',)

logger = get_logger(__name__)

#: Marker a coroutine leaves behind when it cleaned up after itself.
_CLEANED = object()

#: Raised by either of the two unrelated ``CancelledError`` classes, depending
#: on which side of the thread boundary the cancellation was noticed.  They are
#: the same class on some Python builds and distinct on others, so both are
#: always caught.
CANCELLED_ERRORS = (asyncio.CancelledError, FutureCancelledError)

if TYPE_CHECKING:
    from typing import TypedDict

    PoolInfo = TypedDict('PoolInfo', {
        'max-concurrency': int,
        'threads': int,
        'running-coroutines': int,
    })

    TargetFunction = Callable[..., Any]


class _JobLocals(threading.local):
    """Per-thread handle on the job a tracer thread is currently running."""

    job_id: int | None = None
    timeout: float | None = None
    soft_timeout: float | None = None
    timeout_callback: Callable[..., Any] | None = None
    #: Guards against a task body running another task's coroutine inline.
    in_body: bool = False


class _Job:
    """What the pool remembers about one task while it runs."""

    __slots__ = ('id', 'future', 'terminated')

    def __init__(self, job_id: int) -> None:
        self.id = job_id
        #: Future of the coroutine, once the tracer reaches the task body.
        self.future: Future | None = None
        #: Set by terminate_job() -- possibly before there is anything to
        #: cancel, as a revoke can land while the tracer is still decoding
        #: the message.
        self.terminated = False


class ApplyResult:
    """Result handle, shaped like the one returned by the threads pool."""

    def __init__(self, future: Future, pool: TaskPool, job: _Job) -> None:
        self.f = future
        self.get = self.f.result
        self._pool = pool
        self._job = job

    def wait(self, timeout: float | None = None) -> None:
        wait([self.f], timeout)

    def terminate(self, signal: int | None = None) -> None:
        # Deliberately not cancelling a queued future: the job would vanish
        # without its accept/error callbacks ever running, so the request
        # would be announced revoked and then never acknowledged.  Mark it
        # instead and let it terminate itself the moment it starts.
        self._pool.terminate_job(self._job.id, signal)


class TaskPool(BasePool):
    """Asyncio Task Pool."""

    limit: int

    body_can_be_buffer = True
    signal_safe = False

    #: Seconds to wait for the event loop thread to come up or wind down.
    loop_timeout = 5.0

    #: Seconds the loop waits, at shutdown, for the coroutines it just
    #: cancelled to unwind.  A coroutine that swallows its cancellation is
    #: abandoned once this elapses -- there is no way to kill it, the way
    #: prefork can kill a child process.
    cancel_grace = 1.0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
        self._loop_running = threading.Event()
        self._executor: ThreadPoolExecutor | None = None
        #: Monotonic, never reused: a thread id would be, and a revoke that
        #: arrives just after its task finished would then hit whichever task
        #: inherited the thread.
        self._job_ids = count(1)
        self._jobs: dict[int, _Job] = {}
        self._mutex = threading.Lock()
        self._locals = _JobLocals()
        self._runner_token = None
        self._runner_installed = False
        self._shutting_down = False

    # -- Lifecycle

    #: Worker options that belong to prefork's child processes and mean
    #: nothing here.  Warned about rather than ignored in silence.
    unsupported_options = (
        'maxtasksperchild', 'max_memory_per_child', 'max_tasks_per_child',
    )

    def on_start(self) -> None:
        # Reset in case this instance is being started again after a stop:
        # _shutdown() leaves it set, and a stale True would make every
        # coroutine body raise Terminated forever.
        self._shutting_down = False
        if LocalStack is _FastLocalStack:
            raise ImproperlyConfigured(
                'asyncio pool: USE_FAST_LOCALS is set, and the fast local '
                'stack is keyed by thread with no way to override that.  '
                'Every coroutine on the pool\'s loop would share one task '
                'request, so tasks would see each other\'s request ids and '
                'retry each other\'s messages.  Unset USE_FAST_LOCALS to use '
                'this pool.')
        for option in self.unsupported_options:
            if self.options.get(option):
                logger.warning(
                    'asyncio pool: %s has no effect on this pool, it is a '
                    'property of the prefork pool\'s child processes',
                    option)
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            name='celery-asyncio-pool',
            daemon=True,
        )
        self._loop_thread.start()
        if not self._loop_running.wait(self.loop_timeout):
            self._shutdown(wait=False)
            raise RuntimeError(
                'asyncio pool: event loop did not start within '
                f'{self.loop_timeout} seconds')
        self._executor = ThreadPoolExecutor(
            max_workers=self.limit, thread_name_prefix='asyncio-pool')
        # push, not set: another TaskPool instance may already be running in
        # this process (nothing forbids it, and tests do it), and stopping
        # this one must not be able to steal a still-live pool's runner --
        # which a single saved "previous" value would, if the two were not
        # stopped in the exact reverse order they started in.
        self._runner_token = push_coroutine_runner(self.run_coroutine)
        self._runner_installed = True
        try:
            # Single-process pool, like solo: this is the one chance a user
            # gets to set up the resources their tasks share -- a client
            # session, a connection pool -- now that the event loop those
            # will be bound to exists.
            signals.worker_process_init.send(sender=None)
        except BaseException:
            # Leaving a live loop thread and a process-wide runner behind
            # would outlast the worker that failed to start.
            self._shutdown(wait=False)
            raise

    def on_stop(self) -> None:
        self._shutdown(wait=True)
        super().on_stop()

    def on_terminate(self) -> None:
        self._shutdown(wait=False)

    def _shutdown(self, wait: bool) -> None:
        # Refuse new bodies from the first moment: on the terminate path the
        # executor does not drain, so tracer threads outlive this call and
        # would otherwise reach run_coroutine after the loop had gone.
        self._shutting_down = True
        # Order matters throughout.
        #
        # The executor drains first, while the runner and the loop are still
        # installed: a job that is mid-flight would otherwise fall back to the
        # default runner and finish its body on a private, throw-away loop --
        # silently losing every loop-bound resource the worker set up.
        if self._executor is not None:
            if not wait:
                self._cancel_all_jobs()
            self._executor.shutdown(wait=wait, cancel_futures=not wait)
            self._executor = None

        if self._runner_installed:
            # Counterpart of the worker_process_init in on_start, and sent
            # while the loop is still running, so a handler can still close
            # what it opened there.
            signals.worker_process_shutdown.send(
                sender=None, pid=os.getpid(), exitcode=None)
            pop_coroutine_runner(self._runner_token)
            self._runner_token = None
            self._runner_installed = False

        loop, self._loop = self._loop, None
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(loop.stop)

        thread, self._loop_thread = self._loop_thread, None
        if thread is not None and thread.is_alive():
            thread.join(self.loop_timeout)
            if thread.is_alive():  # pragma: no cover
                logger.warning(
                    'asyncio pool: event loop thread did not stop within '
                    '%s seconds', self.loop_timeout)

        self._loop_running.clear()

    def _on_loop_error(self, loop: asyncio.AbstractEventLoop,
                       context: dict) -> None:
        # Keeps asyncio's own diagnostics ("Task was destroyed but it is
        # pending!", unretrieved task exceptions) out of stderr and in the
        # worker's log, where the rest of the pool's output already goes.
        logger.warning('asyncio pool: %s',
                       context.get('message', 'unhandled event loop error'),
                       exc_info=context.get('exception'))

    def _run_event_loop(self) -> None:
        loop = self._loop
        asyncio.set_event_loop(loop)
        loop.set_exception_handler(self._on_loop_error)
        loop.call_soon(self._loop_running.set)
        try:
            loop.run_forever()
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(
                        asyncio.wait(pending, timeout=self.cancel_grace))
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    # -- Running tasks

    def on_apply(
        self,
        target: TargetFunction,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        callback: Callable[..., Any] | None = None,
        accept_callback: Callable[..., Any] | None = None,
        timeout: float | None = None,
        soft_timeout: float | None = None,
        timeout_callback: Callable[..., Any] | None = None,
        **_: Any
    ) -> ApplyResult:
        if self._executor is None:
            raise RuntimeError('asyncio pool: the pool is not running')

        job = _Job(next(self._job_ids))
        with self._mutex:
            self._jobs[job.id] = job

        def run() -> None:
            locals_ = self._locals
            locals_.job_id = job.id
            locals_.timeout = timeout
            locals_.soft_timeout = soft_timeout
            locals_.timeout_callback = timeout_callback
            try:
                # A job revoked before it got here is not dropped: it runs,
                # and stops at its task body, so the tracer is the one that
                # reports it -- the same path as a revoke that lands while the
                # body is running.
                # ``pid`` becomes Request.worker_pid, which is what
                # terminate_job() is later called with.
                apply_target(target, args, kwargs, callback, accept_callback,
                             pid=job.id)
            finally:
                locals_.job_id = None
                locals_.timeout_callback = None
                with self._mutex:
                    self._jobs.pop(job.id, None)

        try:
            future = self._executor.submit(run)
        except BaseException:
            with self._mutex:
                self._jobs.pop(job.id, None)
            raise
        return ApplyResult(future, self, job)

    def run_coroutine(self, coro: Coroutine) -> Any:
        """Run a coroutine task body on the pool's event loop.

        Installed as the process-wide coroutine runner while the pool is
        running (see :mod:`celery.utils.coroutines`) and called by the tracer
        from the thread the task is being traced in.
        """
        loop = self._loop
        if self._shutting_down or loop is None or loop.is_closed():
            coro.close()
            raise Terminated(
                'asyncio pool: the pool is shutting down, '
                'the task body was not started')

        # Read in the tracer's own thread, where the context still lives.
        # Kept on the stack, not on the pool: every tracer thread is running a
        # different task.
        traced = _task_stack.top
        request = traced.request if traced is not None else None

        thread = self._loop_thread
        if thread is not None and thread.ident == threading.get_ident():
            coro.close()
            raise RuntimeError(
                'asyncio pool: refusing to run a coroutine task from inside '
                'the event loop thread, as waiting for it would deadlock the '
                'loop.  This happens when a coroutine task calls another task '
                'eagerly (task.apply() or task_always_eager); await the other '
                "task's coroutine directly instead.")

        locals_ = self._locals
        if locals_.in_body:
            # A synchronous body running another task eagerly: the inner
            # coroutine is not this job, and must not overwrite its future,
            # inherit its time limits or make it unrevokable.
            job = None
            timeout = soft_timeout = timeout_callback = None
        else:
            with self._mutex:
                job = self._jobs.get(locals_.job_id)
            timeout = locals_.timeout
            soft_timeout = locals_.soft_timeout
            timeout_callback = locals_.timeout_callback
        if job is not None and job.terminated:
            coro.close()
            raise Terminated('task was terminated')

        identity: list = []
        guarded = self._guard(
            self._with_request(coro, traced, request, identity),
            timeout, soft_timeout, timeout_callback,
        )
        locals_.in_body = True
        future = asyncio.run_coroutine_threadsafe(guarded, loop)
        if job is not None:
            with self._mutex:
                job.future = future
                if job.terminated:
                    # Terminated between the two checks.
                    future.cancel()
        try:
            return future.result()
        except CANCELLED_ERRORS:
            # terminate_job() cancelled us, or the loop was torn down
            # underneath the task.  Report it the way prefork reports a job
            # killed by a signal, instead of letting a BaseException escape
            # into the tracer.
            raise Terminated('task was terminated') from None
        finally:
            locals_.in_body = False
            if job is not None:
                with self._mutex:
                    job.future = None
            if identity and _CLEANED not in identity:
                # The body outlived the wait: it swallowed its cancellation,
                # and the ``finally`` that would clean up after it runs -- if
                # ever -- when the interpreter collects its frame, where it
                # is no longer safe to touch the stacks.  So clean up here.
                release_local_ident(
                    identity[0], _task_stack,
                    getattr(traced, 'request_stack', None))

    async def _with_request(self, coro: Coroutine, task: Any,
                            request: Any, identity: list) -> Any:
        """Run ``coro`` under the request context of the task being traced.

        The tracer pushed that context in *its* thread, and the coroutine runs
        in the loop's thread, so it has to be pushed again over here or
        ``self.request`` would be empty inside an ``async def`` body --
        breaking ``self.retry()``, ``self.request.id`` and every other bound
        task.  What makes this safe with many coroutines sharing the one loop
        thread is that the stacks are keyed by the running asyncio task; see
        :func:`celery.utils.threads.get_ident`.
        """
        if task is None:
            return await coro
        # Own identity first, so the stacks below are this coroutine's and not
        # shared with every other one on this thread.  asyncio copied the
        # context when it made this task, so the change cannot leak out.
        ident = object()
        identity.append(ident)
        token = use_local_ident(ident)
        try:
            _task_stack.push(task)
            task.request_stack.push(request)
            try:
                return await coro
            finally:
                # Only when this really is still our context.  An abandoned
                # coroutine's finally runs when the interpreter collects its
                # frame, with no loop and no context of ours, and popping
                # then would take the top off whichever stack the collecting
                # thread owns -- another task's request.
                if get_ident() is ident:
                    _task_stack.pop()
                    task.request_stack.pop()
                    identity.append(_CLEANED)
        finally:
            reset_local_ident(token)

    async def _guard(
        self,
        coro: Coroutine,
        timeout: float | None = None,
        soft_timeout: float | None = None,
        timeout_callback: Callable[..., Any] | None = None,
    ) -> Any:
        """Await ``coro``, applying the task's time limits.

        Every path out of here cancels the body first: ``asyncio.wait`` does
        not cancel what it was waiting on, so a body left behind would go on
        running on the shared loop after the worker had already reported the
        task as terminated or timed out.
        """
        if not (timeout or soft_timeout):
            # Runs in this task, so a cancellation of this task is the
            # cancellation of the body.  Nothing to clean up.
            return await coro

        # A hard limit at or below the soft one leaves the soft one no room:
        # prefork would kill the child at the hard limit and the soft one
        # would never fire, so do not let it fire here either.
        if timeout and soft_timeout and timeout <= soft_timeout:
            soft_timeout = None

        task = asyncio.ensure_future(coro)
        try:
            if soft_timeout:
                done, _ = await asyncio.wait({task}, timeout=soft_timeout)
                if done:
                    return task.result()

                # Soft limit: cancel, which surfaces inside the coroutine as
                # CancelledError at its current await point, so it can clean
                # up.
                if timeout_callback is not None:
                    timeout_callback(soft=True, timeout=soft_timeout)
                task.cancel()

                if timeout:
                    done, _ = await asyncio.wait(
                        {task}, timeout=timeout - soft_timeout)
                    if not done:
                        if timeout_callback is not None:
                            timeout_callback(soft=False, timeout=timeout)
                        raise TimeLimitExceeded(timeout)
                else:
                    # No hard limit to fall back on, but the tracer thread
                    # cannot be held for ever by a body that swallowed its
                    # cancellation: give it a moment to unwind and then
                    # report the limit it actually exceeded.
                    done, _ = await asyncio.wait(
                        {task}, timeout=self.cancel_grace)
                    if not done:
                        logger.warning(
                            'asyncio pool: task body ignored the '
                            'cancellation of its soft time limit and is '
                            'still running; abandoning it')
                        raise SoftTimeLimitExceeded(soft_timeout)
                if task.cancelled():
                    raise SoftTimeLimitExceeded(soft_timeout)
                # The body caught the cancellation and returned (or raised)
                # normally: its outcome stands, as it does under prefork.
                return task.result()

            done, _ = await asyncio.wait({task}, timeout=timeout)
            if not done:
                if timeout_callback is not None:
                    timeout_callback(soft=False, timeout=timeout)
                raise TimeLimitExceeded(timeout)
            return task.result()
        finally:
            if not task.done():
                task.cancel()

    # -- Revocation

    def terminate_job(self, job_id: int, signal: int | None = None) -> None:
        """Stop a job, whatever stage it has reached.

        ``job_id`` is what this pool reports as the worker pid when a task is
        accepted.  A job whose coroutine is running is cancelled, and the
        cancellation is delivered at the coroutine's next await point.  A job
        that has not got there yet -- still queued, or in the synchronous part
        of tracing -- is marked, and stops when it reaches its body.

        A task whose body is an ordinary function never reaches that point, so
        it runs to completion: this pool cannot interrupt synchronous code,
        any more than the threads pool can.
        """
        with self._mutex:
            job = self._jobs.get(job_id)
            if job is None:
                # Ordinary outcome of a broadcast revoke: the job finished
                # before the message arrived.
                logger.debug('asyncio pool: no job %s to terminate', job_id)
                return
            job.terminated = True
            future = job.future
        if future is not None:
            future.cancel()
        else:
            logger.info(
                'asyncio pool: job %s is not running a coroutine yet, '
                'it will stop when it reaches its task body', job_id)

    def _cancel_all_jobs(self) -> None:
        with self._mutex:
            jobs = list(self._jobs.values())
        for job in jobs:
            job.terminated = True
            if job.future is not None:
                job.future.cancel()

    # -- Introspection

    def _get_info(self) -> PoolInfo:
        info = super()._get_info()
        executor = self._executor
        with self._mutex:
            running = len(self._jobs)
        info.update({
            'max-concurrency': self.limit,
            'threads': len(executor._threads) if executor is not None else 0,
            'running-coroutines': running,
        })
        return info
