"""Execution of coroutine (``async def``) task bodies.

The task tracer in :mod:`celery.app.trace` is synchronous: it calls the task
body, stores the result, fires the signals and acknowledges the message in one
straight line of code.  A task defined with ``async def`` does not fit that
shape, because calling it returns a coroutine object instead of a result and
something has to drive that coroutine to completion.

This module holds the single point of indirection that decides *how* that
happens, so the tracer stays free of event loop details and so an execution
pool can take over the job.  A pool that owns an event loop -- see
:mod:`celery.concurrency.asyncio` -- installs its own runner through
:func:`push_coroutine_runner` at startup, and from then on every coroutine
task body in the process is awaited on that one shared loop.

When no pool has installed a runner, :func:`default_coroutine_runner` is used
instead: it runs the coroutine on a private event loop that lives only for the
duration of that one task.  That keeps ``async def`` tasks working on the
other pools, but without a shared loop: anything bound to a loop (a database
connection pool, an HTTP client session) cannot be reused between tasks.
"""
from __future__ import annotations

import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Coroutine

from celery.exceptions import CPendingDeprecationWarning, ImproperlyConfigured

__all__ = (
    'CoroutineRunner',
    'default_coroutine_runner',
    'get_coroutine_runner',
    'pop_coroutine_runner',
    'push_coroutine_runner',
    'resolve_coroutine',
    'set_coroutine_runner',
)

#: Signature of a runner: takes a coroutine, returns the value it resolves to,
#: and raises whatever the coroutine raised.
CoroutineRunner = Callable[[Coroutine], Any]

E_NO_COROUTINE_RUNNER = """\
Task body is a coroutine, and this worker has no event loop to run it on.

Start the worker with --pool=asyncio, which runs coroutine tasks on one
event loop shared by the whole process.  To run them anywhere -- on a
private, short-lived loop per task, with none of the benefits of a shared
one -- set worker_resolve_coroutines = True.\
"""

W_RESOLVE_COROUTINES_UNSET = """\
Task body is a coroutine, and this worker has no event loop to run it on.
It is being run anyway, on a private, short-lived loop, for backward
compatibility -- but in Celery 6.0 this will raise ImproperlyConfigured
instead, naming --pool=asyncio, the same as it already does when
worker_resolve_coroutines is explicitly set to False.

To keep it running as it does today, set worker_resolve_coroutines = True.
To opt into the future behaviour now, set it to False.\
"""

#: Runners installed by execution pools that own an event loop, most recent
#: last.  A stack rather than a single slot so that two pool instances alive
#: in the same process (there is no rule against it, and tests do it) do not
#: fight over one another's runner: whichever stops first removes its own
#: entry, wherever it sits, and the pool that is still running keeps whatever
#: it installed -- restoring a single saved "previous" value on stop would
#: instead clobber it if the two were not stopped in the exact reverse order
#: they started in.  Process-local either way: each worker process (or the
#: single process of a threaded pool) has its own.
_coroutine_runner_stack: list[CoroutineRunner] = []


def default_coroutine_runner(coro: Coroutine) -> Any:
    """Run ``coro`` to completion on a private, short-lived event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    # This thread is already running a loop -- an eager task called from
    # async code, typically in a test suite.  Blocking on the coroutine here
    # would deadlock that loop and ``asyncio.run`` refuses outright, so give
    # it a thread and a loop of its own, as ``asgiref`` does.
    with ThreadPoolExecutor(max_workers=1,
                            thread_name_prefix='celery-coroutine') as pool:
        return pool.submit(asyncio.run, coro).result()


def set_coroutine_runner(runner: CoroutineRunner | None) -> CoroutineRunner | None:
    """Replace the whole runner stack with just ``runner`` (or clear it).

    Simple single-slot semantics, for a caller that knows only one runner
    will ever be installed at a time -- a test, or a script.  Returns
    whatever was on top of the stack before, if anything.

    A pool that must coexist safely with another pool instance already
    running in the same process should use :func:`push_coroutine_runner`
    instead: this function's "replace everything" semantics would otherwise
    let two pools silently steal the loop from one another.
    """
    previous = get_coroutine_runner()
    _coroutine_runner_stack[:] = [runner] if runner is not None else []
    return previous


def push_coroutine_runner(runner: CoroutineRunner) -> CoroutineRunner:
    """Install ``runner`` without disturbing one installed before it.

    Safe when more than one pool with its own loop is alive in the same
    process: each keeps its own place on the stack, so stopping one --
    in any order, not necessarily the reverse of how they started -- never
    clobbers a still-running pool's runner.  Returns a token to hand back to
    :func:`pop_coroutine_runner`.
    """
    _coroutine_runner_stack.append(runner)
    return runner


def pop_coroutine_runner(token: CoroutineRunner) -> None:
    """Undo :func:`push_coroutine_runner`, wherever ``token`` ended up.

    A no-op if it is not there any more -- :func:`set_coroutine_runner`, or
    a second call, may have already removed it.
    """
    try:
        _coroutine_runner_stack.remove(token)
    except ValueError:
        pass


def get_coroutine_runner() -> CoroutineRunner | None:
    """Return the runner installed by the most recently started pool, if any."""
    return _coroutine_runner_stack[-1] if _coroutine_runner_stack else None


def resolve_coroutine(coro: Coroutine, fallback: bool | None = False) -> Any:
    """Run a coroutine returned by a task body and return its result.

    With no pool-provided runner, ``fallback`` decides what happens: see
    :setting:`worker_resolve_coroutines`.  :const:`True` and :const:`False`
    are today's and tomorrow's default respectively; :const:`None` -- the
    setting left unset -- keeps today's behaviour for one deprecation cycle,
    with a warning pointing at both.
    """
    runner = get_coroutine_runner()
    if runner is None:
        if fallback is None:
            # warnings.warn() only prints once per (message, module, line)
            # by default, so this does not spam the log of a worker that
            # runs many such tasks.
            warnings.warn(
                CPendingDeprecationWarning(W_RESOLVE_COROUTINES_UNSET),
                stacklevel=2,
            )
        elif not fallback:
            coro.close()
            raise ImproperlyConfigured(E_NO_COROUTINE_RUNNER)
        runner = default_coroutine_runner
    return runner(coro)
