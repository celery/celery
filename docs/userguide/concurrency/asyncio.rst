.. _concurrency-asyncio:

==========================
 Concurrency with asyncio
==========================

.. contents::
    :local:

Introduction
============

The ``asyncio`` pool runs tasks written as coroutines -- ordinary
:keyword:`async def` functions -- on a single event loop shared by the whole
worker process.

.. code-block:: console

    $ celery -A proj worker --pool=asyncio --concurrency=100

.. code-block:: python

    @app.task
    async def fetch(url):
        async with session.get(url) as response:
            return await response.text()

No decorator, setting or base class is needed: a task whose body is a
coroutine function is awaited, a task whose body is a plain function is called,
and both can live in the same worker.

The pool is aimed at IO-bound workloads -- HTTP calls, database queries,
anything that spends its time waiting -- where one worker process can keep
hundreds of requests in flight.  For CPU-bound work use ``prefork``, which is
still the right default.

.. important::

    Always pass an explicit :option:`--concurrency <celery worker
    --concurrency>` with this pool.  Left unset, Celery defaults it to the
    number of CPU cores, which is the right number of *processes* for
    ``prefork`` but not the right number of *tasks in flight* for a pool
    whose whole point is to hold many of them at once while they wait on IO.
    A handful of cores gives you a handful of concurrent tasks -- pick a
    number that reflects how many requests you actually want in flight, not
    how many cores the machine has.

.. _asyncio-shared-loop:

The shared event loop
=====================

Every coroutine task in the process runs on the same loop, which is what makes
loop-bound resources usable:

.. code-block:: python

    session = None

    async def get_session():
        global session
        if session is None:
            session = aiohttp.ClientSession()
        return session

    @app.task
    async def fetch(url):
        client = await get_session()
        async with client.get(url) as response:
            return await response.text()

Build loop-bound resources from inside a task, as above, rather than in a
:signal:`worker_process_init` handler: that signal is sent from the worker's
main thread, where there is no running loop, and several libraries either
refuse to be constructed there (``aiohttp.ClientSession`` raises
``RuntimeError: no running event loop``) or bind themselves to the wrong loop.
Anything that does not care -- ``httpx.AsyncClient``, an SQLAlchemy async
engine -- can still be created in the handler.  This pool sends
:signal:`worker_process_shutdown` when it stops, while its loop is still
running, so a handler there can close what it opened.

A client, connection pool or engine created once is valid for every task,
because there is only ever one loop for it to be bound to.  This is the
difference from wrapping each task body in :func:`asyncio.run` or
``asgiref.sync.async_to_sync``: those create a fresh loop per call, so
connections cannot be pooled across tasks and any object that remembers its
loop breaks on the second task that touches it.

Around each coroutine, the synchronous part of running a task -- decoding the
message, firing the signals, storing the result, acknowledging -- still runs in
a thread, one per task in flight, bounded by :option:`--concurrency
<celery worker --concurrency>`.  So ``--concurrency=100`` means at most 100
tasks in flight, 100 threads doing their bookkeeping, and their 100 coroutines
sharing one loop.

Do not block the loop
=====================

A coroutine that blocks -- a synchronous HTTP call, a long CPU loop,
:func:`time.sleep` -- stalls every other coroutine in the process, not just its
own task.  Hand blocking work to a thread:

.. code-block:: python

    @app.task
    async def resize(path):
        return await asyncio.to_thread(pillow_resize, path)

Tasks that are written as plain functions are not affected by this: they never
touch the loop, they run in their own thread from start to finish.

Bound tasks and retries
=======================

A coroutine task is a task like any other: :keyword:`async def` changes how the
body runs, not what it is.  ``bind=True``, the request, the retry machinery and
the handlers all behave as they do everywhere else.

.. code-block:: python

    @app.task(bind=True, max_retries=3)
    async def fetch(self, url):
        try:
            async with session.get(url) as response:
                return await response.text()
        except aiohttp.ClientError as exc:
            raise self.retry(countdown=2, exc=exc)

    @app.task(autoretry_for=(aiohttp.ClientError,), retry_backoff=True)
    async def fetch_or_retry(url):
        async with session.get(url) as response:
            return await response.text()

:attr:`self.request <celery.app.task.Task.request>` is the running task's own
request, including inside coroutines that are suspended at an ``await`` while
dozens of others run on the same loop.  ``on_success``, ``on_failure`` and
``after_return`` are called as usual, and a coroutine task can be a step of a
:ref:`chain, group or chord <guide-canvas>`.

Calling other tasks
===================

``other_task.delay()`` publishes to the broker synchronously, so calling it
from inside a coroutine blocks the loop for as long as the publish takes --
single-digit milliseconds against a healthy broker, much longer against a sick
one.  It is fine in moderation; a task that dispatches hundreds of others in a
loop should hand the publishing to a thread:

.. code-block:: python

    await asyncio.to_thread(lambda: group(other.s(i) for i in ids).apply_async())

Time limits
===========

:setting:`task_time_limit` and :setting:`task_soft_time_limit` are enforced by
cancelling the coroutine, which surfaces inside it as
:exc:`asyncio.CancelledError` at whatever it is currently awaiting.

When the soft limit expires the task is cancelled and, unless it handles the
cancellation, fails with :exc:`~celery.exceptions.SoftTimeLimitExceeded`.  A
task that wants to clean up can catch the cancellation exactly as it would
anywhere else in asyncio:

.. code-block:: python

    @app.task(soft_time_limit=10)
    async def transfer():
        try:
            return await do_the_work()
        except asyncio.CancelledError:
            await roll_back()
            raise

When the hard limit expires the pool stops waiting and the task fails with
:exc:`~celery.exceptions.TimeLimitExceeded`.

.. warning::

    A coroutine that swallows :exc:`asyncio.CancelledError` without re-raising
    cannot be stopped: unlike ``prefork``, this pool has no child process to
    kill.  Such a task keeps running on the loop until it returns of its own
    accord, even though the worker has already reported it as failed.  The
    same applies at shutdown, where the pool gives cancelled coroutines
    ``cancel_grace`` seconds to unwind before abandoning them.

Revoking tasks
==============

:meth:`~celery.app.control.Control.revoke` with ``terminate=True`` cancels the
coroutine of a running task, with the same semantics as the time limits above:
the cancellation is delivered at the task's next await point.

Synchronous task bodies cannot be terminated by this pool -- there is no safe
way to interrupt a running Python function in a thread.  A revoke that arrives
before the task starts is still honoured, as it is for every pool.

Unsupported options
===================

The following have no effect with this pool, because they are properties of
the ``prefork`` pool's child processes:

- :option:`--max-tasks-per-child <celery worker --max-tasks-per-child>`
- :option:`--max-memory-per-child <celery worker --max-memory-per-child>`
- :control:`pool_restart` and :option:`--autoscale
  <celery worker --autoscale>`

Coroutine tasks on the other pools
==================================

A coroutine task submitted to a worker pool that has no event loop --
``prefork``, ``threads``, or ``solo`` -- fails with
:exc:`~celery.exceptions.ImproperlyConfigured`, naming this pool.  That is
deliberate: before this existed such a task reported ``SUCCESS`` without its
body ever having run.

Setting :setting:`worker_resolve_coroutines` to ``True`` runs them anyway, on
a private event loop created for that one task and thrown away afterwards.
It is a compatibility shim: with a loop per task, nothing loop-bound survives
between tasks, which is the whole point of this pool.

Tasks running eagerly
=====================

Under :setting:`task_always_eager`, or when calling
:meth:`~celery.app.task.Task.apply`, a coroutine task always runs, on a
private event loop created for that one call, whether or not
:setting:`worker_resolve_coroutines` is set.  There is no worker there, so no
pool could ever have owned a shared loop for it either way, and refusing
would break every test suite that runs a coroutine task under
``task_always_eager``.

A coroutine task must not call another task eagerly from inside the coroutine:
waiting for the second task would block the loop the first one is running on,
so the pool refuses it with a :exc:`RuntimeError`.  Await the other coroutine
directly instead.
