"""Tasks auto-retry functionality."""
from inspect import iscoroutine

from vine.utils import wraps

from celery.exceptions import Ignore, Retry
from celery.utils.time import get_exponential_backoff_interval


def add_autoretry_behaviour(task, **options):
    """Wrap task's `run` method with auto-retry functionality."""
    autoretry_for = tuple(
        options.get('autoretry_for',
                    getattr(task, 'autoretry_for', ()))
    )
    dont_autoretry_for = tuple(
        options.get('dont_autoretry_for',
                    getattr(task, 'dont_autoretry_for', ()))
    )
    retry_kwargs = options.get(
        'retry_kwargs', getattr(task, 'retry_kwargs', {})
    )
    retry_backoff = float(
        options.get('retry_backoff',
                    getattr(task, 'retry_backoff', False))
    )
    retry_backoff_max = int(
        options.get('retry_backoff_max',
                    getattr(task, 'retry_backoff_max', 600))
    )
    retry_jitter = options.get(
        'retry_jitter', getattr(task, 'retry_jitter', True)
    )

    if autoretry_for and not hasattr(task, '_orig_run'):

        def retry_for(exc, retry_kwargs, retry_backoff, retry_backoff_max,
                      retry_jitter):
            """Build the :exc:`~@Retry` this exception should be retried by.

            Everything is passed in rather than closed over so that it all
            stays a free variable of the wrapper below: the regression test
            from issue 10456 introspects that closure, and so might other
            code.
            """
            retry_kwargs_for_attempt = retry_kwargs.copy()
            if retry_backoff:
                retry_kwargs_for_attempt['countdown'] = \
                    get_exponential_backoff_interval(
                        factor=int(max(1.0, retry_backoff)),
                        retries=task.request.retries,
                        maximum=retry_backoff_max,
                        full_jitter=retry_jitter)
            # Override max_retries
            if hasattr(task, 'override_max_retries'):
                retry_kwargs_for_attempt['max_retries'] = getattr(
                    task, 'override_max_retries', task.max_retries)
            ret = task.retry(exc=exc, **retry_kwargs_for_attempt)
            # Stop propagation
            if hasattr(task, 'override_max_retries'):
                delattr(task, 'override_max_retries')
            return ret

        async def await_and_retry_for(coro, retry_kwargs):
            """The same handling, for a body that has yet to be awaited.

            A synchronous wrapper alone would only ever see the coroutine
            being built; the exception it raises arrives later, when the
            tracer awaits it, and would miss the handlers below.
            """
            try:
                return await coro
            except Ignore:
                raise
            except Retry:
                raise
            except dont_autoretry_for:
                raise
            except autoretry_for as exc:
                raise retry_for(exc, retry_kwargs, retry_backoff,
                                retry_backoff_max, retry_jitter)

        @wraps(task.run)
        def run(*args, **kwargs):
            try:
                ret = task._orig_run(*args, **kwargs)
            except Ignore:
                # If Ignore signal occurs task shouldn't be retried,
                # even if it suits autoretry_for list
                raise
            except Retry:
                raise
            except dont_autoretry_for:
                raise
            except autoretry_for as exc:
                raise retry_for(exc, retry_kwargs, retry_backoff,
                                retry_backoff_max, retry_jitter)
            # An ``async def`` body -- or a synchronous one that returns a
            # coroutine -- has not run yet.  Decided on what came back rather
            # than on how the task was declared, which is what the tracer
            # does, and which no static test of the function can get right
            # once a decorator sits in between.
            if iscoroutine(ret):
                return await_and_retry_for(ret, retry_kwargs)
            return ret

        task._orig_run, task.run = task.run, run
