"""Tasks auto-retry functionality."""
import inspect

from vine.utils import wraps

from celery.exceptions import Ignore, Retry
from celery.utils.time import get_exponential_backoff_interval


def add_autoretry_behaviour(task, **options):
    """Wrap task's `run` method with auto-retry functionality."""
    _autoretry_for = tuple(
        options.get('autoretry_for',
                    getattr(task, 'autoretry_for', ()))
    )
    _retry_kwargs = options.get(
        'retry_kwargs', getattr(task, 'retry_kwargs', {})
    )
    _retry_backoff = int(
        options.get('retry_backoff',
                    getattr(task, 'retry_backoff', False))
    )
    _retry_backoff_max = int(
        options.get('retry_backoff_max',
                    getattr(task, 'retry_backoff_max', 600))
    )
    _retry_jitter = options.get(
        'retry_jitter', getattr(task, 'retry_jitter', True)
    )

    if not hasattr(task, '_orig_run'):
        arg_spec = inspect.getfullargspec(task.run)
        if 'autoretry_opts' in set(arg_spec.args):
            raise ValueError(
                "'autoretry_opts' is a reserved argument to override "
                "autoretry options. Use a different name for this "
                "parameter please."
            )

        @wraps(task.run)
        def run(*args, autoretry_opts=None, **kwargs):
            if autoretry_opts is None:
                autoretry_opts = {}
            autoretry_for = autoretry_opts.get('autoretry_for', _autoretry_for)
            retry_kwargs = autoretry_opts.get('retry_kwargs', _retry_kwargs)
            retry_backoff = autoretry_opts.get('retry_backoff',_retry_backoff)
            retry_backoff_max = autoretry_opts.get('retry_backoff_max',_retry_backoff_max)
            retry_jitter = autoretry_opts.get('retry_jitter', _retry_jitter)
            try:
                return task._orig_run(*args, **kwargs)
            except Ignore:
                # If Ignore signal occures task shouldn't be retried,
                # even if it suits autoretry_for list
                raise
            except Retry:
                raise
            except autoretry_for as exc:
                if retry_backoff:
                    retry_kwargs['countdown'] = \
                        get_exponential_backoff_interval(
                            factor=retry_backoff,
                            retries=task.request.retries,
                            maximum=retry_backoff_max,
                            full_jitter=retry_jitter)
                # Override max_retries
                if hasattr(task, 'override_max_retries'):
                    retry_kwargs['max_retries'] = getattr(task,
                                                          'override_max_retries',
                                                          task.max_retries)
                ret = task.retry(exc=exc, **retry_kwargs)
                # Stop propagation
                if hasattr(task, 'override_max_retries'):
                    delattr(task, 'override_max_retries')
                raise ret

        task._orig_run, task.run = task.run, run
