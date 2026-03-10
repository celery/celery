"""Warning messages and detection utilities for Celery."""

__all__ = (
    'W_GEVENT_MONKEY_PATCHED_WITH_WRONG_POOL',
    'W_EVENTLET_MONKEY_PATCHED_WITH_WRONG_POOL',
    'is_gevent_monkey_patched',
    'is_eventlet_monkey_patched',
)

W_GEVENT_MONKEY_PATCHED_WITH_WRONG_POOL = """\
Detected that gevent monkey patching has been applied, but the worker pool \
is set to %r instead of 'gevent'. This configuration is likely incorrect \
and may cause the worker to hang or block indefinitely while waiting for pool \
worker processes to start.

To fix this, either:
  1. Use the gevent pool by starting the worker with: celery worker -P gevent
  2. Remove the gevent.monkey.patch_all() call from your code"""

W_EVENTLET_MONKEY_PATCHED_WITH_WRONG_POOL = """\
Detected that eventlet monkey patching has been applied, but the worker pool \
is set to %r instead of 'eventlet'. This configuration is likely incorrect \
and may cause the worker to hang or block indefinitely while waiting for pool \
worker processes to start.

To fix this, either:
  1. Use the eventlet pool by starting the worker with: celery worker -P eventlet
  2. Remove the eventlet.monkey_patch() call from your code"""


def is_gevent_monkey_patched():
    """Detect if gevent monkey patching has been applied.

    Returns True if gevent.monkey.patch_all() or similar has been called,
    False otherwise.
    """
    try:
        from gevent import monkey

        # Check if the socket module has been patched (common indicator)
        return monkey.is_module_patched('socket')
    except ImportError:
        return False


def is_eventlet_monkey_patched():
    """Detect if eventlet monkey patching has been applied.

    Returns True if eventlet.monkey_patch() or similar has been called,
    False otherwise.
    """
    try:
        from eventlet import patcher

        # Check if the socket module has been patched (common indicator)
        return patcher.is_monkey_patched('socket')
    except ImportError:
        return False


def is_gevent_pool(pool_module: str) -> bool:
    return pool_module == 'celery.concurrency.gevent'


def is_eventlet_pool(pool_module: str) -> bool:
    return pool_module == 'celery.concurrency.eventlet'
