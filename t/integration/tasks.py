# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from time import sleep

from celery import group, shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@shared_task
def add(x, y):
    """Add two numbers."""
    return x + y


@shared_task(bind=True)
def add_replaced(self, x, y):
    """Add two numbers (via the add task)."""
    raise self.replace(add.s(x, y))


@shared_task(bind=True)
def add_to_all(self, nums, val):
    """Add the given value to all supplied numbers."""
    subtasks = [add.s(num, val) for num in nums]
    raise self.replace(group(*subtasks))


@shared_task
def print_unicode(log_message='håå®ƒ valmuefrø', print_message='hiöäüß'):
    """Task that both logs and print strings containing funny characters."""
    logger.warning(log_message)
    print(print_message)


@shared_task
def sleeping(i, **_):
    """Task sleeping for ``i`` seconds, and returning nothing."""
    sleep(i)


@shared_task(bind=True)
def ids(self, i):
    """Returns a tuple of ``root_id``, ``parent_id`` and
    the argument passed as ``i``."""
    return self.request.root_id, self.request.parent_id, i


@shared_task(bind=True)
def collect_ids(self, res, i):
    """Used as a callback in a chain or group where the previous tasks
    are :task:`ids`: returns a tuple of::

        (previous_result, (root_id, parent_id, i))

    """
    return res, (self.request.root_id, self.request.parent_id, i)


@shared_task(bind=True, expires=60.0, max_retries=1)
def retry_once(self):
    """Task that fails and is retried. Returns the number of retries."""
    if self.request.retries:
        return self.request.retries
    raise self.retry(countdown=0.1)


@shared_task
def redis_echo(message):
    """Task that appends the message to a redis list"""
    from redis import StrictRedis

    redis_connection = StrictRedis()
    redis_connection.rpush('redis-echo', message)
