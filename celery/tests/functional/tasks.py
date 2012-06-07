from __future__ import absolute_import

import time

from celery import task, subtask


@task()
def add(x, y):
    return x + y


@task()
def add_cb(x, y, callback=None):
    result = x + y
    if callback:
        return subtask(callback).apply_async(result)
    return result


@task()
def sleeptask(i):
    time.sleep(i)
    return i
