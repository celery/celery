# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

import os
import sys
import signal

from kombu import Exchange, Queue
from time import sleep

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded

CSTRESS_QUEUE = os.environ.get('CSTRESS_QUEUE_NAME', 'c.stress')
CSTRESS_BROKER = os.environ.get('CSTRESS_BROKER', 'amqp://')
CSTRESS_BACKEND = os.environ.get('CSTRESS_BACKEND', 'redis://')
CSTRESS_PREFETCH = int(os.environ.get('CSTRESS_PREFETCH', 1))

app = Celery(
    'stress', broker=CSTRESS_BROKER, backend=CSTRESS_BACKEND,
    set_as_current=False,
)
app.conf.update(
    CELERYD_PREFETCH_MULTIPLIER=CSTRESS_PREFETCH,
    CELERY_DEFAULT_QUEUE=CSTRESS_QUEUE,
    CELERY_QUEUES=(
        Queue(CSTRESS_QUEUE,
              exchange=Exchange(CSTRESS_QUEUE),
              routing_key=CSTRESS_QUEUE),
    ),
)


@app.task
def _marker(s, sep='-'):
    print('{0} {1} {2}'.format(sep * 3, s, sep * 3))


@app.task
def add(x, y):
    return x + y


@app.task
def any_(*args, **kwargs):
    wait = kwargs.get('sleep')
    if wait:
        sleep(wait)


@app.task
def exiting(status=0):
    sys.exit(status)


@app.task
def kill(sig=signal.SIGKILL):
    os.kill(os.getpid(), sig)


@app.task
def sleeping(i):
    sleep(i)


@app.task
def sleeping_ignore_limits(i):
    try:
        sleep(i)
    except SoftTimeLimitExceeded:
        sleep(i)


@app.task
def segfault():
    import ctypes
    ctypes.memset(0, 0, 1)
    assert False, 'should not get here'


def marker(s, sep='-'):
    print('{0}{1}'.format(sep, s))
    while True:
        try:
            return _marker.delay(s, sep)
        except Exception as exc:
            print("Retrying marker.delay(). It failed to start: %s" % exc)
