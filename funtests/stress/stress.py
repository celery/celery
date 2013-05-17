from __future__ import absolute_import

import random
import os
import signal
import sys

from time import time, sleep

from celery import Celery, group
from celery.exceptions import TimeoutError, SoftTimeLimitExceeded
from celery.five import range
from celery.utils.debug import blockdetection

# Should be run with workers running using these options:
#
#  1) celery -A stress worker -c 1 --maxtasksperchild=1
#  2) celery -A stress worker -c 8 --maxtasksperchild=1
#
#  3) celery -A stress worker -c 1
#  4) celery -A stress worker -c 8
#
#  5) celery -A stress worker --autoscale=8,0
#
#  6) celery -A stress worker --time-limit=1
#
#  7) celery -A stress worker -c1 --maxtasksperchild=1 -- celery.acks_late=1

BIG = 'x' * 2 ** 20 * 8
SMALL = 'e' * 1024

celery = Celery(
    'stress', broker='pyamqp://', backend='redis://',
    set_as_current=False,
)
celery.conf.update(
    CELERYD_PREFETCH_MULTIPLIER=1,
)


@celery.task
def _marker(s, sep='-'):
    print('{0} {1} {2}'.format(sep * 3, s, sep * 3))


@celery.task
def add(x, y):
    return x + y


@celery.task
def any_(*args, **kwargs):
    wait = kwargs.get('sleep')
    if wait:
        sleep(wait)


@celery.task
def exiting(status=0):
    sys.exit(status)


@celery.task
def kill(sig=signal.SIGKILL):
    os.kill(os.getpid(), sig)


@celery.task
def sleeping(i):
    sleep(i)


@celery.task
def sleeping_ignore_limits(i):
    try:
        sleep(i)
    except SoftTimeLimitExceeded:
        sleep(i)


@celery.task
def segfault():
    import ctypes
    ctypes.memset(0, 0, 1)
    assert False, 'should not get here'


def marker(s, sep='-'):
    print('{0}{1}'.format(sep, s))
    _marker.delay(s, sep)


class Stresstests(object):

    def __init__(self, app, block_timeout=30 * 60):
        self.app = app
        self.connerrors = self.app.connection().recoverable_connection_errors
        self.block_timeout = block_timeout

    def run(self, n=50):
        marker('Stresstest suite start', '+')
        tests = [self.manyshort,
                 self.termbysig,
                 self.bigtasks,
                 self.smalltasks,
                 self.timelimits,
                 self.timelimits_soft,
                 self.revoketermfast,
                 self.revoketermslow]
        for test in tests:
            self.runtest(test, n)
        marker('Stresstest suite end', '+')

    def manyshort(self):
        self.join(group(add.s(i, i) for i in xrange(1000))())

    def runtest(self, fun, n=50):
        with blockdetection(self.block_timeout):
            t = time()
            i = 0
            failed = False
            marker('{0}({1})'.format(fun.__name__, n))
            try:
                for i in range(n):
                    print(i)
                    fun()
            except Exception:
                failed = True
                raise
            finally:
                print('{0} {1} iterations in {2}s'.format(
                    'failed after' if failed else 'completed',
                    i + 1, time() - t,
                ))

    def termbysig(self):
        self._evil_groupmember(kill)

    def termbysegfault(self):
        self._evil_groupmember(segfault)

    def timelimits(self):
        self._evil_groupmember(sleeping, 2, timeout=1)

    def timelimits_soft(self):
        self._evil_groupmember(sleeping_ignore_limits, 2,
                               soft_timeout=1, timeout=1.1)

    def _evil_groupmember(self, evil_t, *eargs, **opts):
        g1 = group(add.s(2, 2).set(**opts), evil_t.s(*eargs).set(**opts),
                   add.s(4, 4).set(**opts), add.s(8, 8).set(**opts))
        g2 = group(add.s(3, 3).set(**opts), add.s(5, 5).set(**opts),
                   evil_t.s(*eargs).set(**opts), add.s(7, 7).set(**opts))
        self.join(g1(), timeout=10)
        self.join(g2(), timeout=10)

    def bigtasks(self, wait=None):
        self._revoketerm(wait, False, False, BIG)

    def smalltasks(self, wait=None):
        self._revoketerm(wait, False, False, SMALL)

    def revoketermfast(self, wait=None):
        self._revoketerm(wait, True, False, SMALL)

    def revoketermslow(self, wait=5):
        self._revoketerm(wait, True, True, BIG)

    def _revoketerm(self, wait=None, terminate=True,
                    joindelay=True, data=BIG):
        g = group(any_.s(data, sleep=wait) for i in range(8))
        r = g()
        if terminate:
            if joindelay:
                sleep(random.choice(range(4)))
            r.revoke(terminate=True)
        self.join(r, timeout=100)

    def join(self, r, **kwargs):
        while 1:
            try:
                return r.get(propagate=False, **kwargs)
            except TimeoutError as exc:
                print('join timed out: %s' % (exc, ))
            except self.connerrors as exc:
                print('join: connection lost: %r' % (exc, ))


if __name__ == '__main__':
    s = Stresstests(celery)
    s.run()
