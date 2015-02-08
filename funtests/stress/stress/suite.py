from __future__ import absolute_import, print_function, unicode_literals

import inspect
import platform
import random
import socket
import sys

from collections import OrderedDict, defaultdict, namedtuple
from itertools import count
from time import sleep

from celery import group, VERSION_BANNER
from celery.exceptions import TimeoutError
from celery.five import items, monotonic, range, values
from celery.utils.debug import blockdetection
from celery.utils.text import pluralize, truncate
from celery.utils.timeutils import humanize_seconds

from .app import (
    marker, _marker, add, any_, exiting, kill, sleeping,
    sleeping_ignore_limits, any_returning
)
from .data import BIG, SMALL
from .fbi import FBI

BANNER = """\
Celery stress-suite v{version}

{platform}

[config]
.> broker: {conninfo}

[toc: {total} {TESTS} total]
{toc}
"""

F_PROGRESS = """\
{0.index}: {0.test.__name__}({0.iteration}/{0.total_iterations}) \
rep#{0.repeats} runtime: {runtime}/{elapsed} \
"""

Progress = namedtuple('Progress', (
    'test', 'iteration', 'total_iterations',
    'index', 'repeats', 'runtime', 'elapsed', 'completed',
))


Inf = float('Inf')


class StopSuite(Exception):
    pass


def pstatus(p):
    return F_PROGRESS.format(
        p,
        runtime=humanize_seconds(monotonic() - p.runtime, now='0 seconds'),
        elapsed=humanize_seconds(monotonic() - p.elapsed, now='0 seconds'),
    )


class Speaker(object):

    def __init__(self, gap=5.0):
        self.gap = gap
        self.last_noise = monotonic() - self.gap * 2

    def beep(self):
        now = monotonic()
        if now - self.last_noise >= self.gap:
            self.emit()
            self.last_noise = now

    def emit(self):
        print('\a', file=sys.stderr, end='')


def testgroup(*funs):
    return OrderedDict((fun.__name__, fun) for fun in funs)


class BaseSuite(object):

    def __init__(self, app, block_timeout=30 * 60):
        self.app = app
        self.connerrors = self.app.connection().recoverable_connection_errors
        self.block_timeout = block_timeout
        self.progress = None
        self.speaker = Speaker()
        self.fbi = FBI(app)
        self.init_groups()

    def init_groups(self):
        acc = defaultdict(list)
        for attr in dir(self):
            if not _is_descriptor(self, attr):
                meth = getattr(self, attr)
                try:
                    groups = meth.__func__.__testgroup__
                except AttributeError:
                    pass
                else:
                    for g in groups:
                        acc[g].append(meth)
        # sort the tests by the order in which they are defined in the class
        for g in values(acc):
            g[:] = sorted(g, key=lambda m: m.__func__.__testsort__)
        self.groups = dict(
            (name, testgroup(*tests)) for name, tests in items(acc)
        )

    def run(self, names=None, iterations=50, offset=0,
            numtests=None, list_all=False, repeat=0, group='all',
            diag=False, no_join=False, **kw):
        self.no_join = no_join
        self.fbi.enable(diag)
        tests = self.filtertests(group, names)[offset:numtests or None]
        if list_all:
            return print(self.testlist(tests))
        print(self.banner(tests))
        print('+ Enabling events')
        self.app.control.enable_events()
        it = count() if repeat == Inf else range(int(repeat) or 1)
        for i in it:
            marker(
                'Stresstest suite start (repetition {0})'.format(i + 1),
                '+',
            )
            for j, test in enumerate(tests):
                self.runtest(test, iterations, j + 1, i + 1)
            marker(
                'Stresstest suite end (repetition {0})'.format(i + 1),
                '+',
            )

    def filtertests(self, group, names):
        tests = self.groups[group]
        try:
            return ([tests[n] for n in names] if names
                    else list(values(tests)))
        except KeyError as exc:
            raise KeyError('Unknown test name: {0}'.format(exc))

    def testlist(self, tests):
        return ',\n'.join(
            '.> {0}) {1}'.format(i + 1, t.__name__)
            for i, t in enumerate(tests)
        )

    def banner(self, tests):
        app = self.app
        return BANNER.format(
            app='{0}:0x{1:x}'.format(app.main or '__main__', id(app)),
            version=VERSION_BANNER,
            conninfo=app.connection().as_uri(),
            platform=platform.platform(),
            toc=self.testlist(tests),
            TESTS=pluralize(len(tests), 'test'),
            total=len(tests),
        )

    def runtest(self, fun, n=50, index=0, repeats=1):
        print('{0}: [[[{1}({2})]]]'.format(repeats, fun.__name__, n))
        with blockdetection(self.block_timeout):
            with self.fbi.investigation():
                runtime = elapsed = monotonic()
                i = 0
                failed = False
                self.progress = Progress(
                    fun, i, n, index, repeats, elapsed, runtime, 0,
                )
                _marker.delay(pstatus(self.progress))
                try:
                    for i in range(n):
                        runtime = monotonic()
                        self.progress = Progress(
                            fun, i + 1, n, index, repeats, runtime, elapsed, 0,
                        )
                        try:
                            fun()
                        except StopSuite:
                            raise
                        except Exception as exc:
                            print('-> {0!r}'.format(exc))
                            print(pstatus(self.progress))
                        else:
                            print(pstatus(self.progress))
                except Exception:
                    failed = True
                    self.speaker.beep()
                    raise
                finally:
                    print('{0} {1} iterations in {2}s'.format(
                        'failed after' if failed else 'completed',
                        i + 1, humanize_seconds(monotonic() - elapsed),
                    ))
                    if not failed:
                        self.progress = Progress(
                            fun, i + 1, n, index, repeats, runtime, elapsed, 1,
                        )

    def missing_results(self, r):
        return [res.id for res in r if res.id not in res.backend._cache]

    def join(self, r, propagate=False, max_retries=10, **kwargs):
        if self.no_join:
            return
        received = []

        def on_result(task_id, value):
            received.append(task_id)

        for i in range(max_retries) if max_retries else count(0):
            received[:] = []
            try:
                return r.get(callback=on_result, propagate=propagate, **kwargs)
            except (socket.timeout, TimeoutError) as exc:
                waiting_for = self.missing_results(r)
                self.speaker.beep()
                marker(
                    'Still waiting for {0}/{1}: [{2}]: {3!r}'.format(
                        len(r) - len(received), len(r),
                        truncate(', '.join(waiting_for)), exc), '!',
                )
                self.fbi.diag(waiting_for)
            except self.connerrors as exc:
                self.speaker.beep()
                marker('join: connection lost: {0!r}'.format(exc), '!')
        raise StopSuite('Test failed: Missing task results')

    def dump_progress(self):
        return pstatus(self.progress) if self.progress else 'No test running'


_creation_counter = count(0)


def testcase(*groups):
    if not groups:
        raise ValueError('@testcase requires at least one group name')

    def _mark_as_case(fun):
        fun.__testgroup__ = groups
        fun.__testsort__ = next(_creation_counter)
        return fun

    return _mark_as_case


def _is_descriptor(obj, attr):
    try:
        cattr = getattr(obj.__class__, attr)
    except AttributeError:
        pass
    else:
        return not inspect.ismethod(cattr) and hasattr(cattr, '__get__')
    return False


class Suite(BaseSuite):

    @testcase('all', 'green')
    def manyshort(self):
        self.join(group(add.s(i, i) for i in range(1000))(),
                  timeout=10, propagate=True)

    @testcase('all')
    def always_timeout(self):
        self.join(
            group(sleeping.s(1).set(time_limit=0.1)
                  for _ in range(100)
            )(),
            timeout=10, propagate=True,
        )

    @testcase('all')
    def termbysig(self):
        self._evil_groupmember(kill)

    @testcase('green')
    def group_with_exit(self):
        self._evil_groupmember(exiting)

    @testcase('all')
    def timelimits(self):
        self._evil_groupmember(sleeping, 2, time_limit=1)

    @testcase('all')
    def timelimits_soft(self):
        self._evil_groupmember(sleeping_ignore_limits, 2,
                               soft_time_limit=1, time_limit=1.1)

    @testcase('all')
    def alwayskilled(self):
        g = group(kill.s() for _ in range(10))
        self.join(g(), timeout=10)

    @testcase('all', 'green')
    def alwaysexits(self):
        g = group(exiting.s() for _ in range(10))
        self.join(g(), timeout=10)

    def _evil_groupmember(self, evil_t, *eargs, **opts):
        g1 = group(add.s(2, 2).set(**opts), evil_t.s(*eargs).set(**opts),
                   add.s(4, 4).set(**opts), add.s(8, 8).set(**opts))
        g2 = group(add.s(3, 3).set(**opts), add.s(5, 5).set(**opts),
                   evil_t.s(*eargs).set(**opts), add.s(7, 7).set(**opts))
        self.join(g1(), timeout=10)
        self.join(g2(), timeout=10)

    @testcase('all', 'green')
    def bigtasksbigvalue(self):
        g = group(any_returning.s(BIG, sleep=0.3) for i in range(8))
        r = g()
        try:
            self.join(r, timeout=10)
        finally:
            # very big values so remove results from backend
            try:
                r.forget()
            except NotImplementedError:
                pass

    @testcase('all', 'green')
    def bigtasks(self, wait=None):
        self._revoketerm(wait, False, False, BIG)

    @testcase('all', 'green')
    def smalltasks(self, wait=None):
        self._revoketerm(wait, False, False, SMALL)

    @testcase('all')
    def revoketermfast(self, wait=None):
        self._revoketerm(wait, True, False, SMALL)

    @testcase('all')
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
        self.join(r, timeout=10)
