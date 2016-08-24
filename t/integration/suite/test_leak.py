from __future__ import absolute_import, print_function, unicode_literals

import gc
import os
import sys
import shlex
import subprocess
import unittest

from case import Case
from case.skip import SkipTest

from celery import current_app
from celery.five import range

import suite  # noqa

GET_RSIZE = b'/bin/ps -p {pid} -o rss='


class Sizes(list):

    def add(self, item):
        if item not in self:
            self.append(item)

    def average(self):
        return sum(self) / len(self)


class LeakFunCase(Case):

    def setUp(self):
        self.app = current_app
        self.debug = os.environ.get('TEST_LEAK_DEBUG', False)

    def get_rsize(self, cmd=GET_RSIZE):
        try:
            return int(subprocess.Popen(
                shlex.split(cmd.format(pid=os.getpid())),
                stdout=subprocess.PIPE).communicate()[0].strip()
            )
        except OSError as exc:
            raise SkipTest(
                'Cannot execute command: {0!r}: {1!r}'.format(cmd, exc))

    def sample_allocated(self, fun, *args, **kwargs):
        before = self.get_rsize()

        fun(*args, **kwargs)
        gc.collect()
        after = self.get_rsize()
        return before, after

    def appx(self, s, r=1):
        """r==1 (10e1): Keep up to hundred kB (e.g., 16,268MB
        becomes 16,2MB)."""
        return int(s / 10.0 ** (r + 1)) / 10.0

    def assertFreed(self, n, fun, *args, **kwargs):
        # call function first to load lazy modules etc.
        fun(*args, **kwargs)

        try:
            base = self.get_rsize()
            first = None
            sizes = Sizes()
            for i in range(n):
                before, after = self.sample_allocated(fun, *args, **kwargs)
                if not first:
                    first = after
                if self.debug:
                    print('{0!r} {1}: before/after: {2}/{3}'.format(
                          fun, i, before, after))
                else:
                    sys.stderr.write('.')
                sizes.add(self.appx(after))
            self.assertEqual(gc.collect(), 0)
            self.assertEqual(gc.garbage, [])
            try:
                assert self.appx(first) >= self.appx(after)
            except AssertionError:
                print('base: {0!r} avg: {1!r} sizes: {2!r}'.format(
                    base, sizes.average(), sizes))
                raise
        finally:
            self.app.control.purge()


class test_leaks(LeakFunCase):

    def test_task_apply_leak(self, its=1000):
        self.assertNotEqual(self.app.conf.BROKER_TRANSPORT, 'memory')

        @self.app.task
        def task1():
            pass

        try:
            pool_limit = self.app.conf.BROKER_POOL_LIMIT
        except AttributeError:
            return self.assertFreed(self.iterations, task1.delay)

        self.app.conf.BROKER_POOL_LIMIT = None
        try:
            self.app._pool = None
            self.assertFreed(its, task1.delay)
        finally:
            self.app.conf.BROKER_POOL_LIMIT = pool_limit

    def test_task_apply_leak_with_pool(self, its=1000):
        self.assertNotEqual(self.app.conf.BROKER_TRANSPORT, 'memory')

        @self.app.task
        def task2():
            pass

        try:
            pool_limit = self.app.conf.BROKER_POOL_LIMIT
        except AttributeError:
            raise SkipTest('This version does not support autopool')

        self.app.conf.BROKER_POOL_LIMIT = 10
        try:
            self.app._pool = None
            self.assertFreed(its, task2.delay)
        finally:
            self.app.conf.BROKER_POOL_LIMIT = pool_limit


if __name__ == '__main__':
    unittest.main()
