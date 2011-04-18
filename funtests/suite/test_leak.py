import gc
import os
import sys
import shlex
import subprocess

sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), os.pardir))

from nose import SkipTest

from celery import current_app
from celery.tests.utils import unittest

import suite

GET_RSIZE = "/bin/ps -p %(pid)s -o rss="
QUICKTEST = int(os.environ.get("QUICKTEST", 0))


class Sizes(list):

    def add(self, item):
        if item not in self:
            self.append(item)

    def average(self):
        return sum(self) / len(self)


class LeakFunCase(unittest.TestCase):

    def setUp(self):
        self.app = current_app
        self.debug = os.environ.get("TEST_LEAK_DEBUG", False)

    def get_rsize(self, cmd=GET_RSIZE):
        try:
            return int(subprocess.Popen(
                        shlex.split(cmd % {"pid": os.getpid()}),
                            stdout=subprocess.PIPE).communicate()[0].strip())
        except OSError, exc:
            raise SkipTest("Can't execute command: %r: %r" % (cmd, exc))

    def sample_allocated(self, fun, *args, **kwargs):
        before = self.get_rsize()

        fun(*args, **kwargs)
        gc.collect()
        after = self.get_rsize()
        return before, after

    def appx(self, s, r=1):
        """r==1 (10e1): Keep up to hundred kB,
        e.g. 16,268MB becomes 16,2MB."""
        return int(s / 10.0 ** (r + 1)) / 10.0

    def assertFreed(self, n, fun, *args, **kwargs):
        # call function first to load lazy modules etc.
        fun(*args, **kwargs)

        try:
            base = self.get_rsize()
            first = None
            sizes = Sizes()
            for i in xrange(n):
                before, after = self.sample_allocated(fun, *args, **kwargs)
                if not first:
                    first = after
                if self.debug:
                    print("%r %s: before/after: %s/%s" % (
                            fun, i, before, after))
                else:
                    sys.stderr.write(".")
                sizes.add(self.appx(after))
            self.assertEqual(gc.collect(), 0)
            self.assertEqual(gc.garbage, [])
            try:
                assert self.appx(first) >= self.appx(after)
            except AssertionError:
                print("BASE: %r AVG: %r SIZES: %r" % (
                    base, sizes.average(), sizes, ))
                raise
        finally:
            self.app.control.discard_all()


class test_leaks(LeakFunCase):

    def test_task_apply_leak(self):
        its = QUICKTEST and 10 or 1000
        self.assertNotEqual(self.app.conf.BROKER_TRANSPORT, "memory")

        @self.app.task
        def task1():
            pass

        try:
            pool_limit = self.app.conf.BROKER_POOL_LIMIT
        except AttributeError:
            return self.assertFreed(self.iterations, foo.delay)

        self.app.conf.BROKER_POOL_LIMIT = None
        try:
            self.app._pool = None
            self.assertFreed(its, task1.delay)
        finally:
            self.app.conf.BROKER_POOL_LIMIT = pool_limit

    def test_task_apply_leak_with_pool(self):
        its = QUICKTEST and 10 or 1000
        self.assertNotEqual(self.app.conf.BROKER_TRANSPORT, "memory")

        @self.app.task
        def task2():
            pass

        try:
            pool_limit = self.app.conf.BROKER_POOL_LIMIT
        except AttributeError:
            raise SkipTest("This version does not support autopool")

        self.app.conf.BROKER_POOL_LIMIT = 10
        try:
            self.app._pool = None
            self.assertFreed(its, task2.delay)
        finally:
            self.app.conf.BROKER_POOL_LIMIT = pool_limit

if __name__ == "__main__":
    unittest.main()
