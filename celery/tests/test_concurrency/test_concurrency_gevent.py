from __future__ import absolute_import

from nose import SkipTest

from celery.concurrency.gevent import TaskPool
from celery.tests.utils import unittest


class GeventCase(unittest.TestCase):

    def setUp(self):
        try:
            self.gevent = __import__("gevent")
        except ImportError:
            raise SkipTest(
                "gevent not installed, skipping related tests.")


class test_TaskPool(GeventCase):

    def test_grow(self):
        pool = TaskPool(10)
        pool.start()
        self.assertEqual(pool._pool.size, 10)
        pool.grow()
        self.assertEqual(pool._pool.size, 11)

    def test_grow_many(self):
        pool = TaskPool(10)
        pool.start()
        self.assertEqual(pool._pool.size, 10)
        pool.grow(2)
        self.assertEqual(pool._pool.size, 12)

    def test_shrink(self):
        pool = TaskPool(10)
        pool.start()
        self.assertEqual(pool._pool.size, 10)
        pool.shrink()
        self.assertEqual(pool._pool.size, 9)

    def test_shrink_many(self):
        pool = TaskPool(10)
        pool.start()
        self.assertEqual(pool._pool.size, 10)
        pool.shrink(2)
        self.assertEqual(pool._pool.size, 8)

    def test_num_processes(self):
        pool = TaskPool(10)
        pool.start()
        pool.apply_async(lambda x: x, (2, ), {})
        self.assertEqual(pool.num_processes, 1)
