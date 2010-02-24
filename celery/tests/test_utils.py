from __future__ import with_statement

import sys
import socket
import unittest

from billiard.utils.functional import wraps

from celery import utils
from celery.tests.utils import sleepdeprived


class TestChunks(unittest.TestCase):

    def test_chunks(self):

        # n == 2
        x = utils.chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2)
        self.assertEquals(list(x),
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]])

        # n == 3
        x = utils.chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 3)
        self.assertEquals(list(x),
            [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]])

        # n == 2 (exact)
        x = utils.chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 2)
        self.assertEquals(list(x),
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]])


class TestGenUniqueId(unittest.TestCase):

    def test_gen_unique_id_without_ctypes(self):
        from celery.tests.utils import mask_modules
        old_utils = sys.modules.pop("celery.utils")
        try:
            mask_modules("ctypes")
            from celery.utils import ctypes, gen_unique_id
            self.assertTrue(ctypes is None)
            uuid = gen_unique_id()
            self.assertTrue(uuid)
            self.assertTrue(isinstance(uuid, basestring))
        finally:
            sys.modules["celery.utils"] = old_utils


class TestDivUtils(unittest.TestCase):

    def test_repeatlast(self):
        items = range(6)
        it = utils.repeatlast(items)
        for i in items:
            self.assertEquals(it.next(), i)
        for j in items:
            self.assertEquals(it.next(), i)


class TestRetryOverTime(unittest.TestCase):

    def test_returns_retval_on_success(self):

        def _fun(x, y):
            return x * y

        ret = utils.retry_over_time(_fun, (socket.error, ), args=[16, 16],
                                    max_retries=3)

        self.assertEquals(ret, 256)

    @sleepdeprived
    def test_raises_on_unlisted_exception(self):

        def _fun(x, y):
            raise KeyError("bar")

        self.assertRaises(KeyError, utils.retry_over_time, _fun,
                         (socket.error, ), args=[32, 32], max_retries=3)

    @sleepdeprived
    def test_retries_on_failure(self):

        iterations = [0]

        def _fun(x, y):
            iterations[0] += 1
            if iterations[0] == 3:
                return x * y
            raise socket.error("foozbaz")

        ret = utils.retry_over_time(_fun, (socket.error, ), args=[32, 32],
                                    max_retries=None)

        self.assertEquals(iterations[0], 3)
        self.assertEquals(ret, 1024)

        self.assertRaises(socket.error, utils.retry_over_time,
                        _fun, (socket.error, ), args=[32, 32], max_retries=1)
