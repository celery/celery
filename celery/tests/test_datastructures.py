import unittest
import sys

from celery.datastructures import PositionQueue, ExceptionInfo


class TestPositionQueue(unittest.TestCase):

    def test_position_queue_unfilled(self):
        q = PositionQueue(length=10)
        for position in q.data:
            self.assertTrue(isinstance(position, q.UnfilledPosition))

        self.assertEquals(q.filled, [])
        self.assertEquals(len(q), 0)
        self.assertFalse(q.full())
        
    def test_position_queue_almost(self):
        q = PositionQueue(length=10)
        q[3] = 3
        q[6] = 6
        q[9] = 9

        self.assertEquals(q.filled, [3, 6, 9])
        self.assertEquals(len(q), 3)
        self.assertFalse(q.full())
    
    def test_position_queue_full(self):
        q = PositionQueue(length=10)
        for i in xrange(10):
            q[i] = i
        self.assertEquals(q.filled, list(xrange(10)))
        self.assertEquals(len(q), 10)
        self.assertTrue(q.full())


class TestExceptionInfo(unittest.TestCase):

    def test_exception_info(self):

        try:
            raise LookupError("The quick brown fox jumps...")
        except LookupError:
            exc_info = sys.exc_info()

        einfo = ExceptionInfo(exc_info)
        self.assertEquals(str(einfo), "The quick brown fox jumps...")
        self.assertTrue(isinstance(einfo.exception, LookupError))
        self.assertEquals(einfo.exception.args,
                ("The quick brown fox jumps...", ))
        self.assertTrue(einfo.traceback)
