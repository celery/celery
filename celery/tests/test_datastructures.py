import unittest
import sys
from Queue import Queue

from celery.datastructures import PositionQueue, ExceptionInfo
from celery.datastructures import LimitedSet, consume_queue
from celery.datastructures import SharedCounter


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

        r = repr(einfo)
        self.assertTrue(r)


class TestUtilities(unittest.TestCase):

    def test_consume_queue(self):
        x = Queue()
        it = consume_queue(x)
        self.assertRaises(StopIteration, it.next)
        x.put("foo")
        it = consume_queue(x)
        self.assertEquals(it.next(), "foo")
        self.assertRaises(StopIteration, it.next)


class TestSharedCounter(unittest.TestCase):

    def test_initial_value(self):
        self.assertEquals(int(SharedCounter(10)), 10)

    def test_increment(self):
        c = SharedCounter(10)
        c.increment()
        self.assertEquals(int(c), 11)
        c.increment(2)
        self.assertEquals(int(c), 13)

    def test_decrement(self):
        c = SharedCounter(10)
        c.decrement()
        self.assertEquals(int(c), 9)
        c.decrement(2)
        self.assertEquals(int(c), 7)

    def test_iadd(self):
        c = SharedCounter(10)
        c += 10
        self.assertEquals(int(c), 20)

    def test_isub(self):
        c = SharedCounter(10)
        c -= 20
        self.assertEquals(int(c), -10)

    def test_repr(self):
        self.assertTrue(repr(SharedCounter(10)).startswith("<SharedCounter:"))


class TestLimitedSet(unittest.TestCase):

    def test_add(self):
        s = LimitedSet(maxlen=2)
        s.add("foo")
        s.add("bar")
        for n in "foo", "bar":
            self.assertTrue(n in s)
        s.add("baz")
        for n in "bar", "baz":
            self.assertTrue(n in s)
        self.assertTrue("foo" not in s)

    def test_iter(self):
        s = LimitedSet(maxlen=2)
        items = "foo", "bar"
        map(s.add, items)
        l = list(iter(items))
        for item in items:
            self.assertTrue(item in l)

    def test_repr(self):
        s = LimitedSet(maxlen=2)
        items = "foo", "bar"
        map(s.add, items)
        self.assertTrue(repr(s).startswith("LimitedSet("))


