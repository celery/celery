import sys
import unittest2 as unittest
from Queue import Queue

from celery.datastructures import PositionQueue, ExceptionInfo, LocalCache
from celery.datastructures import LimitedSet, SharedCounter, consume_queue
from celery.datastructures import AttributeDict


class test_PositionQueue(unittest.TestCase):

    def test_position_queue_unfilled(self):
        q = PositionQueue(length=10)
        for position in q.data:
            self.assertIsInstance(position, q.UnfilledPosition)

        self.assertListEqual(q.filled, [])
        self.assertEqual(len(q), 0)
        self.assertFalse(q.full())

    def test_position_queue_almost(self):
        q = PositionQueue(length=10)
        q[3] = 3
        q[6] = 6
        q[9] = 9

        self.assertListEqual(q.filled, [3, 6, 9])
        self.assertEqual(len(q), 3)
        self.assertFalse(q.full())

    def test_position_queue_full(self):
        q = PositionQueue(length=10)
        for i in xrange(10):
            q[i] = i
        self.assertListEqual(q.filled, list(xrange(10)))
        self.assertEqual(len(q), 10)
        self.assertTrue(q.full())


class test_ExceptionInfo(unittest.TestCase):

    def test_exception_info(self):

        try:
            raise LookupError("The quick brown fox jumps...")
        except LookupError:
            exc_info = sys.exc_info()

        einfo = ExceptionInfo(exc_info)
        self.assertEqual(str(einfo), einfo.traceback)
        self.assertIsInstance(einfo.exception, LookupError)
        self.assertTupleEqual(einfo.exception.args,
                ("The quick brown fox jumps...", ))
        self.assertTrue(einfo.traceback)

        r = repr(einfo)
        self.assertTrue(r)


class test_utilities(unittest.TestCase):

    def test_consume_queue(self):
        x = Queue()
        it = consume_queue(x)
        self.assertRaises(StopIteration, it.next)
        x.put("foo")
        it = consume_queue(x)
        self.assertEqual(it.next(), "foo")
        self.assertRaises(StopIteration, it.next)


class test_SharedCounter(unittest.TestCase):

    def test_initial_value(self):
        self.assertEqual(int(SharedCounter(10)), 10)

    def test_increment(self):
        c = SharedCounter(10)
        c.increment()
        self.assertEqual(int(c), 11)
        c.increment(2)
        self.assertEqual(int(c), 13)

    def test_decrement(self):
        c = SharedCounter(10)
        c.decrement()
        self.assertEqual(int(c), 9)
        c.decrement(2)
        self.assertEqual(int(c), 7)

    def test_iadd(self):
        c = SharedCounter(10)
        c += 10
        self.assertEqual(int(c), 20)

    def test_isub(self):
        c = SharedCounter(10)
        c -= 20
        self.assertEqual(int(c), -10)

    def test_repr(self):
        self.assertIn("<SharedCounter:", repr(SharedCounter(10)))


class test_LimitedSet(unittest.TestCase):

    def test_add(self):
        s = LimitedSet(maxlen=2)
        s.add("foo")
        s.add("bar")
        for n in "foo", "bar":
            self.assertIn(n, s)
        s.add("baz")
        for n in "bar", "baz":
            self.assertIn(n, s)
        self.assertNotIn("foo", s)

    def test_iter(self):
        s = LimitedSet(maxlen=2)
        items = "foo", "bar"
        for item in items:
            s.add(item)
        l = list(iter(s))
        for item in items:
            self.assertIn(item, l)

    def test_repr(self):
        s = LimitedSet(maxlen=2)
        items = "foo", "bar"
        for item in items:
            s.add(item)
        self.assertIn("LimitedSet(", repr(s))


class test_LocalCache(unittest.TestCase):

    def test_expires(self):
        limit = 100
        x = LocalCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x[i] = i
        self.assertListEqual(x.keys(), slots[limit:])


class test_AttributeDict(unittest.TestCase):

    def test_getattr__setattr(self):
        x = AttributeDict({"foo": "bar"})
        self.assertEqual(x["foo"], "bar")
        self.assertRaises(AttributeError, getattr, x, "bar")
        x.bar = "foo"
        self.assertEqual(x["bar"], "foo")
