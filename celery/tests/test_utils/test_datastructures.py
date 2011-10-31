from __future__ import absolute_import
from __future__ import with_statement

import sys

from celery.datastructures import ExceptionInfo, LRUCache
from celery.datastructures import LimitedSet
from celery.datastructures import AttributeDict, DictAttribute
from celery.datastructures import ConfigurationView
from celery.tests.utils import unittest


class Object(object):
    pass


class test_DictAttribute(unittest.TestCase):

    def test_get_set(self):
        x = DictAttribute(Object())
        x["foo"] = "The quick brown fox"
        self.assertEqual(x["foo"], "The quick brown fox")
        self.assertEqual(x["foo"], x.obj.foo)
        self.assertEqual(x.get("foo"), "The quick brown fox")
        self.assertIsNone(x.get("bar"))
        with self.assertRaises(KeyError):
            x["bar"]

    def test_setdefault(self):
        x = DictAttribute(Object())
        self.assertEqual(x.setdefault("foo", "NEW"), "NEW")
        self.assertEqual(x.setdefault("foo", "XYZ"), "NEW")

    def test_contains(self):
        x = DictAttribute(Object())
        x["foo"] = 1
        self.assertIn("foo", x)
        self.assertNotIn("bar", x)

    def test_iteritems(self):
        obj = Object()
        obj.attr1 = 1
        x = DictAttribute(obj)
        x["attr2"] = 2
        self.assertDictEqual(dict(x.iteritems()),
                             dict(attr1=1, attr2=2))


class test_ConfigurationView(unittest.TestCase):

    def setUp(self):
        self.view = ConfigurationView({"changed_key": 1,
                                       "both": 2},
                                      [{"default_key": 1,
                                       "both": 1}])

    def test_setdefault(self):
        self.assertEqual(self.view.setdefault("both", 36), 2)
        self.assertEqual(self.view.setdefault("new", 36), 36)

    def test_contains(self):
        self.assertIn("changed_key", self.view)
        self.assertIn("default_key", self.view)
        self.assertNotIn("new", self.view)

    def test_repr(self):
        self.assertIn("changed_key", repr(self.view))
        self.assertIn("default_key", repr(self.view))

    def test_iter(self):
        expected = {"changed_key": 1,
                    "default_key": 1,
                    "both": 2}
        self.assertDictEqual(dict(self.view.items()), expected)


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


class test_LRUCache(unittest.TestCase):

    def test_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(xrange(limit * 2))
        for i in slots:
            x[i] = i
        self.assertListEqual(x.keys(), list(slots[limit:]))

    def test_least_recently_used(self):
        x = LRUCache(3)

        x[1], x[2], x[3] = 1, 2, 3
        self.assertEqual(x.keys(), [1, 2, 3])

        x[4], x[5] = 4, 5
        self.assertEqual(x.keys(), [3, 4, 5])

        # access 3, which makes it the last used key.
        x[3]
        x[6] = 6
        self.assertEqual(x.keys(), [5, 3, 6])

        x[7] = 7
        self.assertEqual(x.keys(), [3, 6, 7])

    def assertSafeIter(self, method, interval=0.01, size=10000):
        from threading import Thread, Event
        from time import sleep
        x = LRUCache(size)
        x.update(zip(xrange(size), xrange(size)))

        class Burglar(Thread):

            def __init__(self, cache):
                self.cache = cache
                self._is_shutdown = Event()
                self._is_stopped = Event()
                Thread.__init__(self)

            def run(self):
                while not self._is_shutdown.isSet():
                    try:
                        self.cache.data.popitem(last=False)
                    except KeyError:
                        break
                self._is_stopped.set()

            def stop(self):
                self._is_shutdown.set()
                self._is_stopped.wait()
                self.join(1e10)

        burglar = Burglar(x)
        burglar.start()
        try:
            for _ in getattr(x, method)():
                sleep(0.0001)
        finally:
            burglar.stop()

    def test_safe_to_remove_while_iteritems(self):
        self.assertSafeIter("iteritems")

    def test_safe_to_remove_while_keys(self):
        self.assertSafeIter("keys")

    def test_safe_to_remove_while_itervalues(self):
        self.assertSafeIter("itervalues")


class test_AttributeDict(unittest.TestCase):

    def test_getattr__setattr(self):
        x = AttributeDict({"foo": "bar"})
        self.assertEqual(x["foo"], "bar")
        with self.assertRaises(AttributeError):
            x.bar
        x.bar = "foo"
        self.assertEqual(x["bar"], "foo")
