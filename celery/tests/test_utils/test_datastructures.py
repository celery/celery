import sys
from celery.tests.utils import unittest
from Queue import Queue

from celery.datastructures import ExceptionInfo, LocalCache
from celery.datastructures import LimitedSet, consume_queue
from celery.datastructures import AttributeDict, DictAttribute
from celery.datastructures import ConfigurationView


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
        self.assertRaises(KeyError, x.__getitem__, "bar")

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


class test_utilities(unittest.TestCase):

    def test_consume_queue(self):
        x = Queue()
        it = consume_queue(x)
        self.assertRaises(StopIteration, it.next)
        x.put("foo")
        it = consume_queue(x)
        self.assertEqual(it.next(), "foo")
        self.assertRaises(StopIteration, it.next)


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
