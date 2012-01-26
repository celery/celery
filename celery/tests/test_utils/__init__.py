from __future__ import absolute_import
from __future__ import with_statement

from celery import utils
from celery.utils import promise, mpromise
from celery.utils.threads import bgThread
from celery.tests.utils import Case


def double(x):
    return x * 2


class test_bgThread_interface(Case):

    def test_body(self):
        x = bgThread()
        with self.assertRaises(NotImplementedError):
            x.body()


class test_chunks(Case):

    def test_chunks(self):

        # n == 2
        x = utils.chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2)
        self.assertListEqual(list(x),
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]])

        # n == 3
        x = utils.chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 3)
        self.assertListEqual(list(x),
            [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]])

        # n == 2 (exact)
        x = utils.chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 2)
        self.assertListEqual(list(x),
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]])


class test_utils(Case):

    def test_qualname(self):
        Class = type("Fox", (object, ), {"__module__": "quick.brown"})
        self.assertEqual(utils.qualname(Class), "quick.brown.Fox")
        self.assertEqual(utils.qualname(Class()), "quick.brown.Fox")

    def test_is_iterable(self):
        for a in "f", ["f"], ("f", ), {"f": "f"}:
            self.assertTrue(utils.is_iterable(a))
        for b in object(), 1:
            self.assertFalse(utils.is_iterable(b))

    def test_padlist(self):
        self.assertListEqual(utils.padlist(["George", "Costanza", "NYC"], 3),
                ["George", "Costanza", "NYC"])
        self.assertListEqual(utils.padlist(["George", "Costanza"], 3),
                ["George", "Costanza", None])
        self.assertListEqual(utils.padlist(["George", "Costanza", "NYC"], 4,
                                           default="Earth"),
                ["George", "Costanza", "NYC", "Earth"])

    def test_firstmethod_AttributeError(self):
        self.assertIsNone(utils.firstmethod("foo")([object()]))

    def test_firstmethod_promises(self):

        class A(object):

            def __init__(self, value=None):
                self.value = value

            def m(self):
                return self.value

        self.assertEqual("four", utils.firstmethod("m")([
            A(), A(), A(), A("four"), A("five")]))
        self.assertEqual("four", utils.firstmethod("m")([
            A(), A(), A(), promise(lambda: A("four")), A("five")]))

    def test_first(self):
        iterations = [0]

        def predicate(value):
            iterations[0] += 1
            if value == 5:
                return True
            return False

        self.assertEqual(5, utils.first(predicate, xrange(10)))
        self.assertEqual(iterations[0], 6)

        iterations[0] = 0
        self.assertIsNone(utils.first(predicate, xrange(10, 20)))
        self.assertEqual(iterations[0], 10)

    def test_get_cls_by_name__instance_returns_instance(self):
        instance = object()
        self.assertIs(utils.get_cls_by_name(instance), instance)

    def test_truncate_text(self):
        self.assertEqual(utils.truncate_text("ABCDEFGHI", 3), "ABC...")
        self.assertEqual(utils.truncate_text("ABCDEFGHI", 10), "ABCDEFGHI")

    def test_abbr(self):
        self.assertEqual(utils.abbr(None, 3), "???")
        self.assertEqual(utils.abbr("ABCDEFGHI", 6), "ABC...")
        self.assertEqual(utils.abbr("ABCDEFGHI", 20), "ABCDEFGHI")
        self.assertEqual(utils.abbr("ABCDEFGHI", 6, None), "ABCDEF")

    def test_abbrtask(self):
        self.assertEqual(utils.abbrtask(None, 3), "???")
        self.assertEqual(utils.abbrtask("feeds.tasks.refresh", 10),
                                        "[.]refresh")
        self.assertEqual(utils.abbrtask("feeds.tasks.refresh", 30),
                                        "feeds.tasks.refresh")

    def test_cached_property(self):

        def fun(obj):
            return fun.value

        x = utils.cached_property(fun)
        self.assertIs(x.__get__(None), x)
        self.assertIs(x.__set__(None, None), x)
        self.assertIs(x.__delete__(None), x)


class test_mpromise(Case):

    def test_is_memoized(self):

        it = iter(xrange(20, 30))
        p = mpromise(it.next)
        self.assertEqual(p(), 20)
        self.assertTrue(p.evaluated)
        self.assertEqual(p(), 20)
        self.assertEqual(repr(p), "20")
