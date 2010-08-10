import pickle
import sys
import socket
import unittest2 as unittest

from celery import utils
from celery.utils import promise, mpromise, maybe_promise

from celery.tests.utils import sleepdeprived, execute_context
from celery.tests.utils import mask_modules


def double(x):
    return x * 2


class test_chunks(unittest.TestCase):

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


class test_gen_unique_id(unittest.TestCase):

    def test_gen_unique_id_without_ctypes(self):
        old_utils = sys.modules.pop("celery.utils")

        def with_ctypes_masked(_val):
            from celery.utils import ctypes, gen_unique_id

            self.assertIsNone(ctypes)
            uuid = gen_unique_id()
            self.assertTrue(uuid)
            self.assertIsInstance(uuid, basestring)

        try:
            context = mask_modules("ctypes")
            execute_context(context, with_ctypes_masked)
        finally:
            sys.modules["celery.utils"] = old_utils


class test_utils(unittest.TestCase):

    def test_repeatlast(self):
        items = range(6)
        it = utils.repeatlast(items)
        for i in items:
            self.assertEqual(it.next(), i)
        for j in items:
            self.assertEqual(it.next(), i)

    def test_get_full_cls_name(self):
        Class = type("Fox", (object, ), {"__module__": "quick.brown"})
        self.assertEqual(utils.get_full_cls_name(Class), "quick.brown.Fox")

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


class test_retry_over_time(unittest.TestCase):

    def test_returns_retval_on_success(self):

        def _fun(x, y):
            return x * y

        ret = utils.retry_over_time(_fun, (socket.error, ), args=[16, 16],
                                    max_retries=3)

        self.assertEqual(ret, 256)

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

        self.assertEqual(iterations[0], 3)
        self.assertEqual(ret, 1024)

        self.assertRaises(socket.error, utils.retry_over_time,
                        _fun, (socket.error, ), args=[32, 32], max_retries=1)


class test_promise(unittest.TestCase):

    def test__str__(self):
        self.assertEqual(str(promise(lambda: "the quick brown fox")),
                "the quick brown fox")

    def test__repr__(self):
        self.assertEqual(repr(promise(lambda: "fi fa fo")),
                "'fi fa fo'")

    def test_evaluate(self):
        self.assertEqual(promise(lambda: 2 + 2)(), 4)
        self.assertEqual(promise(lambda x: x * 4, 2), 8)
        self.assertEqual(promise(lambda x: x * 8, 2)(), 16)

    def test_cmp(self):
        self.assertEqual(promise(lambda: 10), promise(lambda: 10))
        self.assertNotEqual(promise(lambda: 10), promise(lambda: 20))

    def test__reduce__(self):
        x = promise(double, 4)
        y = pickle.loads(pickle.dumps(x))
        self.assertEqual(x(), y())

    def test__deepcopy__(self):
        from copy import deepcopy
        x = promise(double, 4)
        y = deepcopy(x)
        self.assertEqual(x._fun, y._fun)
        self.assertEqual(x._args, y._args)
        self.assertEqual(x(), y())


class test_mpromise(unittest.TestCase):

    def test_is_memoized(self):

        it = iter(xrange(20, 30))
        p = mpromise(it.next)
        self.assertEqual(p(), 20)
        self.assertTrue(p.evaluated)
        self.assertEqual(p(), 20)
        self.assertEqual(repr(p), "20")


class test_maybe_promise(unittest.TestCase):

    def test_evaluates(self):
        self.assertEqual(maybe_promise(promise(lambda: 10)), 10)
        self.assertEqual(maybe_promise(20), 20)
