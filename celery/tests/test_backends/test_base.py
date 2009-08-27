import unittest
import types
from celery.backends.base import BaseBackend, KeyValueStoreBackend
from celery.serialization import find_nearest_pickleable_exception as fnpe
from celery.serialization import UnpickleableExceptionWrapper
from django.db.models.base import subclass_exception


class wrapobject(object):

    def __init__(self, *args, **kwargs):
        self.args = args


Oldstyle = types.ClassType("Oldstyle", (), {})
Unpickleable = subclass_exception("Unpickleable", KeyError, "foo.module")
Impossible = subclass_exception("Impossible", object, "foo.module")
Lookalike = subclass_exception("Lookalike", wrapobject, "foo.module")
b = BaseBackend()


class TestBaseBackendInterface(unittest.TestCase):

    def test_get_status(self):
        self.assertRaises(NotImplementedError,
                b.is_done, "SOMExx-N0Nex1stant-IDxx-")

    def test_store_result(self):
        self.assertRaises(NotImplementedError,
                b.store_result, "SOMExx-N0nex1stant-IDxx-", 42, "DONE")

    def test_get_result(self):
        self.assertRaises(NotImplementedError,
                b.get_result, "SOMExx-N0nex1stant-IDxx-")


class TestPickleException(unittest.TestCase):

    def test_oldstyle(self):
        self.assertTrue(fnpe(Oldstyle()) is None)

    def test_BaseException(self):
        self.assertTrue(fnpe(Exception()) is None)

    def test_unpickleable(self):
        self.assertTrue(isinstance(fnpe(Unpickleable()), KeyError))
        self.assertEquals(fnpe(Impossible()), None)


class TestPrepareException(unittest.TestCase):

    def test_unpickleable(self):
        x = b.prepare_exception(Unpickleable(1, 2, "foo"))
        self.assertTrue(isinstance(x, KeyError))
        y = b.exception_to_python(x)
        self.assertTrue(isinstance(y, KeyError))

    def test_impossible(self):
        x = b.prepare_exception(Impossible())
        self.assertTrue(isinstance(x, UnpickleableExceptionWrapper))
        y = b.exception_to_python(x)
        self.assertEquals(y.__class__.__name__, "Impossible")
        self.assertEquals(y.__class__.__module__, "foo.module")

    def test_regular(self):
        x = b.prepare_exception(KeyError("baz"))
        self.assertTrue(isinstance(x, KeyError))
        y = b.exception_to_python(x)
        self.assertTrue(isinstance(y, KeyError))


class TestKeyValueStoreBackendInterface(unittest.TestCase):

    def test_get(self):
        self.assertRaises(NotImplementedError, KeyValueStoreBackend().get,
                "a")

    def test_set(self):
        self.assertRaises(NotImplementedError, KeyValueStoreBackend().set,
                "a", 1)

    def test_cleanup(self):
        self.assertFalse(KeyValueStoreBackend().cleanup())
