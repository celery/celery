import unittest
import types

from django.db.models.base import subclass_exception
from billiard.serialization import find_nearest_pickleable_exception as fnpe
from billiard.serialization import UnpickleableExceptionWrapper
from billiard.serialization import get_pickleable_exception as gpe

from celery.backends.base import BaseBackend, KeyValueStoreBackend


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
                b.is_successful, "SOMExx-N0Nex1stant-IDxx-")

    def test_store_result(self):
        self.assertRaises(NotImplementedError,
                b.store_result, "SOMExx-N0nex1stant-IDxx-", 42, "SUCCESS")

    def test_get_result(self):
        self.assertRaises(NotImplementedError,
                b.get_result, "SOMExx-N0nex1stant-IDxx-")

    def test_get_taskset(self):
        self.assertRaises(NotImplementedError,
                b.get_taskset, "SOMExx-N0nex1stant-IDxx-")

    def test_store_taskset(self):
        self.assertRaises(NotImplementedError,
                b.store_taskset, "SOMExx-N0nex1stant-IDxx-", "blergh")

    def test_get_traceback(self):
        self.assertRaises(NotImplementedError,
                b.get_traceback, "SOMExx-N0nex1stant-IDxx-")


class TestPickleException(unittest.TestCase):

    def test_oldstyle(self):
        self.assertTrue(fnpe(Oldstyle()) is None)

    def test_BaseException(self):
        self.assertTrue(fnpe(Exception()) is None)

    def test_get_pickleable_exception(self):
        exc = Exception("foo")
        self.assertEquals(gpe(exc), exc)

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
