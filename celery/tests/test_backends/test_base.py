import sys
import types
import unittest2 as unittest

from celery.serialization import subclass_exception
from celery.serialization import find_nearest_pickleable_exception as fnpe
from celery.serialization import UnpickleableExceptionWrapper
from celery.serialization import get_pickleable_exception as gpe

from celery import states
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
                b.get_status, "SOMExx-N0Nex1stant-IDxx-")

    def test_store_result(self):
        self.assertRaises(NotImplementedError,
                b.store_result, "SOMExx-N0nex1stant-IDxx-", 42, states.SUCCESS)

    def test_reload_task_result(self):
        self.assertRaises(NotImplementedError,
                b.reload_task_result, "SOMExx-N0nex1stant-IDxx-")

    def test_reload_taskset_result(self):
        self.assertRaises(NotImplementedError,
                b.reload_taskset_result, "SOMExx-N0nex1stant-IDxx-")

    def test_get_result(self):
        self.assertRaises(NotImplementedError,
                b.get_result, "SOMExx-N0nex1stant-IDxx-")

    def test_restore_taskset(self):
        self.assertRaises(NotImplementedError,
                b.restore_taskset, "SOMExx-N0nex1stant-IDxx-")

    def test_save_taskset(self):
        self.assertRaises(NotImplementedError,
                b.save_taskset, "SOMExx-N0nex1stant-IDxx-", "blergh")

    def test_get_traceback(self):
        self.assertRaises(NotImplementedError,
                b.get_traceback, "SOMExx-N0nex1stant-IDxx-")


class TestPickleException(unittest.TestCase):

    def test_oldstyle(self):
        self.assertIsNone(fnpe(Oldstyle()))

    def test_BaseException(self):
        self.assertIsNone(fnpe(Exception()))

    def test_get_pickleable_exception(self):
        exc = Exception("foo")
        self.assertEqual(gpe(exc), exc)

    def test_unpickleable(self):
        self.assertIsInstance(fnpe(Unpickleable()), KeyError)
        self.assertIsNone(fnpe(Impossible()))


class TestPrepareException(unittest.TestCase):

    def test_unpickleable(self):
        x = b.prepare_exception(Unpickleable(1, 2, "foo"))
        self.assertIsInstance(x, KeyError)
        y = b.exception_to_python(x)
        self.assertIsInstance(y, KeyError)

    def test_impossible(self):
        x = b.prepare_exception(Impossible())
        self.assertIsInstance(x, UnpickleableExceptionWrapper)
        y = b.exception_to_python(x)
        self.assertEqual(y.__class__.__name__, "Impossible")
        if sys.version_info < (2, 5):
            self.assertTrue(y.__class__.__module__)
        else:
            self.assertEqual(y.__class__.__module__, "foo.module")

    def test_regular(self):
        x = b.prepare_exception(KeyError("baz"))
        self.assertIsInstance(x, KeyError)
        y = b.exception_to_python(x)
        self.assertIsInstance(y, KeyError)


class TestKeyValueStoreBackendInterface(unittest.TestCase):

    def test_get(self):
        self.assertRaises(NotImplementedError, KeyValueStoreBackend().get,
                "a")

    def test_set(self):
        self.assertRaises(NotImplementedError, KeyValueStoreBackend().set,
                "a", 1)

    def test_cleanup(self):
        self.assertFalse(KeyValueStoreBackend().cleanup())
