import sys
import types
from celery.tests.utils import unittest

from celery.utils import serialization
from celery.utils.serialization import subclass_exception
from celery.utils.serialization import \
        find_nearest_pickleable_exception as fnpe
from celery.utils.serialization import UnpickleableExceptionWrapper
from celery.utils.serialization import get_pickleable_exception as gpe

from celery import states
from celery.backends.base import BaseBackend, KeyValueStoreBackend
from celery.backends.base import BaseDictBackend
from celery.utils import gen_unique_id


class wrapobject(object):

    def __init__(self, *args, **kwargs):
        self.args = args


Oldstyle = types.ClassType("Oldstyle", (), {})
Unpickleable = subclass_exception("Unpickleable", KeyError, "foo.module")
Impossible = subclass_exception("Impossible", object, "foo.module")
Lookalike = subclass_exception("Lookalike", wrapobject, "foo.module")
b = BaseBackend()


class test_serialization(unittest.TestCase):

    def test_create_exception_cls(self):
        self.assertTrue(serialization.create_exception_cls("FooError", "m"))
        self.assertTrue(serialization.create_exception_cls("FooError",
                                                            "m",
                                                            KeyError))


class test_BaseBackend_interface(unittest.TestCase):

    def test_get_status(self):
        self.assertRaises(NotImplementedError,
                b.get_status, "SOMExx-N0Nex1stant-IDxx-")

    def test__forget(self):
        self.assertRaises(NotImplementedError,
                b.forget, "SOMExx-N0Nex1stant-IDxx-")

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

    def test_forget(self):
        self.assertRaises(NotImplementedError,
                b.forget, "SOMExx-N0nex1stant-IDxx-")


class test_exception_pickle(unittest.TestCase):

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


class test_prepare_exception(unittest.TestCase):

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


class KVBackend(KeyValueStoreBackend):

    def __init__(self, *args, **kwargs):
        self.db = {}
        super(KVBackend, self).__init__(KeyValueStoreBackend)

    def get(self, key):
        return self.db.get(key)

    def set(self, key, value):
        self.db[key] = value

    def delete(self, key):
        self.db.pop(key, None)


class DictBackend(BaseDictBackend):

    def _save_taskset(self, taskset_id, result):
        return "taskset-saved"

    def _restore_taskset(self, taskset_id):
        if taskset_id == "exists":
            return {"result": "taskset"}

    def _get_task_meta_for(self, task_id):
        if task_id == "task-exists":
            return {"result": "task"}


class test_BaseDictBackend(unittest.TestCase):

    def setUp(self):
        self.b = DictBackend()

    def test_save_taskset(self):
        self.assertEqual(self.b.save_taskset("foofoo", "xxx"),
                         "taskset-saved")

    def test_restore_taskset(self):
        self.assertIsNone(self.b.restore_taskset("missing"))
        self.assertIsNone(self.b.restore_taskset("missing"))
        self.assertEqual(self.b.restore_taskset("exists"), "taskset")
        self.assertEqual(self.b.restore_taskset("exists"), "taskset")
        self.assertEqual(self.b.restore_taskset("exists", cache=False),
                         "taskset")

    def test_reload_taskset_result(self):
        self.b._cache = {}
        self.b.reload_taskset_result("exists")
        self.b._cache["exists"] = {"result": "taskset"}

    def test_reload_task_result(self):
        self.b._cache = {}
        self.b.reload_task_result("task-exists")
        self.b._cache["task-exists"] = {"result": "task"}


class test_KeyValueStoreBackend(unittest.TestCase):

    def setUp(self):
        self.b = KVBackend()

    def test_get_store_delete_result(self):
        tid = gen_unique_id()
        self.b.mark_as_done(tid, "Hello world")
        self.assertEqual(self.b.get_result(tid), "Hello world")
        self.assertEqual(self.b.get_status(tid), states.SUCCESS)
        self.b.forget(tid)
        self.assertEqual(self.b.get_status(tid), states.PENDING)

    def test_get_missing_meta(self):
        self.assertIsNone(self.b.get_result("xxx-missing"))
        self.assertEqual(self.b.get_status("xxx-missing"), states.PENDING)

    def test_save_restore_taskset(self):
        tid = gen_unique_id()
        self.b.save_taskset(tid, "Hello world")
        self.assertEqual(self.b.restore_taskset(tid), "Hello world")

    def test_restore_missing_taskset(self):
        self.assertIsNone(self.b.restore_taskset("xxx-nonexistant"))


class test_KeyValueStoreBackend_interface(unittest.TestCase):

    def test_get(self):
        self.assertRaises(NotImplementedError, KeyValueStoreBackend().get,
                "a")

    def test_set(self):
        self.assertRaises(NotImplementedError, KeyValueStoreBackend().set,
                "a", 1)

    def test_cleanup(self):
        self.assertFalse(KeyValueStoreBackend().cleanup())

    def test_delete(self):
        self.assertRaises(NotImplementedError, KeyValueStoreBackend().delete,
                "a")

    def test_forget(self):
        self.assertRaises(NotImplementedError, KeyValueStoreBackend().forget,
                "a")
