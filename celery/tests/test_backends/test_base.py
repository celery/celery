from __future__ import absolute_import
from __future__ import with_statement

import sys
import types

from mock import Mock
from nose import SkipTest

from celery.result import AsyncResult
from celery.utils import serialization
from celery.utils.serialization import subclass_exception
from celery.utils.serialization import \
        find_nearest_pickleable_exception as fnpe
from celery.utils.serialization import UnpickleableExceptionWrapper
from celery.utils.serialization import get_pickleable_exception as gpe

from celery import states
from celery.backends.base import BaseBackend, KeyValueStoreBackend
from celery.backends.base import BaseDictBackend, DisabledBackend
from celery.utils import uuid

from celery.tests.utils import Case


class wrapobject(object):

    def __init__(self, *args, **kwargs):
        self.args = args

if sys.version_info >= (3, 0):
    Oldstyle = None
else:
    Oldstyle = types.ClassType("Oldstyle", (), {})
Unpickleable = subclass_exception("Unpickleable", KeyError, "foo.module")
Impossible = subclass_exception("Impossible", object, "foo.module")
Lookalike = subclass_exception("Lookalike", wrapobject, "foo.module")
b = BaseBackend()


class test_serialization(Case):

    def test_create_exception_cls(self):
        self.assertTrue(serialization.create_exception_cls("FooError", "m"))
        self.assertTrue(serialization.create_exception_cls("FooError",
                                                            "m",
                                                            KeyError))


class test_BaseBackend_interface(Case):

    def test_get_status(self):
        with self.assertRaises(NotImplementedError):
            b.get_status("SOMExx-N0Nex1stant-IDxx-")

    def test__forget(self):
        with self.assertRaises(NotImplementedError):
            b.forget("SOMExx-N0Nex1stant-IDxx-")

    def test_store_result(self):
        with self.assertRaises(NotImplementedError):
            b.store_result("SOMExx-N0nex1stant-IDxx-", 42, states.SUCCESS)

    def test_mark_as_started(self):
        with self.assertRaises(NotImplementedError):
            b.mark_as_started("SOMExx-N0nex1stant-IDxx-")

    def test_reload_task_result(self):
        with self.assertRaises(NotImplementedError):
            b.reload_task_result("SOMExx-N0nex1stant-IDxx-")

    def test_reload_taskset_result(self):
        with self.assertRaises(NotImplementedError):
            b.reload_taskset_result("SOMExx-N0nex1stant-IDxx-")

    def test_get_result(self):
        with self.assertRaises(NotImplementedError):
            b.get_result("SOMExx-N0nex1stant-IDxx-")

    def test_restore_taskset(self):
        with self.assertRaises(NotImplementedError):
            b.restore_taskset("SOMExx-N0nex1stant-IDxx-")

    def test_delete_taskset(self):
        with self.assertRaises(NotImplementedError):
            b.delete_taskset("SOMExx-N0nex1stant-IDxx-")

    def test_save_taskset(self):
        with self.assertRaises(NotImplementedError):
            b.save_taskset("SOMExx-N0nex1stant-IDxx-", "blergh")

    def test_get_traceback(self):
        with self.assertRaises(NotImplementedError):
            b.get_traceback("SOMExx-N0nex1stant-IDxx-")

    def test_forget(self):
        with self.assertRaises(NotImplementedError):
            b.forget("SOMExx-N0nex1stant-IDxx-")

    def test_on_chord_apply(self, unlock="celery.chord_unlock"):
        from celery.registry import tasks
        p, tasks[unlock] = tasks.get(unlock), Mock()
        try:
            b.on_chord_apply("dakj221", "sdokqweok",
                             result=map(AsyncResult, [1, 2, 3]))
            self.assertTrue(tasks[unlock].apply_async.call_count)
        finally:
            tasks[unlock] = p


class test_exception_pickle(Case):

    def test_oldstyle(self):
        if Oldstyle is None:
            raise SkipTest("py3k does not support old style classes")
        self.assertIsNone(fnpe(Oldstyle()))

    def test_BaseException(self):
        self.assertIsNone(fnpe(Exception()))

    def test_get_pickleable_exception(self):
        exc = Exception("foo")
        self.assertEqual(gpe(exc), exc)

    def test_unpickleable(self):
        self.assertIsInstance(fnpe(Unpickleable()), KeyError)
        self.assertIsNone(fnpe(Impossible()))


class test_prepare_exception(Case):

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
    mget_returns_dict = False

    def __init__(self, *args, **kwargs):
        self.db = {}
        super(KVBackend, self).__init__(KeyValueStoreBackend)

    def get(self, key):
        return self.db.get(key)

    def set(self, key, value):
        self.db[key] = value

    def mget(self, keys):
        if self.mget_returns_dict:
            return dict((key, self.get(key)) for key in keys)
        else:
            return [self.get(key) for key in keys]

    def delete(self, key):
        self.db.pop(key, None)


class DictBackend(BaseDictBackend):

    def __init__(self, *args, **kwargs):
        BaseDictBackend.__init__(self, *args, **kwargs)
        self._data = {"can-delete": {"result": "foo"}}

    def _restore_taskset(self, taskset_id):
        if taskset_id == "exists":
            return {"result": "taskset"}

    def _get_task_meta_for(self, task_id):
        if task_id == "task-exists":
            return {"result": "task"}

    def _delete_taskset(self, taskset_id):
        self._data.pop(taskset_id, None)


class test_BaseDictBackend(Case):

    def setUp(self):
        self.b = DictBackend()

    def test_delete_taskset(self):
        self.b.delete_taskset("can-delete")
        self.assertNotIn("can-delete", self.b._data)

    def test_save_taskset(self):
        b = BaseDictBackend()
        b._save_taskset = Mock()
        b.save_taskset("foofoo", "xxx")
        b._save_taskset.assert_called_with("foofoo", "xxx")

    def test_forget_interface(self):
        b = BaseDictBackend()
        with self.assertRaises(NotImplementedError):
            b.forget("foo")

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


class test_KeyValueStoreBackend(Case):

    def setUp(self):
        self.b = KVBackend()

    def test_get_store_delete_result(self):
        tid = uuid()
        self.b.mark_as_done(tid, "Hello world")
        self.assertEqual(self.b.get_result(tid), "Hello world")
        self.assertEqual(self.b.get_status(tid), states.SUCCESS)
        self.b.forget(tid)
        self.assertEqual(self.b.get_status(tid), states.PENDING)

    def test_strip_prefix(self):
        x = self.b.get_key_for_task("x1b34")
        self.assertEqual(self.b._strip_prefix(x), "x1b34")
        self.assertEqual(self.b._strip_prefix("x1b34"), "x1b34")

    def test_get_many(self):
        for is_dict in True, False:
            self.b.mget_returns_dict = is_dict
            ids = dict((uuid(), i) for i in xrange(10))
            for id, i in ids.items():
                self.b.mark_as_done(id, i)
            it = self.b.get_many(ids.keys())
            for i, (got_id, got_state) in enumerate(it):
                self.assertEqual(got_state["result"], ids[got_id])
            self.assertEqual(i, 9)
            self.assertTrue(list(self.b.get_many(ids.keys())))

    def test_get_missing_meta(self):
        self.assertIsNone(self.b.get_result("xxx-missing"))
        self.assertEqual(self.b.get_status("xxx-missing"), states.PENDING)

    def test_save_restore_delete_taskset(self):
        tid = uuid()
        self.b.save_taskset(tid, "Hello world")
        self.assertEqual(self.b.restore_taskset(tid), "Hello world")
        self.b.delete_taskset(tid)
        self.assertIsNone(self.b.restore_taskset(tid))

    def test_restore_missing_taskset(self):
        self.assertIsNone(self.b.restore_taskset("xxx-nonexistant"))


class test_KeyValueStoreBackend_interface(Case):

    def test_get(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend().get("a")

    def test_set(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend().set("a", 1)

    def test_cleanup(self):
        self.assertFalse(KeyValueStoreBackend().cleanup())

    def test_delete(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend().delete("a")

    def test_mget(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend().mget(["a"])

    def test_forget(self):
        with self.assertRaises(NotImplementedError):
            KeyValueStoreBackend().forget("a")


class test_DisabledBackend(Case):

    def test_store_result(self):
        DisabledBackend().store_result()

    def test_is_disabled(self):
        with self.assertRaises(NotImplementedError):
            DisabledBackend().get_status("foo")
