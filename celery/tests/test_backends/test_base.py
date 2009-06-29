import unittest
from celery.backends.base import find_nearest_pickleable_exception as fnpe
from celery.backends.base import BaseBackend
from celery.backends.base import UnpickleableExceptionWrapper
from django.db.models.base import subclass_exception

class wrapobject(object):

    def __init__(self, *args, **kwargs):
        self.args = args


Unpickleable = subclass_exception("Unpickleable", KeyError, "foo.module")
Impossible = subclass_exception("Impossible", object, "foo.module")
Lookalike = subclass_exception("Lookalike", wrapobject, "foo.module")
b = BaseBackend()


class TestPickleException(unittest.TestCase):

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
