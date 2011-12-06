from __future__ import absolute_import
from __future__ import with_statement

from celery import current_app
from celery import states
from celery.exceptions import RetryTaskError
from celery.execute.trace import eager_trace_task
from celery.tests.utils import unittest


@current_app.task
def add(x, y):
    return x + y


@current_app.task
def raises(exc):
    raise exc


def trace(task, args=(), kwargs={}, propagate=False):
    return eager_trace_task(task, "id-1", args, kwargs,
                      propagate=propagate)


class test_trace(unittest.TestCase):

    def test_trace_successful(self):
        retval, info = trace(add, (2, 2), {})
        self.assertIsNone(info)
        self.assertEqual(retval, 4)

    def test_trace_SystemExit(self):
        with self.assertRaises(SystemExit):
            trace(raises, (SystemExit(), ), {})

    def test_trace_RetryTaskError(self):
        exc = RetryTaskError("foo", "bar")
        _, info = trace(raises, (exc, ), {})
        self.assertEqual(info.state, states.RETRY)
        self.assertIs(info.retval, exc)

    def test_trace_exception(self):
        exc = KeyError("foo")
        _, info = trace(raises, (exc, ), {})
        self.assertEqual(info.state, states.FAILURE)
        self.assertIs(info.retval, exc)

    def test_trace_exception_propagate(self):
        with self.assertRaises(KeyError):
            trace(raises, (KeyError("foo"), ), {}, propagate=True)
