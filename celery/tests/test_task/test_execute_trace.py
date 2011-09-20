from __future__ import absolute_import
from __future__ import with_statement

import operator

from celery import states
from celery.exceptions import RetryTaskError
from celery.execute.trace import TraceInfo
from celery.tests.utils import unittest

trace = TraceInfo.trace


def raises(exc):
    raise exc


class test_TraceInfo(unittest.TestCase):

    def test_trace_successful(self):
        info = trace(operator.add, (2, 2), {})
        self.assertEqual(info.status, states.SUCCESS)
        self.assertEqual(info.retval, 4)

    def test_trace_SystemExit(self):
        with self.assertRaises(SystemExit):
            trace(raises, (SystemExit(), ), {})

    def test_trace_RetryTaskError(self):
        exc = RetryTaskError("foo", "bar")
        info = trace(raises, (exc, ), {})
        self.assertEqual(info.status, states.RETRY)
        self.assertIs(info.retval, exc)

    def test_trace_exception(self):
        exc = KeyError("foo")
        info = trace(raises, (exc, ), {})
        self.assertEqual(info.status, states.FAILURE)
        self.assertIs(info.retval, exc)

    def test_trace_exception_propagate(self):
        with self.assertRaises(KeyError):
            trace(raises, (KeyError("foo"), ), {}, propagate=True)
