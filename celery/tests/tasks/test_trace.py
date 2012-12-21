from __future__ import absolute_import
from __future__ import with_statement

from mock import patch

from celery import current_app
from celery import states
from celery.exceptions import RetryTaskError
from celery.task.trace import TraceInfo, eager_trace_task, trace_task
from celery.tests.utils import Case, Mock


@current_app.task
def add(x, y):
    return x + y


@current_app.task(ignore_result=True)
def add_cast(x, y):
    return x + y


@current_app.task
def raises(exc):
    raise exc


def trace(task, args=(), kwargs={}, propagate=False):
    return eager_trace_task(task, 'id-1', args, kwargs,
                            propagate=propagate)


class test_trace(Case):

    def test_trace_successful(self):
        retval, info = trace(add, (2, 2), {})
        self.assertIsNone(info)
        self.assertEqual(retval, 4)

    def test_trace_SystemExit(self):
        with self.assertRaises(SystemExit):
            trace(raises, (SystemExit(), ), {})

    def test_trace_RetryTaskError(self):
        exc = RetryTaskError('foo', 'bar')
        _, info = trace(raises, (exc, ), {})
        self.assertEqual(info.state, states.RETRY)
        self.assertIs(info.retval, exc)

    def test_trace_exception(self):
        exc = KeyError('foo')
        _, info = trace(raises, (exc, ), {})
        self.assertEqual(info.state, states.FAILURE)
        self.assertIs(info.retval, exc)

    def test_trace_exception_propagate(self):
        with self.assertRaises(KeyError):
            trace(raises, (KeyError('foo'), ), {}, propagate=True)

    @patch('celery.task.trace.build_tracer')
    @patch('celery.task.trace.report_internal_error')
    def test_outside_body_error(self, report_internal_error, build_tracer):
        tracer = Mock()
        tracer.side_effect = KeyError('foo')
        build_tracer.return_value = tracer

        @current_app.task
        def xtask():
            pass

        trace_task(xtask, 'uuid', (), {})
        self.assertTrue(report_internal_error.call_count)
        self.assertIs(xtask.__trace__, tracer)


class test_TraceInfo(Case):

    class TI(TraceInfo):
        __slots__ = TraceInfo.__slots__ + ('__dict__', )

    def test_handle_error_state(self):
        x = self.TI(states.FAILURE)
        x.handle_failure = Mock()
        x.handle_error_state(add_cast)
        x.handle_failure.assert_called_with(
            add_cast,
            store_errors=add_cast.store_errors_even_if_ignored,
        )
