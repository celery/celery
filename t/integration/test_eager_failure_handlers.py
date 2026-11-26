"""
Integration tests for eager failure handlers (issue #5666).

Based on the reproduction script from PR #10881: with
``task_always_eager`` and ``task_eager_propagates`` enabled, a failing
task used to re-raise the exception before ``on_failure`` and
``after_return`` ran. These tests drive the task end to end through
``.delay()`` on a standalone app, with no worker or external broker,
and assert the failure handlers fire before the exception reaches
the caller.
"""

import pytest

from celery import Celery, Task
from celery.signals import task_failure


def _eager_app():
    """Standalone app configured exactly like the repro script."""
    app = Celery('x', broker='memory://', backend='cache+memory://')
    app.conf.task_always_eager = True
    app.conf.task_eager_propagates = True
    return app


def _hooked_app(calls):
    """App with a task base that records on_failure/after_return."""
    app = _eager_app()

    class Base(Task):
        def after_return(self, status, retval, task_id, args, kwargs,
                         einfo):
            calls.append(('after_return', status))

        def on_failure(self, exc, task_id, args, kwargs, einfo):
            calls.append(('on_failure', repr(exc)))

    return app, Base


def test_eager_propagate_delay_runs_on_failure_and_after_return():
    """Repro from PR #10881: failing eager task via .delay().

    Before the fix, ``calls`` stayed empty because the exception was
    re-raised before any failure handling ran.
    """
    calls = []
    app, Base = _hooked_app(calls)

    @app.task(base=Base)
    def fails():
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        fails.delay()

    assert calls == [
        ('on_failure', "ValueError('boom')"),
        ('after_return', 'FAILURE'),
    ]


def test_eager_propagate_delay_runs_link_error_errback():
    """The link_error errback runs before the exception propagates."""
    calls = []
    app = _eager_app()

    @app.task
    def errback(request, exc, traceback):
        calls.append(('errback', repr(exc)))

    @app.task
    def fails():
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        fails.apply_async(link_error=errback.s())

    assert calls == [('errback', "ValueError('boom')")]


def test_eager_propagate_delay_fires_task_failure_signal():
    """The task_failure signal fires before the exception propagates."""
    calls = []
    app = _eager_app()

    def on_task_failure(sender, task_id, exception, args, kwargs,
                        traceback, einfo, **extra):
        calls.append(('task_failure', repr(exception)))

    task_failure.connect(on_task_failure)
    try:
        @app.task
        def fails():
            raise ValueError('boom')

        with pytest.raises(ValueError, match='boom'):
            fails.delay()
    finally:
        task_failure.disconnect(on_task_failure)

    assert calls == [('task_failure', "ValueError('boom')")]


def test_eager_success_runs_after_return_once():
    """Control: a succeeding eager task still runs after_return once."""
    calls = []
    app, Base = _hooked_app(calls)

    @app.task(base=Base)
    def succeeds():
        return 'ok'

    assert succeeds.delay().get() == 'ok'
    assert calls == [('after_return', 'SUCCESS')]


if __name__ == '__main__':
    # Allow running these tests standalone for debugging.
    print('Running eager failure handler integration tests...')
    test_eager_propagate_delay_runs_on_failure_and_after_return()
    test_eager_propagate_delay_runs_link_error_errback()
    test_eager_propagate_delay_fires_task_failure_signal()
    test_eager_success_runs_after_return_once()
    print('Eager failure handler integration tests completed')
