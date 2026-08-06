"""Tests for :mod:`celery.app.autoretry`."""
from unittest.mock import patch

from celery import Celery
from celery.exceptions import Retry


class test_SharedRetryKwargs:

    def test_retry_kwargs_not_mutated_across_executions(self):
        """The `retry_kwargs` dict captured by the run() closure must not be
        mutated by a task execution. Before the fix, the shared dict built
        once at task-registration time was modified in place on every retry,
        leaking a `countdown` key (and potentially `max_retries`) into the
        task-level dict and corrupting backoff under concurrent execution.
        """
        app = Celery(set_as_current=False)

        @app.task(
            bind=True,
            shared=False,
            autoretry_for=(ZeroDivisionError,),
            retry_backoff=True,
            retry_jitter=False,
            max_retries=3,
        )
        def task(self_, x, y):
            return x / y

        # Locate the `retry_kwargs` dict captured by the run() closure at
        # registration time -- this is the exact object shared across every
        # future execution of this task in the same worker process.
        free_vars = task.run.__code__.co_freevars
        shared_retry_kwargs = task.run.__closure__[
            free_vars.index("retry_kwargs")
        ].cell_contents

        assert shared_retry_kwargs == {}

        with patch.object(task, "retry", side_effect=Retry):
            for retries in range(3):
                task.push_request(retries=retries)
                try:
                    task.run(1, 0)
                except Retry:
                    pass
                finally:
                    task.pop_request()

        # The shared dict must remain untouched after retries.
        assert shared_retry_kwargs == {}, (
            "BUG: shared retry_kwargs dict was mutated!"
        )

    def test_per_invocation_countdown_uses_own_retries(self):
        """Each invocation must compute its backoff countdown from its own
        `task.request.retries`, not from a value written by another execution.
        """
        app = Celery(set_as_current=False)

        @app.task(
            bind=True,
            shared=False,
            autoretry_for=(ZeroDivisionError,),
            retry_backoff=True,
            retry_jitter=False,
            max_retries=3,
        )
        def task(self_, x, y):
            return x / y

        captured = {}

        def fake_retry(exc=None, **kwargs):
            captured["countdown"] = kwargs.get("countdown")
            captured["max_retries"] = kwargs.get("max_retries")
            raise Retry()

        with patch.object(task, "retry", side_effect=fake_retry):
            task.push_request(retries=0)
            try:
                task.run(1, 0)
            except Retry:
                pass
            finally:
                task.pop_request()

        # countdown for retries=0 with backoff factor 1, full_jitter disabled
        assert captured["countdown"] is not None
        assert captured["countdown"] >= 0