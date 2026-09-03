import time

from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from t.smoke.tasks import add


def _wait_until(predicate, timeout=10, interval=0.2):
    deadline = time.monotonic() + timeout
    result = predicate()
    while not result and time.monotonic() < deadline:
        time.sleep(interval)
        result = predicate()
    return result


class test_query_task:
    """Smoke test for #5321.

    Schedule a task with a countdown against a real, containerized worker
    and confirm ``inspect().query_task()`` finds it while it's still
    waiting on its ETA -- not only once it starts running, as it did
    before the fix.

    The countdown only needs to be long enough for the scheduled-vs-running
    window to be observable over the broker round trip, so it's kept short
    (2s) to avoid needlessly slowing down the smoke suite.
    """

    def test_query_task_finds_task_scheduled_with_countdown(self, celery_setup: CeleryTestSetup):
        hostname = celery_setup.worker.hostname()
        result = add.s(2, 2).apply_async(countdown=2)
        task_id = result.id
        # A single destination makes kombu wait only for that one reply
        # instead of the full default timeout, so this stays fast.
        inspect = celery_setup.app.control.inspect([hostname])

        queried = {}

        def has_task():
            nonlocal queried
            queried = inspect.query_task(task_id) or {}
            return task_id in queried.get(hostname, {})

        assert _wait_until(has_task), (
            "query_task() never found the task while it was "
            "waiting on its countdown"
        )
        state, _info = queried[hostname][task_id]
        assert state == 'scheduled'

        # sanity check: it was also visible via inspect().scheduled(),
        # exactly as described in the issue.
        scheduled = inspect.scheduled() or {}
        assert any(
            entry.get('request', {}).get('id') == task_id
            for entry in scheduled.get(hostname, [])
        )

        assert result.get(timeout=RESULT_TIMEOUT) == 4
