import time

from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from t.smoke.tasks import add


def _wait_until(predicate, timeout=RESULT_TIMEOUT, interval=0.5):
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

    The countdown needs enough headroom over the control-command round
    trip (broker + worker scheduling delay, which can be significant
    under CI load) that the task is still reliably waiting on its ETA
    when we check -- otherwise it can already be done and gone from
    worker state by the time we look, which isn't something a polling
    loop can recover from. Both checks below are done in the same poll
    iteration so they can't race each other across the same deadline.
    """

    def test_query_task_finds_task_scheduled_with_countdown(self, celery_setup: CeleryTestSetup):
        hostname = celery_setup.worker.hostname()
        result = add.s(2, 2).apply_async(
            countdown=10, queue=celery_setup.worker.worker_queue)
        task_id = result.id
        # A single destination makes kombu wait only for that one reply
        # instead of the full default timeout, so this stays fast.
        inspect = celery_setup.app.control.inspect([hostname])

        found = {}

        def is_scheduled():
            queried = inspect.query_task(task_id) or {}
            if task_id not in queried.get(hostname, {}):
                return False
            state, _info = queried[hostname][task_id]
            if state != 'scheduled':
                return False

            # sanity check: it's also visible via inspect().scheduled(),
            # exactly as described in the issue. Checked in the same poll
            # iteration as the query_task() result above so it can't race
            # past the ETA on its own.
            scheduled = inspect.scheduled() or {}
            found['seen_in_scheduled'] = any(
                entry.get('request', {}).get('id') == task_id
                for entry in scheduled.get(hostname, [])
            )
            return found['seen_in_scheduled']

        assert _wait_until(is_scheduled), (
            "query_task()/scheduled() never found the task while it was "
            "waiting on its countdown"
        )

        assert result.get(timeout=RESULT_TIMEOUT) == 4
