from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup
from tenacity import retry, stop_after_attempt, wait_fixed

from celery import Celery, chord
from t.integration.tasks import add
from t.smoke.tasks import long_running_task, summarize_results


class test_revoke_chord_member:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        # A single worker slot keeps the second chord member reserved while
        # the first one blocks, so its Request is locally known at revoke time.
        app.conf.worker_concurrency = 1
        return app

    def test_chord_completes_when_reserved_member_is_revoked(
        self,
        celery_setup: CeleryTestSetup,
    ):
        app = celery_setup.app
        worker = celery_setup.worker
        queue = worker.worker_queue

        result = chord(
            [long_running_task.s(8), add.s(1, 2)],
            summarize_results.s(),
        ).apply_async(queue=queue)
        header = result.parent
        slow_id = header.results[0].id
        quick_id = header.results[1].id

        # Wait until the blocking member occupies the worker and the second
        # member has been prefetched, i.e. its Request is locally known.
        @retry(stop=stop_after_attempt(60), wait=wait_fixed(0.5), reraise=True)
        def wait_until_reserved() -> None:
            inspect = app.control.inspect(timeout=2)
            reserved_ids = [
                task['id']
                for tasks in (inspect.reserved() or {}).values()
                for task in tasks
            ]
            assert quick_id in reserved_ids, "quick member is not reserved yet"
            assert slow_id not in reserved_ids, "slow member should be running"

        wait_until_reserved()

        # Revoke the reserved member through the worker control command. This
        # must update the chord bookkeeping so the chord does not wait for a
        # member that will never execute.
        app.control.revoke(quick_id)

        # The worker discards the revoked member once the blocking member
        # finishes.
        worker.assert_log_exists("Discarding revoked task", timeout=30)

        # The chord must reach a terminal state instead of hanging. The
        # callback join raises because the revoked member's stored state is
        # REVOKED, which is the expected way for the chord to account for it.
        result.get(timeout=RESULT_TIMEOUT, propagate=False)
        assert result.state == 'FAILURE'
        assert 'TaskRevokedError' in str(result.result)

        # The revoked member was stored as REVOKED in the backend and the
        # blocking member ran to completion.
        assert header.results[1].state == 'REVOKED'
        assert header.results[0].get(timeout=RESULT_TIMEOUT) is True
