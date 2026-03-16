"""Integration tests for chain/callback dispatch on the dedup fast-path.

When ``worker_deduplicate_successful_tasks=True`` and
``task_acks_late=True``, a redelivered task that hits the dedup
fast-path in ``trace.py`` must still dispatch its chain and link
callbacks.

See https://github.com/celery/celery/issues/9835
"""

import pytest

from celery import chain
from celery.contrib.testing.worker import start_worker
from celery.result import AsyncResult

from .conftest import flaky
from .tasks import add, identity, reject_then_succeed, store_success_then_reject

TIMEOUT = 60


@pytest.fixture()
def dedup_worker(celery_session_app):
    """Solo worker with dedup enabled.

    Temporarily enables ``worker_deduplicate_successful_tasks`` on the
    session app, starts a solo worker, and restores the original
    setting on teardown.
    """
    if not celery_session_app.backend.persistent:
        raise pytest.skip('Requires a persistent result backend.')

    orig_dedup = celery_session_app.conf.worker_deduplicate_successful_tasks
    orig_acks_late = celery_session_app.conf.task_acks_late
    celery_session_app.conf.worker_deduplicate_successful_tasks = True
    celery_session_app.conf.task_acks_late = True

    try:
        with start_worker(
            celery_session_app,
            pool='solo',
            concurrency=1,
            perform_ping_check=False,
            shutdown_timeout=TIMEOUT,
        ) as worker:
            yield worker
    finally:
        celery_session_app.conf.worker_deduplicate_successful_tasks = orig_dedup
        celery_session_app.conf.task_acks_late = orig_acks_late


class test_dedup_chain_dispatch:
    """Test chain/callback dispatch on the dedup fast-path."""

    @flaky
    @pytest.mark.usefixtures('dedup_worker')
    def test_chain_completes_with_dedup_enabled(self):
        """Smoke test: a normal chain works when dedup is on."""
        c = chain(add.s(2, 3), add.s(5))
        assert c().get(timeout=TIMEOUT) == 10

    @flaky
    @pytest.mark.usefixtures('dedup_worker')
    def test_reject_requeue_completes_chain(self):
        """Reject passthrough: chain completes after rejection + redelivery."""
        c = chain(reject_then_succeed.s(), identity.s())
        assert c().get(timeout=TIMEOUT) == 'second-pass'

    @flaky
    @pytest.mark.usefixtures('dedup_worker')
    def test_dedup_dispatches_chain_on_redelivery(self):
        """Core test: dedup fast-path dispatches the chain."""
        c = chain(store_success_then_reject.s(), identity.s())
        assert c().get(timeout=TIMEOUT) == 'first-pass'

    @flaky
    @pytest.mark.usefixtures('dedup_worker')
    def test_dedup_dispatches_callback_on_redelivery(self, celery_session_app):
        """Dedup fast-path dispatches link callbacks."""
        import uuid as _uuid
        cb_id = _uuid.uuid4().hex
        sig = store_success_then_reject.s()
        sig.link(identity.s().set(task_id=cb_id))
        sig.apply_async()
        cb_result = AsyncResult(cb_id, app=celery_session_app)
        assert cb_result.get(timeout=TIMEOUT) == 'first-pass'
