"""Smoke tests for chain/callback dispatch on the dedup fast-path.

When ``worker_deduplicate_successful_tasks=True`` and
``task_acks_late=True``, a redelivered task that hits the dedup
fast-path in ``trace.py`` must still dispatch its chain and link
callbacks.

See https://github.com/celery/celery/issues/9835
"""
from __future__ import annotations

import uuid

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery import Celery
from celery.canvas import chain
from celery.result import AsyncResult
from t.integration.tasks import identity, reject_then_succeed, store_success_then_reject


class test_dedup_chain_dispatch:
    """Dedup fast-path must dispatch chain and link callbacks on redelivery."""

    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_deduplicate_successful_tasks = True
        app.conf.task_acks_late = True
        return app

    def test_reject_requeue_completes_chain(self, celery_setup: CeleryTestSetup):
        """Reject passthrough: chain completes after rejection + redelivery."""
        queue = celery_setup.worker.worker_queue
        c = chain(
            reject_then_succeed.s().set(queue=queue),
            identity.s().set(queue=queue),
        )
        assert c.apply_async().get(timeout=RESULT_TIMEOUT) == 'second-pass'

    def test_dedup_dispatches_chain_on_redelivery(self, celery_setup: CeleryTestSetup):
        """Core: dedup fast-path dispatches the chain on redelivery."""
        queue = celery_setup.worker.worker_queue
        c = chain(
            store_success_then_reject.s().set(queue=queue),
            identity.s().set(queue=queue),
        )
        assert c.apply_async().get(timeout=RESULT_TIMEOUT) == 'first-pass'

    def test_dedup_dispatches_callback_on_redelivery(self, celery_setup: CeleryTestSetup):
        """Dedup fast-path dispatches link callbacks on redelivery."""
        queue = celery_setup.worker.worker_queue
        cb_id = uuid.uuid4().hex
        sig = store_success_then_reject.s().set(queue=queue)
        sig.link(identity.s().set(task_id=cb_id, queue=queue))
        sig.apply_async()
        cb_result = AsyncResult(cb_id, app=celery_setup.app)
        assert cb_result.get(timeout=RESULT_TIMEOUT) == 'first-pass'
