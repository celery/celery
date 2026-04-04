"""Smoke tests for chord_unlock routing to quorum queues.

Reproduces Celery Discussion #9742: when chord_unlock is routed to a
quorum queue via topic exchange, it may not be consumed even if declared
and bound.
"""
from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery import chord
from t.smoke.tasks import add, summarize_results


class test_chord_unlock_routing:
    """Chord unlock routing to quorum queues with topic exchanges."""

    @pytest.mark.xfail(
        reason="chord_unlock routed to quorum/topic queue intermittently fails under load",
    )
    def test_chord_unlock_stress_routing(self, celery_setup: CeleryTestSetup):
        """Submit multiple chords routed via topic exchange and verify completion."""
        queue = celery_setup.worker.worker_queue
        chord_count = 10
        failures = []

        results = []
        for i in range(chord_count):
            header = [add.s(i, j).set(queue=queue) for j in range(3)]
            callback = summarize_results.s().set(queue=queue)
            result = chord(header)(callback)
            results.append((i, result))

        for i, result in results:
            try:
                result.get(timeout=RESULT_TIMEOUT)
            except Exception as exc:
                failures.append((i, exc))

        assert not failures, f"{len(failures)} of {chord_count} chords failed or got stuck"
