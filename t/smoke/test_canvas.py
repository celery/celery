import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery.canvas import chain, chord, group, signature
from t.smoke.tasks import add, identity


class test_signature:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        sig = signature(identity, args=("test_signature",), queue=celery_setup.worker.worker_queue)
        assert sig.delay().get(timeout=RESULT_TIMEOUT) == "test_signature"


class test_group:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        sig = group(
            group(add.si(1, 1), add.si(2, 2)),
            group([add.si(1, 1), add.si(2, 2)]),
            group(s for s in [add.si(1, 1), add.si(2, 2)]),
        )
        res = sig.apply_async(queue=celery_setup.worker.worker_queue)
        assert res.get(timeout=RESULT_TIMEOUT) == [2, 4, 2, 4, 2, 4]


class test_chain:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        sig = chain(
            identity.si("chain_task1").set(queue=queue),
            identity.si("chain_task2").set(queue=queue),
        ) | identity.si("test_chain").set(queue=queue)
        res = sig.apply_async()
        assert res.get(timeout=RESULT_TIMEOUT) == "test_chain"


class test_chord:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        if not celery_setup.chords_allowed():
            pytest.skip("Chords are not supported")

        upgraded_chord = signature(
            group(
                identity.si("header_task1"),
                identity.si("header_task2"),
            )
            | identity.si("body_task"),
            queue=celery_setup.worker.worker_queue,
        )

        sig = group(
            [
                upgraded_chord,
                chord(
                    group(
                        identity.si("header_task3"),
                        identity.si("header_task4"),
                    ),
                    identity.si("body_task"),
                ),
                chord(
                    (
                        sig
                        for sig in [
                            identity.si("header_task5"),
                            identity.si("header_task6"),
                        ]
                    ),
                    identity.si("body_task"),
                ),
            ]
        )
        res = sig.apply_async(queue=celery_setup.worker.worker_queue)
        assert res.get(timeout=RESULT_TIMEOUT) == ["body_task"] * 3
