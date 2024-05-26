import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery.canvas import chain, chord, group, signature
from t.integration.tasks import ExpectedException, add, fail, identity


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

    def test_chain_gets_last_task_id_with_failing_tasks_in_chain(self, celery_setup: CeleryTestSetup):
        """https://github.com/celery/celery/issues/8786"""
        queue = celery_setup.worker.worker_queue
        sig = chain(
            identity.si("start").set(queue=queue),
            group(
                identity.si("a").set(queue=queue),
                fail.si().set(queue=queue),
            ),
            identity.si("break").set(queue=queue),
            identity.si("end").set(queue=queue),
        )
        res = sig.apply_async()
        celery_setup.worker.assert_log_does_not_exist(
            "ValueError: task_id must not be empty. Got None instead."
        )

        with pytest.raises(ExpectedException):
            res.get(timeout=RESULT_TIMEOUT)


class test_chord:
    def test_sanity(self, celery_setup: CeleryTestSetup):
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
