import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker

from celery.canvas import chain, chord, group, signature
from t.smoke.tasks import add, identity


class test_canvas:
    def test_sanity(self, celery_setup: CeleryTestSetup):
        assert celery_setup.ready(ping=True)
        worker: CeleryTestWorker = celery_setup.worker_cluster[0]
        queue = worker.worker_queue
        expected = "test_sanity"
        res = identity.s(expected).apply_async(queue=queue)
        assert res.get(timeout=RESULT_TIMEOUT) == expected

        worker.wait_for_log(expected)

        if len(celery_setup.worker_cluster) > 1:
            queue = celery_setup.worker_cluster[1].worker_queue

        res = add.s(1, 2).apply_async(queue=queue)
        assert res.get(timeout=RESULT_TIMEOUT) == 3

        if len(celery_setup.worker_cluster) > 1:
            assert expected not in celery_setup.worker_cluster[1].logs()
            expected = "succeeded"
            celery_setup.worker_cluster[1].wait_for_log(expected)
        else:
            worker.wait_for_log(expected)

    def test_signature(self, celery_setup: CeleryTestSetup):
        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            queue = worker.worker_queue
            sig = signature(identity, args=("test_signature",), queue=queue)
            assert sig.delay().get(timeout=RESULT_TIMEOUT) == "test_signature"

    def test_group(self, celery_setup: CeleryTestSetup):
        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            queue = worker.worker_queue
            sig = group(
                group(add.si(1, 1), add.si(2, 2)),
                group([add.si(1, 1), add.si(2, 2)]),
                group(s for s in [add.si(1, 1), add.si(2, 2)]),
            )
            res = sig.apply_async(queue=queue)
            assert res.get(timeout=RESULT_TIMEOUT) == [2, 4, 2, 4, 2, 4]

    def test_chain(self, celery_setup: CeleryTestSetup):
        worker: CeleryTestWorker
        for worker in celery_setup.worker_cluster:
            queue = worker.worker_queue
            sig = chain(
                identity.si("chain_task1").set(queue=queue),
                identity.si("chain_task2").set(queue=queue),
            ) | identity.si("test_chain").set(queue=queue)
            res = sig.apply_async()
            assert res.get(timeout=RESULT_TIMEOUT) == "test_chain"

    def test_chord(self, celery_setup: CeleryTestSetup):
        worker: CeleryTestWorker
        if not celery_setup.chords_allowed():
            pytest.skip("Chords are not supported")

        for worker in celery_setup.worker_cluster:
            queue = worker.worker_queue

            upgraded_chord = signature(
                group(
                    identity.si("header_task1"),
                    identity.si("header_task2"),
                )
                | identity.si("body_task"),
                queue=queue,
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
            res = sig.apply_async(queue=queue)
            assert res.get(timeout=RESULT_TIMEOUT) == ["body_task"] * 3
