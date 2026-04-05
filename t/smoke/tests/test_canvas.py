import uuid

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery.canvas import chain, chord, group, signature
from t.integration.conftest import get_redis_connection
from t.integration.tasks import ExpectedException, add, fail, identity, redis_echo


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
        celery_setup.worker.assert_log_does_not_exist("ValueError: task_id must not be empty. Got None instead.")

        with pytest.raises(ExpectedException):
            res.get(timeout=RESULT_TIMEOUT)

    def test_upgrade_to_chord_inside_chains(self, celery_setup: CeleryTestSetup):
        redis_key = str(uuid.uuid4())
        queue = celery_setup.worker.worker_queue
        group1 = group(redis_echo.si("a", redis_key), redis_echo.si("a", redis_key))
        group2 = group(redis_echo.si("a", redis_key), redis_echo.si("a", redis_key))
        chord1 = group1 | group2
        chain1 = chain(chord1, (redis_echo.si("a", redis_key) | redis_echo.si("b", redis_key).set(queue=queue)))
        chain1.apply_async(queue=queue).get(timeout=RESULT_TIMEOUT)
        redis_connection = get_redis_connection()
        actual = redis_connection.lrange(redis_key, 0, -1)
        assert actual.count(b"a") == 5
        assert actual.count(b"b") == 1
        redis_connection.delete(redis_key)


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

    @pytest.mark.parametrize(
        "input_body",
        [
            (lambda queue: add.si(9, 7).set(queue=queue)),
            (
                lambda queue: chain(
                    add.si(9, 7).set(queue=queue),
                    add.si(5, 7).set(queue=queue),
                )
            ),
            (
                lambda queue: group(
                    [
                        add.si(9, 7).set(queue=queue),
                        add.si(5, 7).set(queue=queue),
                    ]
                )
            ),
            (
                lambda queue: chord(
                    group(
                        [
                            add.si(1, 1).set(queue=queue),
                            add.si(2, 2).set(queue=queue),
                        ]
                    ),
                    add.si(10, 10).set(queue=queue),
                )
            ),
        ],
        ids=[
            "body is a single_task",
            "body is a chain",
            "body is a group",
            "body is a chord",
        ],
    )
    def test_chord_error_propagation_with_different_body_types(
        self, celery_setup: CeleryTestSetup, input_body
    ) -> None:
        """Reproduce issue #9773 with different chord body types.

        This test verifies that the "task_id must not be empty" error is fixed
        regardless of the chord body type. The issue occurs when:
        1. A chord has a group with both succeeding and failing tasks
        2. The chord body can be any signature type (single task, chain, group, chord)
        3. When the group task fails, error propagation should work correctly

        Args:
            input_body (callable): A callable that returns a Celery signature for the chord body.
        """
        queue = celery_setup.worker.worker_queue

        # Create the failing group header (same for all tests)
        failing_group = group(
            [
                add.si(15, 7).set(queue=queue),
                # failing task
                fail.si().set(queue=queue),
            ]
        )

        # Create the chord
        test_chord = chord(failing_group, input_body(queue))

        result = test_chord.apply_async()

        # The worker should not log the "task_id must not be empty" error
        celery_setup.worker.assert_log_does_not_exist(
            "ValueError: task_id must not be empty. Got None instead."
        )

        # The chord should fail with the expected exception from the failing task
        with pytest.raises(ExpectedException):
            result.get(timeout=RESULT_TIMEOUT)


class test_complex_workflow:
    def test_pending_tasks_released_on_forget(self, celery_setup: CeleryTestSetup):
        sig = add.si(1, 1) | group(
            add.s(1) | group(add.si(1, 1), add.si(2, 2)) | add.si(2, 2),
            add.s(1) | group(add.si(1, 1), add.si(2, 2)) | add.si(2, 2)
        ) | add.si(1, 1)
        res = sig.apply_async(queue=celery_setup.worker.worker_queue)
        assert not all(len(mapping) == 0 for mapping in res.backend._pending_results)
        res.forget()
        assert all(len(mapping) == 0 for mapping in res.backend._pending_results)
