from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

import pytest
from pytest_celery import CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery import Celery
from celery.app.base import set_default_app
from celery.signals import after_task_publish
from t.integration.tasks import identity


@pytest.fixture(
    params=[
        # Single worker
        ["celery_setup_worker"],
        # Workers cluster (same queue)
        ["celery_setup_worker", "celery_alt_dev_worker"],
    ]
)
def celery_worker_cluster(request: pytest.FixtureRequest) -> CeleryWorkerCluster:
    nodes: tuple[CeleryTestWorker] = [
        request.getfixturevalue(worker) for worker in request.param
    ]
    cluster = CeleryWorkerCluster(*nodes)
    yield cluster
    cluster.teardown()


class test_thread_safety:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.broker_pool_limit = 42
        return app

    @pytest.mark.parametrize(
        "threads_count",
        [
            # Single
            1,
            # Multiple
            2,
            # Many
            42,
        ],
    )
    def test_multithread_task_publish(
        self,
        celery_setup: CeleryTestSetup,
        threads_count: int,
    ):
        signal_was_called = Mock()

        @after_task_publish.connect
        def after_task_publish_handler(*args, **kwargs):
            signal_was_called(True)

        def thread_worker():
            set_default_app(celery_setup.app)
            identity.si("Published from thread").apply_async(
                queue=celery_setup.worker.worker_queue
            )

        executor = ThreadPoolExecutor(threads_count)

        with executor:
            for _ in range(threads_count):
                executor.submit(thread_worker)

        assert signal_was_called.call_count == threads_count

    def test_event_dispatcher_close_waits_for_in_flight_publish(
        self,
        celery_setup: CeleryTestSetup,
    ):
        """Close waits for sends while explicit producers remain usable."""
        publish_started = Event()
        finish_publish = Event()
        close_started = Event()
        close_finished = Event()
        published_ids = []

        with celery_setup.app.connection_for_write() as connection:
            dispatcher = celery_setup.app.events.Dispatcher(
                connection, buffer_while_offline=False,
            )
            producer = dispatcher.producer
            original_publish = producer.publish

            def blocking_publish(*args, **kwargs):
                publish_started.set()
                if not finish_publish.wait(10):
                    raise TimeoutError("event publish was not allowed to finish")
                result = original_publish(*args, **kwargs)
                published_ids.append(args[0]["uuid"])
                return result

            def close_dispatcher():
                close_started.set()
                try:
                    dispatcher.close()
                finally:
                    close_finished.set()

            producer.publish = blocking_publish
            with ThreadPoolExecutor(max_workers=2) as pool:
                publisher = pool.submit(
                    dispatcher.send, "task-sent", uuid="in-flight-event",
                )
                try:
                    assert publish_started.wait(10)
                    closer = pool.submit(close_dispatcher)
                    assert close_started.wait(10)
                    assert not close_finished.wait(0.2), (
                        "close() returned while publish() was still in progress"
                    )
                finally:
                    finish_publish.set()

                publisher.result(timeout=10)
                closer.result(timeout=10)

            assert dispatcher.producer is None
            assert published_ids == ["in-flight-event"]

            dispatcher.send("task-sent", uuid="closed-send")
            # close() detaches the producer; explicit publication still works.
            dispatcher.publish(
                "task-sent", {"uuid": "explicit-event"}, producer,
            )
            assert published_ids == ["in-flight-event", "explicit-event"]
