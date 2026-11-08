"""Revoked ids received from another worker, on mingle or through ``hello``.

They are stamped with the receiving worker's own clock, whatever stamps
they come with (#4300): a set filled with stamps ahead of the local clock
would otherwise purge every id revoked from then on the moment it is
added, and the task would run.
"""
from __future__ import annotations

from time import monotonic

import pytest
from pytest_celery import (CeleryBrokerCluster, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster,
                           RedisTestBroker)
from tenacity import retry, stop_after_attempt, wait_fixed

from celery import uuid
from t.smoke.tasks import noop

REVOKES_MAX = 3


def revoked_ids(setup: CeleryTestSetup, worker: CeleryTestWorker) -> list[str]:
    reply = setup.app.control.inspect(
        destination=[worker.hostname()], timeout=5,
    ).revoked() or {}
    return reply.get(worker.hostname(), [])


@retry(stop=stop_after_attempt(60), wait=wait_fixed(1), reraise=True)
def assert_revoked(setup: CeleryTestSetup, worker: CeleryTestWorker, task_id: str) -> None:
    assert task_id in revoked_ids(setup, worker), f"{task_id} is not revoked on {worker.hostname()}"


class test_hello:
    @pytest.fixture
    def default_worker_env(self, default_worker_env: dict) -> dict:
        # A set this small is full after one hello.
        default_worker_env.update({"CELERY_WORKER_REVOKES_MAX": str(REVOKES_MAX)})
        return default_worker_env

    def test_received_ids_are_stamped_locally(self, celery_setup: CeleryTestSetup):
        app = celery_setup.app
        worker = celery_setup.worker

        # Play a worker on a host with a longer uptime: its stamps are ahead
        # of the clock of the worker under test.
        ahead = monotonic() + 10 ** 6
        received = [uuid() for _ in range(REVOKES_MAX)]
        reply = app.control.inspect(
            destination=[worker.hostname()], timeout=5,
        ).hello(
            "neighbour@elsewhere",
            revoked={task_id: [ahead, seq, task_id] for seq, task_id in enumerate(received)},
        )

        # The set is full: an id revoked now must stay, the received ids
        # being the oldest by the local clock -- or the newest, as sent.
        revoked = uuid()
        app.control.revoke(revoked, destination=[worker.hostname()])
        assert_revoked(celery_setup, worker, revoked)

        # And a task with an ETA revoked while the worker holds it is
        # discarded rather than run.
        result = noop.s().apply_async(queue=worker.worker_queue, countdown=10)
        worker.assert_log_exists(f"noop[{result.id}] received")
        result.revoke()
        worker.assert_log_exists(f"Discarding revoked task: t.smoke.tasks.noop[{result.id}]")
        worker.assert_log_does_not_exist(f"noop[{result.id}] succeeded", timeout=5)

        # The reply to the hello carried the ids alone.
        ours = reply[worker.hostname()]["revoked"]
        assert isinstance(ours, list)
        assert set(received) <= set(ours)


class test_mingle:
    @pytest.fixture
    def celery_broker_cluster(self, celery_redis_broker: RedisTestBroker) -> CeleryBrokerCluster:
        # The last release cannot mingle on RabbitMQ 4.3, which refuses
        # its transient non-exclusive pidbox reply queue.
        cluster = CeleryBrokerCluster(celery_redis_broker)
        yield cluster
        cluster.teardown()

    @pytest.fixture
    def celery_worker_cluster(
        self,
        celery_worker: CeleryTestWorker,
        celery_latest_worker: CeleryTestWorker,
    ) -> CeleryWorkerCluster:
        # The last release and the current source in one cluster: each
        # side receives the other's format on mingle.
        cluster = CeleryWorkerCluster(celery_worker, celery_latest_worker)
        yield cluster
        cluster.teardown()

    def test_sync_with_another_version(self, celery_setup: CeleryTestSetup):
        app = celery_setup.app
        dev, latest = celery_setup.worker_cluster

        # A worker restarting takes the revoked ids of its neighbour.
        revoked_on_dev = uuid()
        app.control.revoke(revoked_on_dev, destination=[dev.hostname()])
        assert_revoked(celery_setup, dev, revoked_on_dev)
        latest.restart()
        assert_revoked(celery_setup, latest, revoked_on_dev)

        revoked_on_latest = uuid()
        app.control.revoke(revoked_on_latest, destination=[latest.hostname()])
        assert_revoked(celery_setup, latest, revoked_on_latest)
        dev.restart()
        assert_revoked(celery_setup, dev, revoked_on_latest)
        # ...and gets back the id it had revoked before its restart, which
        # the other release took on its own.
        assert_revoked(celery_setup, dev, revoked_on_dev)
