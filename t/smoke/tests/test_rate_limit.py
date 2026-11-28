from __future__ import annotations

import time

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryBrokerCluster, CeleryTestSetup, RabbitMQTestBroker

from t.smoke.tasks import rate_limited_noop

RATE = 5  # The rate limit of rate_limited_noop, in tasks each second.
SECONDS = 10


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    # The rate limit is applied by the worker consumer, so one broker is enough.
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


def count_rate_limit_timers(celery_setup: CeleryTestSetup) -> int:
    worker = celery_setup.worker
    replies = celery_setup.app.control.broadcast(
        "rate_limit_timers_count",
        destination=[worker.hostname()],
        reply=True,
        timeout=RESULT_TIMEOUT,
    )
    assert len(replies) == 1, replies
    return replies[0][worker.hostname()]


class RateLimitTimers:
    def test_one_timer_per_bucket(self, celery_setup: CeleryTestSetup):
        # More tasks than the worker runs in the test, as with a backlog. Each task that waits
        # for a token must not arm a timer of its own: each such timer arms the next one while
        # the bucket has tasks, so the timers would grow with the tasks received (#5927).
        for _ in range(RATE * SECONDS * 3):
            rate_limited_noop.s().apply_async(queue=celery_setup.worker.worker_queue)

        counts = []
        for _ in range(SECONDS):
            time.sleep(1)
            counts.append(count_rate_limit_timers(celery_setup))

        assert max(counts) <= 1, counts


class test_rate_limit_timers_prefork(RateLimitTimers):
    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls):
        return default_worker_container_cls.command("--pool=prefork", "--concurrency=4")


class test_rate_limit_timers_gevent(RateLimitTimers):
    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls):
        return default_worker_container_cls.command("--pool=gevent", "--concurrency=4")
