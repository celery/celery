import pytest

from .tasks import add


@pytest.fixture(scope="session")
def celery_worker_pool():
    return "spawn"


@pytest.mark.flaky(reruns=5, reruns_delay=2)
def test_basic_task_spawn(manager):
    results = []
    for i in range(5):
        results.append([i + i, add.delay(i, i)])
    for expected, result in results:
        assert result.get(timeout=10) == expected
        assert result.status == "SUCCESS"
        assert result.ready() is True
        assert result.successful() is True
