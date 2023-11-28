import pytest


@pytest.fixture
def default_worker_tasks(default_worker_tasks: set) -> set:
    from t.smoke.tests.stamping import tasks as stamping_tasks

    default_worker_tasks.add(stamping_tasks)
    yield default_worker_tasks


@pytest.fixture
def default_worker_signals(default_worker_signals: set) -> set:
    from t.smoke.tests.stamping import signals

    default_worker_signals.add(signals)
    yield default_worker_signals
