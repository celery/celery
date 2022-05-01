import pytest

from celery import Celery
from celery.contrib.testing.worker import start_worker

app = Celery('celerytest',
             backend='cache+memory://',
             broker='memory://',
             )


@app.task
def add(x, y):
    return x + y


def test_start_worker():
    app.config_from_object({
        'worker_hijack_root_logger': False,
    })
    # this import adds a @shared_task, which uses connect_on_app_finalize
    # to install the celery.ping task that the test lib uses
    import celery.contrib.testing.tasks  # noqa: F401

    # to avoid changing the root logger level to ERROR,
    # we have we have to set both app.log.loglevel start_worker arg to 0
    # (see celery.app.log.setup_logging_subsystem)
    app.log.loglevel = 0
    with start_worker(app=app, loglevel=0):
        result = add.s(1, 2).apply_async()
        val = result.get(timeout=5)
    assert val == 3


@app.task
def error_task():
    raise NotImplementedError()


def test_start_worker_with_exception():
    """Make sure that start_worker does not hang on exception"""
    app.config_from_object({
        'worker_hijack_root_logger': False,
    })
    # this import adds a @shared_task, which uses connect_on_app_finalize
    # to install the celery.ping task that the test lib uses
    import celery.contrib.testing.tasks  # noqa: F401

    # to avoid changing the root logger level to ERROR,
    # we have we have to set both app.log.loglevel start_worker arg to 0
    # (see celery.app.log.setup_logging_subsystem)
    app.log.loglevel = 0
    with pytest.raises(NotImplementedError):
        with start_worker(app=app, loglevel=0):
            result = error_task.apply_async()
            result.get(timeout=5)
