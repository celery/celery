from __future__ import absolute_import, unicode_literals

import os
import threading

from celery import worker
from celery.result import allow_join_result, _set_task_join_will_block
from celery.utils.dispatch import Signal

test_worker_starting = Signal(providing_args=[])
test_worker_started = Signal(providing_args=['worker', 'consumer'])
test_worker_stopped = Signal(providing_args=['worker'])

NO_WORKER = os.environ.get('NO_WORKER')
WORKER_LOGLEVEL = os.environ.get('WORKER_LOGLEVEL', 'error')


class TestWorkController(worker.WorkController):

    def __init__(self, *args, **kwargs):
        self._on_started = threading.Event()
        super(TestWorkController, self).__init__(*args, **kwargs)

    def on_consumer_ready(self, consumer):
        self._on_started.set()
        test_worker_started.send(
            sender=self.app, worker=self, consumer=consumer)

    def ensure_started(self):
        self._on_started.wait()


def start_worker(app,
                 concurrency=1,
                 pool='solo',
                 loglevel=WORKER_LOGLEVEL,
                 logfile=None,
                 WorkController=TestWorkController,
                 perform_ping_check=True,
                 ping_task_timeout=3.0,
                 **kwargs):
    test_worker_starting.send(sender=app)

    setup_app_for_worker(app)
    worker = WorkController(
        app=app,
        concurrency=concurrency,
        pool=pool,
        loglevel=loglevel,
        logfile=logfile,
        # not allowed to override TestWorkController.on_consumer_ready
        ready_callback=None,
        **kwargs)

    t = threading.Thread(target=worker.start)
    t.start()

    worker.ensure_started()

    if perform_ping_check:
        from .tasks import ping
        with allow_join_result():
            assert ping.delay().get(ping_task_timeout=3) == 'pong'
    _set_task_join_will_block(False)

    yield worker

    worker.stop()
    test_worker_stopped.send(sender=app, worker=worker)
    t.join()


def setup_app_for_worker(app):
    app.finalize()
    app.set_current()
    app.set_default()
    app.log.setup()
