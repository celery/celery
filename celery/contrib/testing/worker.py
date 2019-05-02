"""Embedded workers for integration tests."""
from __future__ import absolute_import, unicode_literals

import os
import threading
from contextlib import contextmanager

from celery import worker
from celery.result import _set_task_join_will_block, allow_join_result
from celery.utils.dispatch import Signal
from celery.utils.nodenames import anon_nodename

WORKER_LOGLEVEL = os.environ.get('WORKER_LOGLEVEL', 'error')

test_worker_starting = Signal(
    name='test_worker_starting',
    providing_args={},
)
test_worker_started = Signal(
    name='test_worker_started',
    providing_args={'worker', 'consumer'},
)
test_worker_stopped = Signal(
    name='test_worker_stopped',
    providing_args={'worker'},
)


class TestWorkController(worker.WorkController):
    """Worker that can synchronize on being fully started."""

    def __init__(self, *args, **kwargs):
        # type: (*Any, **Any) -> None
        self._on_started = threading.Event()
        super(TestWorkController, self).__init__(*args, **kwargs)

    def on_consumer_ready(self, consumer):
        # type: (celery.worker.consumer.Consumer) -> None
        """Callback called when the Consumer blueprint is fully started."""
        self._on_started.set()
        test_worker_started.send(
            sender=self.app, worker=self, consumer=consumer)

    def ensure_started(self):
        # type: () -> None
        """Wait for worker to be fully up and running.

        Warning:
            Worker must be started within a thread for this to work,
            or it will block forever.
        """
        self._on_started.wait()


@contextmanager
def start_worker(app,
                 concurrency=1,
                 pool='solo',
                 loglevel=WORKER_LOGLEVEL,
                 logfile=None,
                 perform_ping_check=True,
                 ping_task_timeout=10.0,
                 **kwargs):
    # type: (Celery, int, str, Union[str, int],
    #        str, bool, float, **Any) -> # Iterable
    """Start embedded worker.

    Yields:
        celery.app.worker.Worker: worker instance.
    """
    test_worker_starting.send(sender=app)

    with _start_worker_thread(app,
                              concurrency=concurrency,
                              pool=pool,
                              loglevel=loglevel,
                              logfile=logfile,
                              **kwargs) as worker:
        if perform_ping_check:
            from .tasks import ping
            with allow_join_result():
                assert ping.delay().get(timeout=ping_task_timeout) == 'pong'

        yield worker
    test_worker_stopped.send(sender=app, worker=worker)


@contextmanager
def _start_worker_thread(app,
                         concurrency=1,
                         pool='solo',
                         loglevel=WORKER_LOGLEVEL,
                         logfile=None,
                         WorkController=TestWorkController,
                         **kwargs):
    # type: (Celery, int, str, Union[str, int], str, Any, **Any) -> Iterable
    """Start Celery worker in a thread.

    Yields:
        celery.worker.Worker: worker instance.
    """
    setup_app_for_worker(app, loglevel, logfile)
    assert 'celery.ping' in app.tasks
    # Make sure we can connect to the broker
    with app.connection(hostname=os.environ.get('TEST_BROKER')) as conn:
        conn.default_channel.queue_declare

    worker = WorkController(
        app=app,
        concurrency=concurrency,
        hostname=anon_nodename(),
        pool=pool,
        loglevel=loglevel,
        logfile=logfile,
        # not allowed to override TestWorkController.on_consumer_ready
        ready_callback=None,
        without_heartbeat=True,
        without_mingle=True,
        without_gossip=True,
        **kwargs)

    t = threading.Thread(target=worker.start)
    t.start()
    worker.ensure_started()
    _set_task_join_will_block(False)

    yield worker

    from celery.worker import state
    state.should_terminate = 0
    t.join(10)
    state.should_terminate = None


@contextmanager
def _start_worker_process(app,
                          concurrency=1,
                          pool='solo',
                          loglevel=WORKER_LOGLEVEL,
                          logfile=None,
                          **kwargs):
    # type (Celery, int, str, Union[int, str], str, **Any) -> Iterable
    """Start worker in separate process.

    Yields:
        celery.app.worker.Worker: worker instance.
    """
    from celery.apps.multi import Cluster, Node

    app.set_current()
    cluster = Cluster([Node('testworker1@%h')])
    cluster.start()
    yield
    cluster.stopwait()


def setup_app_for_worker(app, loglevel, logfile):
    # type: (Celery, Union[str, int], str) -> None
    """Setup the app to be used for starting an embedded worker."""
    app.finalize()
    app.set_current()
    app.set_default()
    type(app.log)._setup = False
    app.log.setup(loglevel=loglevel, logfile=logfile)
