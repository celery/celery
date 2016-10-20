from __future__ import absolute_import, unicode_literals

import os
import threading

from contextlib import contextmanager

from celery import worker
from celery.result import allow_join_result, _set_task_join_will_block
from celery.utils.dispatch import Signal
from celery.utils.nodenames import anon_nodename

test_worker_starting = Signal(providing_args=[])
test_worker_started = Signal(providing_args=['worker', 'consumer'])
test_worker_stopped = Signal(providing_args=['worker'])

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


@contextmanager
def start_worker_thread(app,
                        concurrency=1,
                        pool='solo',
                        loglevel=WORKER_LOGLEVEL,
                        logfile=None,
                        WorkController=TestWorkController,
                        **kwargs):
    setup_app_for_worker(app, loglevel, logfile)
    print('BROKER: %r' % (app.conf.broker_url,))
    assert 'celery.ping' in app.tasks
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
    print('WORKER STARTED')
    _set_task_join_will_block(False)

    yield worker

    print('STOPPING WORKER')
    from celery.worker import state
    state.should_terminate = 0
    print('JOINING WORKER THREAD')
    t.join(10)
    state.should_terminate = None


@contextmanager
def start_worker_process(app,
                         concurrency=1,
                         pool='solo',
                         loglevel=WORKER_LOGLEVEL,
                         logfile=None,
                         **kwargs):
    from celery.apps.multi import Cluster, Node

    app.set_current()
    cluster = Cluster([Node('testworker1@%h')])
    cluster.start()
    yield
    cluster.stopwait()


@contextmanager
def start_worker(app,
                 concurrency=1,
                 pool='solo',
                 loglevel=WORKER_LOGLEVEL,
                 logfile=None,
                 perform_ping_check=True,
                 ping_task_timeout=10.0,
                 **kwargs):
    test_worker_starting.send(sender=app)

    with start_worker_thread(app,
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


def setup_app_for_worker(app, loglevel, logfile):
    app.finalize()
    app.set_current()
    app.set_default()
    type(app.log)._setup = False
    app.log.setup(loglevel=loglevel, logfile=logfile)
