"""Integration tests for the worker liveness file (worker_liveness_file)."""
import os
import signal
import subprocess
import sys
import time
from uuid import uuid4

import pytest

from celery import Celery

from .conftest import TEST_BROKER, flaky

INTERVAL = 1.0

APP_MODULE = '''
import time

from celery import Celery

app = Celery('liveapp', broker={broker!r})
app.conf.worker_liveness_interval = {interval!r}


@app.task
def sleep(seconds):
    time.sleep(seconds)
'''

pytestmark = pytest.mark.skipif(
    not hasattr(signal, 'SIGSTOP'), reason='needs SIGSTOP')


def wait_for(predicate, timeout=30, message='condition'):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(f'timed out waiting for {message}')


class LivenessWorker:

    def __init__(self, tmp_path, broker=TEST_BROKER):
        self.path = tmp_path / 'alive'
        self.log = tmp_path / 'worker.log'
        (tmp_path / 'liveapp.py').write_text(
            APP_MODULE.format(broker=broker, interval=INTERVAL))
        self.hostname = f'liveness-{uuid4().hex[:8]}@integration'
        self.tmp_path = tmp_path
        self.process = None

    def start(self):
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(
            [str(self.tmp_path), env.get('PYTHONPATH', '')])
        with open(self.log, 'wb') as log:
            self.process = subprocess.Popen(
                [sys.executable, '-m', 'celery', '-A', 'liveapp', 'worker',
                 '-P', 'prefork', '-c', '1', '-n', self.hostname, '-l', 'INFO',
                 '--without-mingle', '--without-gossip',
                 '--liveness-file', str(self.path)],
                cwd=self.tmp_path, env=env,
                stdout=log, stderr=subprocess.STDOUT)
        return self

    def stop(self):
        if self.process is None or self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()

    def age(self):
        try:
            return time.time() - os.stat(self.path).st_mtime
        except FileNotFoundError:
            return float('inf')

    def wait_until_fresh(self):
        wait_for(lambda: self.age() < 2 * INTERVAL, timeout=60,
                 message=f'a fresh liveness file:\n{self.log.read_text()}')

    def max_age_during(self, seconds):
        worst = 0.0
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            assert self.process.poll() is None, self.log.read_text()
            worst = max(worst, self.age())
            time.sleep(0.1)
        return worst


@pytest.fixture
def liveness_worker(tmp_path):
    workers = []

    def start(**kwargs):
        worker = LivenessWorker(tmp_path, **kwargs).start()
        workers.append(worker)
        return worker

    yield start
    for worker in workers:
        worker.stop()


@flaky
def test_file_stays_fresh_while_the_worker_is_busy(liveness_worker):
    worker = liveness_worker()
    worker.wait_until_fresh()
    app = Celery(broker=TEST_BROKER)
    try:
        # One process and three long tasks: the prefetch window is full.
        for _ in range(3):
            app.send_task('liveapp.sleep', args=[4])
        assert worker.max_age_during(10) < 3 * INTERVAL
    finally:
        app.close()


@flaky
def test_file_goes_stale_while_the_worker_is_frozen(liveness_worker):
    worker = liveness_worker()
    worker.wait_until_fresh()
    os.kill(worker.process.pid, signal.SIGSTOP)
    try:
        time.sleep(4 * INTERVAL)
        assert worker.age() > 3 * INTERVAL
    finally:
        os.kill(worker.process.pid, signal.SIGCONT)
    worker.wait_until_fresh()


@flaky
def test_leftover_file_is_removed_before_connecting(liveness_worker, tmp_path):
    (tmp_path / 'alive').write_text('')
    # Nothing listens on port 1, so the worker keeps retrying to connect.
    broker = ('redis://127.0.0.1:1/0' if TEST_BROKER.startswith('redis')
              else 'pyamqp://guest:guest@127.0.0.1:1//')
    worker = liveness_worker(broker=broker)
    wait_for(lambda: not worker.path.exists(), timeout=60,
             message='the leftover file to be removed')
    time.sleep(2 * INTERVAL)
    assert worker.process.poll() is None, worker.log.read_text()
    assert not worker.path.exists()


@flaky
@pytest.mark.skipif(not TEST_BROKER.startswith('redis'), reason='Redis only')
def test_no_probe_keys_left_on_redis(liveness_worker):
    worker = liveness_worker()
    worker.wait_until_fresh()
    worker.stop()
    app = Celery(broker=TEST_BROKER)
    try:
        with app.connection_for_write() as conn:
            client = conn.default_channel.client
            assert not [key for key in client.scan_iter(match='*celery.liveness*')]
    finally:
        app.close()
