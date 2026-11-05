"""chord_unlock fails the chord when it can't retry (#10955).

The database backend joins chords with ``celery.chord_unlock``, which polls
the header and retries itself until the header is ready. When that retry
can't be sent, or the retries run out, nothing polls the chord again, so the
chord must fail instead of leaving its body PENDING.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from time import monotonic, sleep

import pytest
from kombu import Queue
from pytest_celery import RabbitMQTestBroker

from celery import Celery, chord, signals, states, uuid
from celery.contrib.testing.worker import start_worker
from celery.exceptions import ChordError
from celery.result import allow_join_result

pytestmark = pytest.mark.timeout(180)

RESULT_TIMEOUT = 30
RETRY_INTERVAL = 0.5


@pytest.fixture
def chord_app(celery_rabbitmq_broker: RabbitMQTestBroker, tmp_path):
    name = f'chord-unlock-{uuid()}'
    # chord_unlock is sent to the body's queue, and the errback goes to a
    # queue of its own so that refusing the body's queue doesn't refuse it.
    queues = [Queue(f'{name}-{suffix}', exchange='', routing_key=f'{name}-{suffix}')
              for suffix in ('header', 'body', 'errback')]
    header_queue, body_queue, errback_queue = queues
    app = Celery(name, broker=celery_rabbitmq_broker.config()['host_url'],
                 backend=f'db+sqlite:///{tmp_path / "results.db"}',
                 set_as_current=False)
    app.conf.update(
        broker_transport_options={'confirm_publish': True},
        task_queues=queues,
        task_default_queue=header_queue.name,
        task_default_exchange='',
        task_default_routing_key=header_queue.name,
        task_routes={
            'chord_unlock_retry.body': {'queue': body_queue.name},
            'chord_unlock_retry.errback': {'queue': errback_queue.name},
        },
        result_chord_retry_interval=RETRY_INTERVAL,
    )
    gate = tmp_path / 'gate'

    @app.task(name='chord_unlock_retry.header', shared=False)
    def header(value):
        # Wait until the test opens the gate, so the test decides when the
        # header finishes.  A file works whatever the pool.
        deadline = monotonic() + RESULT_TIMEOUT
        while not gate.exists() and monotonic() < deadline:
            sleep(0.05)
        return value

    @app.task(name='chord_unlock_retry.body', shared=False)
    def body(values):
        with (tmp_path / 'body').open('a') as stream:
            stream.write(f'{values}\n')
        return sum(values)

    @app.task(name='chord_unlock_retry.errback', shared=False)
    def errback(task_id):
        with (tmp_path / 'errback').open('a') as stream:
            stream.write(f'{task_id}\n')

    def count_retry(sender=None, **_):
        if sender is not None and sender.name == 'celery.chord_unlock':
            with (tmp_path / 'retries').open('a') as stream:
                stream.write('retry\n')

    signals.task_retry.connect(count_retry, weak=False)
    try:
        with app.connection_for_write() as connection:
            for queue in queues:
                queue(connection).declare()
        yield app
    finally:
        signals.task_retry.disconnect(count_retry)
        gate.touch()
        with app.connection_for_write() as connection:
            for queue in queues:
                queue(connection).delete()
        app.close()


@contextmanager
def _worker(app):
    hostname = f'chord-unlock-{uuid()}@test'
    # Threads, so that both header tasks and chord_unlock run at once.
    with start_worker(app, pool='threads', concurrency=3,
                      queues=[queue.name for queue in app.conf.task_queues],
                      hostname=hostname,
                      perform_ping_check=False, shutdown_timeout=30):
        try:
            yield
        finally:
            # Otherwise the worker's event loop sees the stop only when its
            # poll times out, up to 10 s later.
            app.control.shutdown(destination=[hostname])


def _apply_chord(app, **options):
    tasks = app.tasks
    return chord(
        [tasks['chord_unlock_retry.header'].s(1), tasks['chord_unlock_retry.header'].s(2)],
        tasks['chord_unlock_retry.body'].s().on_error(tasks['chord_unlock_retry.errback'].s()),
    ).apply_async(**options)


def _lines(path):
    return path.read_text().splitlines() if path.exists() else []


def _wait_for(predicate, message, timeout=RESULT_TIMEOUT):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.05)
    pytest.fail(message)


def _assert_chord_failed(result, tmp_path):
    with allow_join_result():
        with pytest.raises(ChordError, match='could not be retried'):
            result.get(timeout=RESULT_TIMEOUT)
    assert result.state == states.FAILURE
    _wait_for(lambda: _lines(tmp_path / 'errback'), 'the errback did not run')
    # Finish the header, and give a stray chord_unlock time to poll again:
    # a failed chord must not run its body afterwards.
    (tmp_path / 'gate').touch()
    with allow_join_result():
        result.parent.join(timeout=RESULT_TIMEOUT)
    sleep(3 * RETRY_INTERVAL)
    assert _lines(tmp_path / 'errback') == [result.id]
    assert _lines(tmp_path / 'body') == []


@contextmanager
def _refuse_publishes(broker, queue):
    # Refuse every new message for the queue, as a full queue would.
    policy = f'refuse-{queue}'
    definition = json.dumps({'max-length': 0, 'overflow': 'reject-publish'})
    exit_code, output = broker.container.exec_run(
        ['rabbitmqctl', 'set_policy', '--apply-to', 'queues',
         policy, f'^{queue}$', definition],
    )
    assert exit_code == 0, output
    try:
        yield
    finally:
        broker.container.exec_run(['rabbitmqctl', 'clear_policy', policy])


def test_chord_waits_for_its_header(chord_app, tmp_path):
    # chord_unlock retries while the header runs, and that must not fail it.
    app = chord_app
    with _worker(app):
        result = _apply_chord(app)
        _wait_for(lambda: len(_lines(tmp_path / 'retries')) >= 2,
                  'chord_unlock did not retry')
        (tmp_path / 'gate').touch()
        with allow_join_result():
            assert result.get(timeout=RESULT_TIMEOUT) == 3
    assert (tmp_path / 'body').read_text().splitlines() == ['[1, 2]']
    assert not (tmp_path / 'errback').exists()


def test_chord_fails_when_chord_unlock_runs_out_of_retries(chord_app, tmp_path):
    app = chord_app
    with _worker(app):
        # The header waits for the gate, so both retries find it unfinished.
        result = _apply_chord(app, max_retries=2)
        _assert_chord_failed(result, tmp_path)


def test_chord_fails_when_chord_unlock_retry_is_refused(
        chord_app, celery_rabbitmq_broker, tmp_path):
    app = chord_app
    body_queue = app.conf.task_routes['chord_unlock_retry.body']['queue']
    with _worker(app):
        result = _apply_chord(app)
        # The first chord_unlock must be delivered before the queue refuses
        # messages, so that only its retry is refused.
        with app.connection_for_write() as connection:
            queue = app.amqp.queues[body_queue](connection)
            _wait_for(lambda: queue.queue_declare(passive=True).message_count == 0,
                      'the worker did not receive chord_unlock')
        # The header waits for the gate, so the policy can take as long as
        # it needs: no retry finds the header done first.
        with _refuse_publishes(celery_rabbitmq_broker, body_queue):
            _assert_chord_failed(result, tmp_path)
