"""chord_unlock fails the chord when it can't retry (#10955).

The database backend joins chords with ``celery.chord_unlock``, which polls
the header and retries itself until the header is ready. When that retry
can't be sent, or the retries run out, nothing polls the chord again, so the
chord must fail instead of leaving its body PENDING.
"""
from __future__ import annotations

from contextlib import contextmanager
from time import sleep

import pytest
from kombu import Queue
from pytest_celery import RabbitMQTestBroker

from celery import Celery, chord, uuid
from celery.contrib.testing.worker import start_worker
from celery.result import allow_join_result

pytestmark = pytest.mark.timeout(180)

HEADER_SECONDS = 4
RESULT_TIMEOUT = 30


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
        result_chord_retry_interval=0.5,
    )

    @app.task(name='chord_unlock_retry.header', shared=False)
    def header(value):
        sleep(HEADER_SECONDS)
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

    try:
        with app.connection_for_write() as connection:
            for queue in queues:
                queue(connection).declare()
        yield app
    finally:
        with app.connection_for_write() as connection:
            for queue in queues:
                queue(connection).delete()
        app.close()


@contextmanager
def _worker(app):
    # Threads, so that both header tasks and chord_unlock run at once.
    with start_worker(app, pool='threads', concurrency=3,
                      queues=[queue.name for queue in app.conf.task_queues],
                      hostname=f'chord-unlock-{uuid()}@test',
                      perform_ping_check=False, shutdown_timeout=30):
        yield


def _apply_chord(app, **options):
    tasks = app.tasks
    return chord(
        [tasks['chord_unlock_retry.header'].s(1), tasks['chord_unlock_retry.header'].s(2)],
        tasks['chord_unlock_retry.body'].s().on_error(tasks['chord_unlock_retry.errback'].s()),
    ).apply_async(**options)


def test_chord_waits_for_its_header(chord_app, tmp_path):
    # chord_unlock retries while the header runs, and that must not fail it.
    app = chord_app
    with _worker(app):
        result = _apply_chord(app)
        with allow_join_result():
            assert result.get(timeout=RESULT_TIMEOUT) == 3
    assert (tmp_path / 'body').read_text().splitlines() == ['[1, 2]']
    assert not (tmp_path / 'errback').exists()
