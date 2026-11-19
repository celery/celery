from __future__ import annotations

from contextlib import contextmanager
from time import monotonic, sleep

import pytest
from amqp import Channel
from amqp.exceptions import MessageNacked
from kombu import Producer, Queue
from pytest_celery import CeleryBackendCluster, CeleryBrokerCluster, RabbitMQTestBroker, RedisTestBackend

from celery import Celery, chain, chord, states, uuid
from celery.contrib.testing.worker import start_worker
from celery.result import allow_join_result

pytestmark = pytest.mark.timeout(180)


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster(celery_redis_backend: RedisTestBackend) -> CeleryBackendCluster:
    cluster = CeleryBackendCluster(celery_redis_backend)
    yield cluster
    cluster.teardown()


@pytest.fixture
def callback_app(celery_setup_app, tmp_path, acks_late, queue_type, monkeypatch):
    name = f'callback-retry-{uuid()}'
    parent_queue = Queue(name, exchange='', routing_key=name,
                         queue_arguments={'x-queue-type': queue_type})
    blocked = Queue(f'{name}-full', exchange='', routing_key=f'{name}-full',
                    queue_arguments={'x-max-length': 1, 'x-overflow': 'reject-publish'})
    app = Celery(name, broker=celery_setup_app.conf.broker_url,
                 backend=celery_setup_app.conf.result_backend, set_as_current=False)
    app.conf.update(
        broker_transport_options={'confirm_publish': True},
        task_queues=(parent_queue, blocked),
        task_default_queue=name,
        task_default_exchange='',
        task_default_routing_key=name,
        task_routes={'callback_retry.body': {'queue': blocked.name}},
        task_publish_retry_policy={
            'max_retries': 2, 'interval_start': 0.2,
            'interval_step': 0, 'interval_max': 0.2,
        },
        worker_prefetch_multiplier=1,
        worker_deduplicate_successful_tasks=True,
    )

    @app.task(name='callback_retry.parent', shared=False, acks_late=acks_late)
    def parent():
        with (tmp_path / 'parent').open('a') as stream:
            stream.write('run\n')
        return 4

    @app.task(name='callback_retry.body', shared=False)
    def body(value):
        with (tmp_path / 'body').open('a') as stream:
            stream.write(f'{value}\n')
        return value

    @app.task(name='callback_retry.errback', shared=False)
    def errback(task_id):
        with (tmp_path / 'errback').open('a') as stream:
            stream.write(f'{task_id}\n')

    publish = Channel.basic_publish_confirm

    def record_refusal(channel, *args, **kwargs):
        try:
            return publish(channel, *args, **kwargs)
        except MessageNacked:
            if kwargs.get('routing_key') == blocked.name:
                with (tmp_path / 'refusals').open('a') as stream:
                    stream.write('refused\n')
            raise

    # Observe real publisher confirms, including in prefork children. The
    # publish signals describe a logical message, not each transport attempt.
    monkeypatch.setattr(Channel, 'basic_publish_confirm', record_refusal)
    try:
        with app.connection_for_write() as connection:
            blocked(connection).declare()
            Producer(connection).publish('filler', exchange='', routing_key=blocked.name, serializer='json')
        yield app
    finally:
        with app.connection_for_write() as connection:
            for queue in (parent_queue, blocked):
                queue(connection).delete()
        app.close()


def _submit(app, kind):
    parent = app.tasks['callback_retry.parent']
    body = app.tasks['callback_retry.body'].s()
    errback = app.tasks['callback_retry.errback'].s()
    if kind == 'chord':
        result = chord([parent.s()], body.on_error(errback)).apply_async()
        return result.parent.results[0], result
    if kind == 'chain':
        result = chain(parent.s(), body).on_error(errback).apply_async()
        return result.parent, result
    body_result = body.freeze()
    return parent.apply_async(link=body, link_error=errback), body_result


def _wait_for_lines(path, count, timeout=10):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if path.exists() and len(path.read_text().splitlines()) >= count:
            return
        sleep(0.01)
    pytest.fail(f'{path.name} did not reach {count} entries')


@contextmanager
def _worker(app, pool, queue):
    with start_worker(app, pool=pool, concurrency=1, queues=[queue],
                      hostname=f'callback-{uuid()}@test',
                      perform_ping_check=False, shutdown_timeout=30):
        yield


@pytest.mark.parametrize('queue_type', ['classic'])
@pytest.mark.parametrize('acks_late', [False, True])
@pytest.mark.parametrize('pool', ['solo', 'threads', 'prefork'])
@pytest.mark.parametrize('kind', ['link', 'chain', 'chord'])
def test_transient_callback_refusal(callback_app, tmp_path, pool, kind):
    app = callback_app
    app.conf.task_publish_retry_policy = {
        'max_retries': 2, 'interval_start': 1, 'interval_step': 0, 'interval_max': 1,
    }
    parent_queue = app.conf.task_default_queue
    blocked = app.amqp.queues[app.conf.task_routes['callback_retry.body']['queue']]
    with _worker(app, pool, parent_queue):
        parent, result = _submit(app, kind)
        _wait_for_lines(tmp_path / 'refusals', 1)
        # Keep the queue full briefly: immediate retries would all fail.
        sleep(0.5)
        with app.connection_for_write() as connection:
            filler = blocked(connection).get(no_ack=False)
            assert filler.payload == 'filler'
            filler.ack()
        with allow_join_result():
            assert parent.get(timeout=10) == 4

    with _worker(app, pool, blocked.name):
        with allow_join_result():
            assert result.get(timeout=10) == ([4] if kind == 'chord' else 4)
    assert (tmp_path / 'parent').read_text().splitlines() == ['run']
    assert (tmp_path / 'body').read_text().splitlines() == [str([4] if kind == 'chord' else 4)]
    assert not (tmp_path / 'errback').exists()


@pytest.mark.parametrize('queue_type', ['quorum'])
@pytest.mark.parametrize('acks_late', [False, True])
@pytest.mark.parametrize('pool', ['solo', 'threads', 'prefork'])
@pytest.mark.parametrize('kind', ['link', 'chain', 'chord'])
def test_permanent_callback_refusal(callback_app, tmp_path, pool, kind):
    app = callback_app
    with _worker(app, pool, app.conf.task_default_queue):
        parent, body = _submit(app, kind)
        result = body if kind == 'chord' else parent
        with allow_join_result():
            result.get(timeout=10, propagate=False)
            if kind == 'chain':
                body.get(timeout=10, propagate=False)
        assert result.state == states.FAILURE
        if kind == 'chain':
            assert body.state == states.FAILURE
        _wait_for_lines(tmp_path / 'errback', 1, timeout=30)
    assert (tmp_path / 'parent').read_text().splitlines() == ['run']
    assert (tmp_path / 'refusals').read_text().splitlines() == ['refused'] * 3
    assert len((tmp_path / 'errback').read_text().splitlines()) == 1
    assert not (tmp_path / 'body').exists()


@pytest.mark.parametrize('queue_type', ['classic'])
@pytest.mark.parametrize('acks_late', [False, True])
@pytest.mark.parametrize('pool', ['solo', 'threads', 'prefork'])
def test_refused_link_does_not_republish_accepted_link(callback_app, tmp_path, pool):
    app = callback_app
    parent = app.tasks['callback_retry.parent']
    body = app.tasks['callback_retry.body']
    accepted = body.s().set(queue=app.conf.task_default_queue)
    refused = body.s()
    errback = app.tasks['callback_retry.errback'].s()
    with _worker(app, pool, app.conf.task_default_queue):
        result = parent.apply_async(link=[accepted, refused], link_error=errback)
        with allow_join_result():
            result.get(timeout=10, propagate=False)
        assert result.state == states.FAILURE
        _wait_for_lines(tmp_path / 'errback', 1, timeout=30)
        # A final task on the same FIFO queue ensures all accepted links ran.
        with allow_join_result():
            assert body.apply_async(('drained',), queue=app.conf.task_default_queue).get(timeout=10) == 'drained'
    assert (tmp_path / 'body').read_text().splitlines() == ['4', 'drained']
    assert (tmp_path / 'parent').read_text().splitlines() == ['run']
    assert (tmp_path / 'refusals').read_text().splitlines() == ['refused'] * 3
    assert len((tmp_path / 'errback').read_text().splitlines()) == 1
