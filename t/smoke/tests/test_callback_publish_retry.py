from __future__ import annotations

from contextlib import contextmanager
from time import monotonic, sleep

import pytest
from kombu import Producer, Queue
from pytest_celery import CeleryBrokerCluster, RabbitMQTestBroker

from celery import Celery, chain, chord, states, uuid
from celery.contrib.testing.worker import start_worker
from celery.result import allow_join_result
from celery.signals import before_task_publish


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def callback_app(celery_setup_app, tmp_path, acks_late, queue_type):
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
        return value

    @app.task(name='callback_retry.errback', shared=False)
    def errback(task_id):
        with (tmp_path / 'errback').open('a') as stream:
            stream.write(f'{task_id}\n')

    def record_publish(sender, **kwargs):
        if sender == body.name:
            with (tmp_path / 'attempts').open('a') as stream:
                stream.write('publish\n')

    before_task_publish.connect(record_publish, weak=False)
    try:
        with app.connection_for_write() as connection:
            blocked(connection).declare()
            Producer(connection).publish('filler', exchange='', routing_key=blocked.name, serializer='json')
        yield app
    finally:
        before_task_publish.disconnect(record_publish)
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
        result = chain(parent.s(), body).apply_async()
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
                      perform_ping_check=False, shutdown_timeout=10) as worker:
        try:
            yield
        finally:
            app.control.shutdown(destination=[worker.hostname])


@pytest.mark.parametrize('queue_type', ['classic'])
@pytest.mark.parametrize('acks_late', [False, True])
@pytest.mark.parametrize('pool', ['solo', 'threads', 'prefork'])
@pytest.mark.parametrize('kind', ['link', 'chain', 'chord'])
def test_transient_callback_refusal(callback_app, tmp_path, pool, kind):
    app = callback_app
    parent_queue = app.conf.task_default_queue
    blocked = app.amqp.queues[app.conf.task_routes['callback_retry.body']['queue']]
    with _worker(app, pool, parent_queue):
        parent, result = _submit(app, kind)
        # A second publish proves that RabbitMQ refused the first confirmed write.
        _wait_for_lines(tmp_path / 'attempts', 2)
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
    assert not (tmp_path / 'errback').exists()


@pytest.mark.parametrize('queue_type', ['quorum'])
@pytest.mark.parametrize('acks_late', [False, True])
@pytest.mark.parametrize('pool', ['solo', 'threads', 'prefork'])
@pytest.mark.parametrize('kind', ['link', 'chord'])
def test_permanent_callback_refusal(callback_app, tmp_path, pool, kind):
    app = callback_app
    with _worker(app, pool, app.conf.task_default_queue):
        parent, body = _submit(app, kind)
        result = body if kind == 'chord' else parent
        with allow_join_result():
            result.get(timeout=10, propagate=False)
        assert result.state == states.FAILURE
        _wait_for_lines(tmp_path / 'errback', 1, timeout=30)
    assert (tmp_path / 'parent').read_text().splitlines() == ['run']
    assert (tmp_path / 'attempts').read_text().splitlines() == ['publish'] * 3
    assert len((tmp_path / 'errback').read_text().splitlines()) == 1
