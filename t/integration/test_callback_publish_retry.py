"""A real worker retries a callback publish the broker refused.

The callback queue is a RabbitMQ queue that already holds its one allowed
message and rejects anything further (``x-overflow: reject-publish``).
Publisher confirms surface that refusal to the worker.
"""
from contextlib import contextmanager, suppress
from time import monotonic, sleep
from urllib.parse import urlparse

import pytest
from kombu import Connection, Producer, Queue

from celery import Celery, chain, chord, states, uuid
from celery.backends.base import Backend
from celery.contrib.testing.worker import start_worker
from celery.result import allow_join_result
from celery.signals import before_task_publish

from .conftest import TEST_BACKEND, TEST_BROKER

_AMQP_SCHEMES = {'amqp', 'amqps', 'pyamqp', 'pyamqps'}


def _scheme(url):
    parsed = urlparse(url)
    return parsed.scheme or url


pytestmark = pytest.mark.skipif(
    _scheme(TEST_BROKER) not in _AMQP_SCHEMES,
    reason='refused callback publish needs a RabbitMQ broker',
)


def _reachable(url):
    scheme = _scheme(url)
    try:
        if scheme == 'redis' or scheme.startswith('redis+'):
            from redis import Redis
            Redis.from_url(url, socket_connect_timeout=3).ping()
            return True
        if scheme in _AMQP_SCHEMES or scheme == 'rpc':
            with Connection(TEST_BROKER if scheme == 'rpc' else url, connect_timeout=3) as conn:
                conn.ensure_connection(max_retries=0)
            return True
    except Exception:
        return False
    # Other backends have no local probe; let the test run.
    return True


@pytest.fixture
def callback_app(tmp_path, acks_late):
    if not _reachable(TEST_BROKER):
        pytest.skip(f'broker is not reachable: {TEST_BROKER}')
    if _scheme(TEST_BACKEND) == 'disabled' or not _reachable(TEST_BACKEND):
        pytest.skip(f'result backend is not reachable: {TEST_BACKEND}')

    name = f'callback-retry-{uuid()}'
    parent_queue = Queue(name, exchange='', routing_key=name)
    blocked = Queue(
        f'{name}-full', exchange='', routing_key=f'{name}-full',
        queue_arguments={'x-max-length': 1, 'x-overflow': 'reject-publish'},
    )
    app = Celery(
        name, broker=TEST_BROKER, backend=TEST_BACKEND, set_as_current=False,
    )
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
        try:
            with app.connection_for_write() as connection:
                blocked(connection).declare()
                Producer(connection).publish(
                    'filler', exchange='', routing_key=blocked.name, serializer='json',
                )
        except Exception as exc:
            pytest.skip(f'broker cannot refuse a publish: {exc}')
        yield app
    finally:
        before_task_publish.disconnect(record_publish)
        with suppress(Exception):
            with app.connection_for_write() as connection:
                for queue in (parent_queue, blocked):
                    queue(connection).delete()
        app.close()


def _skip_unless_header_publishes_chord_body(app, kind):
    if kind != 'chord':
        return
    try:
        app.backend.ensure_chords_allowed()
    except NotImplementedError as exc:
        pytest.skip(str(exc).splitlines()[0])
    # chord_unlock is routed to the body queue, which is the queue we filled.
    publish = getattr(app.backend.apply_chord, '__func__', app.backend.apply_chord)
    if publish is Backend.apply_chord:
        pytest.skip('this backend publishes chord_unlock to the refused queue')


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


def _wait_for_lines(path, count, timeout=30):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if path.exists() and len(path.read_text().splitlines()) >= count:
            return
        sleep(0.01)
    seen = path.read_text() if path.exists() else ''
    pytest.fail(f'{path.name} did not reach {count} entries: {seen!r}')


@contextmanager
def _worker(app, queue):
    with start_worker(
        app, pool='solo', concurrency=1, queues=[queue],
        perform_ping_check=False, shutdown_timeout=30,
    ) as worker:
        try:
            yield
        finally:
            app.control.shutdown(destination=[worker.hostname], timeout=1)


def _expected(kind):
    return [4] if kind == 'chord' else 4


@pytest.mark.timeout(180)
@pytest.mark.parametrize('acks_late', [False, True])
@pytest.mark.parametrize('kind', ['link', 'chain', 'chord'])
def test_transient_callback_refusal(callback_app, tmp_path, kind):
    app = callback_app
    _skip_unless_header_publishes_chord_body(app, kind)
    blocked = app.amqp.queues[app.conf.task_routes['callback_retry.body']['queue']]
    with _worker(app, app.conf.task_default_queue):
        parent, result = _submit(app, kind)
        # A second publish shows the broker refused the first confirmed write.
        _wait_for_lines(tmp_path / 'attempts', 2)
        with app.connection_for_write() as connection:
            filler = blocked(connection).get(no_ack=False)
            assert filler.payload == 'filler'
            filler.ack()
        with allow_join_result():
            assert parent.get(timeout=30) == 4
        # The errback is consumed by this worker. Give it a chance to run.
        sleep(1)
        assert not (tmp_path / 'errback').exists()

    with _worker(app, blocked.name):
        with allow_join_result():
            assert result.get(timeout=30) == _expected(kind)
            assert result.state == states.SUCCESS
    assert (tmp_path / 'parent').read_text().splitlines() == ['run']
    assert not (tmp_path / 'errback').exists()


@pytest.mark.timeout(180)
@pytest.mark.parametrize('acks_late', [False, True])
@pytest.mark.parametrize('kind', ['link', 'chain', 'chord'])
def test_callback_refused_briefly_is_published_once_accepted(callback_app, tmp_path, kind):
    app = callback_app
    _skip_unless_header_publishes_chord_body(app, kind)
    app.conf.task_publish_retry_policy = {
        'max_retries': 2, 'interval_start': 1, 'interval_step': 0, 'interval_max': 1,
    }
    blocked = app.amqp.queues[app.conf.task_routes['callback_retry.body']['queue']]
    with _worker(app, app.conf.task_default_queue):
        parent, result = _submit(app, kind)
        _wait_for_lines(tmp_path / 'parent', 1)
        sleep(0.5)
        with app.connection_for_write() as connection:
            filler = blocked(connection).get(no_ack=False)
            assert filler.payload == 'filler'
            filler.ack()
        with allow_join_result():
            assert parent.get(timeout=30) == 4
    with _worker(app, blocked.name):
        with allow_join_result():
            assert result.get(timeout=30) == _expected(kind)
    assert (tmp_path / 'parent').read_text().splitlines() == ['run']
    assert not (tmp_path / 'errback').exists()


@pytest.mark.timeout(180)
@pytest.mark.parametrize('acks_late', [False, True])
@pytest.mark.parametrize('kind', ['link', 'chain', 'chord'])
def test_exhausted_callback_refusal(callback_app, tmp_path, kind):
    app = callback_app
    _skip_unless_header_publishes_chord_body(app, kind)
    with _worker(app, app.conf.task_default_queue):
        parent, result = _submit(app, kind)
        # The chord result is the body. A link or chain fails the parent task,
        # and a chain failure is also stored on the following step.
        failed = result if kind == 'chord' else parent
        with allow_join_result():
            failed.get(timeout=30, propagate=False)
            if kind == 'chain':
                result.get(timeout=30, propagate=False)
        assert failed.state == states.FAILURE
        if kind == 'chain':
            assert result.state == states.FAILURE
        _wait_for_lines(tmp_path / 'errback', 1)
    assert (tmp_path / 'parent').read_text().splitlines() == ['run']
    assert (tmp_path / 'attempts').read_text().splitlines() == ['publish'] * 3
    assert len((tmp_path / 'errback').read_text().splitlines()) == 1
