"""Publication budgets start before pool acquisition and remain local options."""
from time import monotonic
from unittest.mock import Mock

import pytest
from kombu import Connection, Producer
from kombu.pools import ProducerPool

from celery import signals
from celery.app import base
from celery.exceptions import ImproperlyConfigured

try:
    from amqp.utils import operation_deadline
    from kombu.exceptions import PublicationTimeout
    from kombu.utils.time import current_publication_deadline, publication_timeout
except ImportError:
    pytest.skip('requires publication deadline support in Kombu and py-amqp',
                allow_module_level=True)


@pytest.fixture
def producer():
    producer = Producer(Connection('amqp://'))
    producer._publish = Mock()
    producer._publish.__name__ = 'publish'
    return producer


@pytest.mark.parametrize('explicit, expected', [(False, .5), (True, .2)])
def test_config_and_per_call_share_transport_deadline(app, producer, explicit, expected):
    app.conf.task_publish_timeout = .5
    observed = []

    def publish(*args, **kwargs):
        observed.append(operation_deadline() - monotonic())
        assert current_publication_deadline().expires == operation_deadline()

    producer._publish.side_effect = publish
    options = {'publication_timeout': .2} if explicit else {}
    app.send_task('deadline.test', producer=producer, ignore_result=True, **options)
    assert len(observed) == 1
    assert 0 < observed[0] <= expected
    assert current_publication_deadline() is None
    assert operation_deadline() is None


def test_explicit_none_disables_config_default(app, producer):
    app.conf.task_publish_timeout = 0
    producer._publish.side_effect = lambda *a, **k: (
        current_publication_deadline() is None or pytest.fail('unexpected deadline'))
    app.send_task('deadline.test', producer=producer, publication_timeout=None,
                  ignore_result=True)
    producer._publish.assert_called_once()


def test_explicit_none_cannot_extend_outer_budget(app, producer):
    app.conf.task_publish_timeout = 100
    with publication_timeout(1) as outer:
        producer._publish.side_effect = lambda *a, **k: (
            current_publication_deadline() is outer or pytest.fail('budget reset'))
        app.send_task('deadline.test', producer=producer, publication_timeout=None,
                      ignore_result=True)
    producer._publish.assert_called_once()


@pytest.mark.parametrize('inner_pool', [False, True])
def test_budget_includes_producer_and_connection_pool_waits(app, inner_pool):
    connections = Connection('amqp://').Pool(limit=1)
    producers = ProducerPool(connections, limit=1)
    app.amqp._producer_pool = producers
    held = connections.acquire() if inner_pool else producers.acquire()
    started = monotonic()
    try:
        with pytest.raises(PublicationTimeout):
            app.send_task('deadline.test', publication_timeout=.04)
        assert .03 <= monotonic() - started < .5
        assert len(producers._dirty) == (0 if inner_pool else 1)
    finally:
        held.release()
        producers.force_close_all()
        connections.force_close_all()


def test_zero_timeout_has_no_pool_side_effect(app, monkeypatch):
    acquire = Mock()
    monkeypatch.setattr(app, 'producer_or_acquire', acquire)
    with pytest.raises(PublicationTimeout):
        app.send_task('deadline.test', publication_timeout=0)
    acquire.assert_not_called()


def test_apply_async_option_is_consumed_and_not_metadata(app, producer):
    @app.task
    def add(x, y):
        return x + y

    sent = []

    def before_publish(**kwargs):
        sent.append(kwargs)

    signals.before_task_publish.connect(before_publish, weak=False)
    try:
        add.apply_async((1, 2), producer=producer, publication_timeout=1)
    finally:
        signals.before_task_publish.disconnect(before_publish)
    assert len(sent) == 1
    assert 'publication_timeout' not in sent[0]
    assert 'publication_timeout' not in sent[0]['properties']
    assert 'publication_timeout' not in sent[0]['headers']
    properties = producer._publish.call_args.args[5]
    assert 'publication_timeout' not in properties


def test_backend_and_task_sent_event_inherit_deadline(app, producer, monkeypatch):
    app.conf.task_send_sent_event = True
    observed = []

    def check(*args, **kwargs):
        assert operation_deadline() is not None
        observed.append(current_publication_deadline().expires)

    monkeypatch.setattr(app.backend, 'on_task_call', check)
    monkeypatch.setattr(app.events.Dispatcher, 'publish', check)
    producer._publish.side_effect = check
    app.send_task('deadline.test', producer=producer, publication_timeout=1)
    assert len(observed) == 3
    assert len(set(observed)) == 1


def test_old_kombu_default_works_opt_in_fails(app, producer, monkeypatch):
    def unavailable(timeout):
        raise ImproperlyConfigured('Kombu publication deadline support required')

    monkeypatch.setattr(base, 'publication_timeout_context', unavailable)
    app.send_task('deadline.test', producer=producer, ignore_result=True)
    producer._publish.assert_called_once()
    with pytest.raises(ImproperlyConfigured, match='Kombu'):
        app.send_task('deadline.test', producer=producer, publication_timeout=1)
    producer._publish.assert_called_once()


def test_eager_execution_does_not_use_publication_budget(app):
    app.conf.task_always_eager = True
    app.conf.task_publish_timeout = 0

    @app.task
    def add(x, y):
        return x + y

    assert add.apply_async((1, 2), publication_timeout=0).get() == 3


def test_explicit_outer_budget_includes_canvas_producer_acquisition(app):
    from celery import group

    @app.task
    def add(x, y):
        return x + y

    connections = Connection('amqp://').Pool(limit=1)
    producers = ProducerPool(connections, limit=1)
    app.amqp._producer_pool = producers
    held = producers.acquire()
    started = monotonic()
    try:
        with pytest.raises(PublicationTimeout), publication_timeout(.04):
            group(add.s(1, 2), app=app).apply_async()
        assert .03 <= monotonic() - started < .5
    finally:
        held.release()
        producers.force_close_all()
        connections.force_close_all()
