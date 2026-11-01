from unittest.mock import Mock, patch

import pytest
from kombu.exceptions import EncodeError, OperationalError, TimeoutError

from celery import signals, uuid
from celery.app.amqp import _callback_publish_retry


def _callback_message(app):
    return app.amqp.as_task_v2(uuid(), 'foo', create_sent_event=False)


def _producer(side_effect=None):
    producer = Mock(name='producer')
    producer.connection.recoverable_connection_errors = ()
    if side_effect is not None:
        producer.publish.side_effect = side_effect
    return producer


def _publish_in_callback_context(app, publish):
    return app.amqp._retry_publish(publish)


@pytest.mark.parametrize('error', [OperationalError('refused'), TimeoutError('slow')])
@pytest.mark.parametrize('retry_errors', [(TimeoutError,), [TimeoutError]])
def test_callback_publish_honours_retry_errors(app, error, retry_errors):
    app.conf.task_publish_retry_policy = {
        'max_retries': 1, 'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
        'retry_errors': retry_errors,
    }
    message = _callback_message(app)
    producer = _producer([error, None])

    def publish():
        app.amqp.send_task_message(producer, 'foo', message)

    _publish_in_callback_context(app, publish)
    assert producer.publish.call_count == 2


@pytest.mark.parametrize('error', [EncodeError('bad payload'), ValueError('bad callback')])
def test_callback_publish_does_not_retry_permanent_errors(app, error):
    message = _callback_message(app)
    producer = _producer(error)

    def publish():
        app.amqp.send_task_message(producer, 'foo', message)

    with pytest.raises(type(error)):
        _publish_in_callback_context(app, publish)
    producer.publish.assert_called_once()


def test_callback_publish_can_bypass_retry(app):
    app.conf.task_publish_retry = False
    message = _callback_message(app)
    producer = _producer(OperationalError('refused'))

    def publish():
        app.amqp.send_task_message(producer, 'foo', message)

    with pytest.raises(OperationalError):
        _publish_in_callback_context(app, publish)
    producer.publish.assert_called_once()


def test_callback_publish_skipped_when_always_eager(app):
    app.conf.task_always_eager = True
    publish = Mock(side_effect=_callback_publish_retry.get)
    assert app.amqp._retry_publish(publish) is False
    publish.assert_called_once_with()
    assert _callback_publish_retry.get() is False


def test_callback_publish_retries_each_message(app):
    app.conf.task_publish_retry_policy = {
        'max_retries': 2, 'interval_start': 1, 'interval_step': 1, 'interval_max': 2,
    }
    messages = [_callback_message(app) for _ in range(2)]
    producers = [
        _producer(None),
        _producer([OperationalError('refused'), OperationalError('refused'), None]),
    ]

    def publish():
        app.amqp.send_task_message(producers[0], 'foo', messages[0], priority=3)
        app.amqp.send_task_message(producers[1], 'foo', messages[1], priority=3)

    with patch('kombu.utils.functional.sleep') as sleep:
        _publish_in_callback_context(app, publish)
    assert producers[0].publish.call_count == 1
    assert producers[1].publish.call_count == 3
    assert sum(call.args[0] for call in sleep.call_args_list) == 3


def test_callback_publish_does_not_stack_with_connection_retry(app):
    app.conf.task_publish_retry_policy = {
        'max_retries': 1, 'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
    }
    message = _callback_message(app)
    seen = []

    def publish_side_effect(*args, **kwargs):
        seen.append(kwargs.get('retry'))
        if len(seen) < 2:
            raise OperationalError('refused')

    producer = _producer(publish_side_effect)

    def publish():
        app.amqp.send_task_message(producer, 'foo', message)

    _publish_in_callback_context(app, publish)
    assert seen == [False, False]

    producer = _producer(None)
    app.amqp.send_task_message(producer, 'foo', message)
    assert producer.publish.call_args[1]['retry'] is True


def test_callback_publish_stops_at_the_retry_limit(app):
    app.conf.task_publish_retry_policy = {
        'max_retries': 2, 'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
    }
    message = _callback_message(app)
    error = OperationalError('refused')
    producer = _producer(error)

    def publish():
        app.amqp.send_task_message(producer, 'foo', message)

    with pytest.raises(OperationalError) as raised:
        _publish_in_callback_context(app, publish)
    assert raised.value is error
    assert producer.publish.call_count == 3


def test_callback_publish_honours_signature_retry_false(app):
    message = _callback_message(app)
    producer = _producer(OperationalError('refused'))

    def publish():
        app.amqp.send_task_message(producer, 'foo', message, retry=False)

    with pytest.raises(OperationalError):
        _publish_in_callback_context(app, publish)
    producer.publish.assert_called_once()


def test_callback_publish_honours_signature_retry_policy(app):
    message = _callback_message(app)
    producer = _producer([OperationalError('refused')] * 6 + [None])

    def publish():
        app.amqp.send_task_message(
            producer, 'foo', message,
            retry_policy={'max_retries': 6, 'interval_start': 0,
                          'interval_step': 0, 'interval_max': 0},
        )

    _publish_in_callback_context(app, publish)
    assert producer.publish.call_count == 7


def test_callback_publish_caps_unbounded_max_retries(app, caplog):
    app.conf.task_publish_retry_policy = {
        'max_retries': None, 'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
    }
    message = _callback_message(app)
    producer = _producer([OperationalError('refused')] * 4)

    def publish():
        app.amqp.send_task_message(producer, 'foo', message)

    with pytest.raises(OperationalError):
        _publish_in_callback_context(app, publish)
    assert producer.publish.call_count == 4
    assert 'Unbounded max_retries for worker callback' in caplog.text


@pytest.mark.parametrize('default_limit, expected_attempts', [(2, 3), (None, 4)])
def test_callback_publish_caps_signature_unbounded_retries(app, default_limit, expected_attempts):
    app.conf.task_publish_retry_policy = {
        'max_retries': default_limit, 'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
    }
    producer = _producer(OperationalError('refused'))
    message = _callback_message(app)
    with pytest.raises(OperationalError):
        app.amqp._retry_publish(
            app.amqp.send_task_message, producer, 'foo', message,
            retry_policy={'max_retries': None},
        )
    assert producer.publish.call_count == expected_attempts


def test_callback_retries_preserve_message_and_emit_publish_signals_once(app):
    app.conf.task_publish_retry_policy = {
        'max_retries': 1, 'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
    }
    message = _callback_message(app)
    producer = _producer([OperationalError('refused'), None])
    before, after = Mock(), Mock()
    signals.before_task_publish.connect(before)
    signals.after_task_publish.connect(after)
    try:
        app.amqp._retry_publish(app.amqp.send_task_message, producer, 'foo', message)
    finally:
        signals.before_task_publish.disconnect(before)
        signals.after_task_publish.disconnect(after)
    before.assert_called_once()
    after.assert_called_once()
    assert producer.publish.call_count == 2
    first, second = producer.publish.call_args_list
    assert first == second
    assert first.kwargs['headers']['id'] == message.headers['id']


@pytest.mark.parametrize('signature_policy', [False, True])
@pytest.mark.parametrize('refused', [False, True])
def test_unbounded_policy_warns_only_on_first_refusal(app, caplog, signature_policy, refused):
    app.conf.task_publish_retry_policy = {
        'max_retries': 2 if signature_policy else None,
        'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
    }
    producer = _producer([OperationalError('refused')] * 2 + [None] if refused else None)
    policy = {'max_retries': None} if signature_policy else None
    app.amqp._retry_publish(
        app.amqp.send_task_message, producer, 'foo', _callback_message(app), retry_policy=policy,
    )
    warnings = [record.getMessage() for record in caplog.records if 'Unbounded max_retries' in record.getMessage()]
    assert len(warnings) == int(refused)
    if refused:
        assert 'task_publish_retry_policy' not in warnings[0]
    else:
        assert not caplog.records


@pytest.mark.parametrize('signature_policy', [False, True])
def test_bounded_policy_does_not_warn_about_unbounded_retries(app, caplog, signature_policy):
    app.conf.task_publish_retry_policy = {
        'max_retries': None if signature_policy else 2,
        'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
    }
    producer = _producer([OperationalError('refused'), None])
    policy = {'max_retries': 2} if signature_policy else None
    app.amqp._retry_publish(
        app.amqp.send_task_message, producer, 'foo', _callback_message(app), retry_policy=policy,
    )
    assert producer.publish.call_count == 2
    assert 'Unbounded max_retries' not in caplog.text


def test_callback_publish_revives_after_transport_connection_error(app):
    app.conf.task_publish_retry_policy = {
        'max_retries': 1, 'interval_start': 0, 'interval_step': 0, 'interval_max': 0,
    }
    producer = _producer([ConnectionResetError('connection lost'), None])
    connection = producer.connection
    connection.recoverable_connection_errors = (ConnectionResetError,)
    app.amqp._retry_publish(app.amqp.send_task_message, producer, 'foo', _callback_message(app))
    assert producer.publish.call_count == 2
    producer.revive.assert_called_once_with(connection)
    assert _callback_publish_retry.get() is False
