import threading

import pytest

from celery import states, uuid
from t.integration.conftest import get_active_redis_channels
from t.integration.tasks import identity


@pytest.mark.celery(
    result_serializer='json',
    accept_content=['json'],
    result_compression='gzip',
)
def test_decode_responses_disables_compression(app):
    url = app.conf.result_backend
    if not url.startswith('redis'):
        pytest.skip('Requires redis result backend.')

    separator = '&' if '?' in url else '?'
    app.conf.result_backend = f'{url}{separator}decode_responses=true'
    backend = app.backend
    task_id = uuid()
    result = {'answer': 42, 'text': 'hello world'}

    try:
        backend.store_result(task_id, result, states.SUCCESS)

        assert app.AsyncResult(task_id).get(timeout=5) == result
        assert isinstance(backend.client.get(backend.get_key_for_task(task_id)), str)
        # With decode_responses enabled, the backend ignores the configured
        # compression without changing the app configuration.
        assert backend.compression is None
        assert app.conf.result_compression == 'gzip'
    finally:
        backend.forget(task_id)


@pytest.mark.celery(
    result_serializer='json',
    accept_content=['json'],
)
def test_concurrent_result_gets_on_shared_pubsub(app, celery_session_worker):
    # Smoke test for the ResultConsumer pubsub lock (#4670 follow-up).
    # Every AsyncResult.get() subscribes, polls and unsubscribes on the
    # same shared redis-py pubsub object from its own thread.  Before the
    # consumer serialized these calls, concurrent gets could wedge the
    # connection (ConcurrentObjectUseError under gevent, protocol desync
    # with plain threads).
    url = app.conf.result_backend
    if not url.startswith('redis'):
        pytest.skip('Requires redis result backend.')

    results = [identity.delay(i) for i in range(12)]
    gathered = {}
    failures = []

    def collect(res):
        try:
            gathered[res.id] = res.get(timeout=30)
        except Exception as exc:
            failures.append(exc)

    threads = [threading.Thread(target=collect, args=(res,))
               for res in results]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not failures
    assert gathered == {res.id: i for i, res in enumerate(results)}


@pytest.mark.celery(
    result_serializer='json',
    accept_content=['json'],
)
def test_pubsub_subscribe_churn_while_draining(app):
    # Hammer subscribe/unsubscribe from several threads while a drainer
    # thread polls, driving the ResultConsumer directly against the real
    # server.  This exercises the socket-level interleaving that the unit
    # tests' fake pubsub cannot reproduce.
    url = app.conf.result_backend
    if not url.startswith('redis'):
        pytest.skip('Requires redis result backend.')

    consumer = app.backend.result_consumer
    initial = uuid()
    consumer.start(initial)

    errors = []
    stop = threading.Event()

    def drain():
        while not stop.is_set():
            try:
                consumer.drain_events(timeout=0.05)
            except Exception as exc:
                errors.append(exc)
                stop.set()

    def churn(n):
        for _ in range(25):
            task_id = uuid()
            consumer.consume_from(task_id)
            consumer.cancel_for(task_id)

    drainer = threading.Thread(target=drain)
    drainer.start()
    workers = [threading.Thread(target=churn, args=(n,)) for n in range(4)]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join()
    stop.set()
    drainer.join()

    try:
        assert not errors
        # only the initial subscription survives the churn
        assert consumer.subscribed_to == {
            consumer._get_key_for_task(initial)}
    finally:
        consumer.cancel_for(initial)
        consumer.stop()

    # the server forgets every channel we subscribed to
    meta_channels = [
        channel for channel in get_active_redis_channels()
        if channel.startswith(b'celery-task-meta-')]
    assert meta_channels == []


@pytest.mark.celery(
    result_serializer='json',
    accept_content=['json'],
)
def test_pubsub_reentrant_cancel_from_finalizer(app):
    # The garbage collector can run AsyncResult.__del__ -> cancel_for while
    # this thread is still inside a SUBSCRIBE or UNSUBSCRIBE on the shared
    # pubsub connection.  The nested command must wait until the outer one
    # has returned, and every channel must still be released on the server.
    url = app.conf.result_backend
    if not url.startswith('redis'):
        pytest.skip('Requires redis result backend.')

    consumer = app.backend.result_consumer
    initial = uuid()
    consumer.start(initial)
    pubsub = consumer._pubsub
    finalized = [uuid() for _ in range(6)]
    for task_id in finalized:
        consumer.consume_from(task_id)

    in_flight = []
    nested = []
    pending = list(finalized)

    def reentrant(command):
        def wrapper(*args):
            if in_flight:
                nested.append((command, args))
            in_flight.append(command)
            try:
                result = command(*args)
                # "finalize" another result while the command is in flight
                if pending:
                    consumer.cancel_for(pending.pop())
                return result
            finally:
                in_flight.pop()
        return wrapper

    pubsub.subscribe = reentrant(pubsub.subscribe)
    pubsub.unsubscribe = reentrant(pubsub.unsubscribe)

    errors = []

    def run():
        try:
            outer = uuid()
            consumer.consume_from(outer)
            consumer.cancel_for(outer)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=30)

    try:
        assert not thread.is_alive(), 'pub/sub connection deadlocked'
        assert not errors
        assert not nested
        assert not pending
        assert consumer.subscribed_to == {
            consumer._get_key_for_task(initial)}
    finally:
        del pubsub.subscribe, pubsub.unsubscribe
        consumer.cancel_for(initial)
        consumer.stop()

    meta_channels = [
        channel for channel in get_active_redis_channels()
        if channel.startswith(b'celery-task-meta-')]
    assert meta_channels == []
