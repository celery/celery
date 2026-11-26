"""The Redis result backend on a Redis Cluster.

These run when the backend's client is a :class:`redis.RedisCluster`, i.e.
with ``TEST_BACKEND_CLUSTER=1`` and ``TEST_BACKEND`` pointing at a cluster
node (the ``redis_cluster`` tox environment), and are skipped otherwise.
"""
import time

import pytest

from celery import chord, states, uuid
from t.integration.tasks import identity

redis = pytest.importorskip('redis')
from redis.crc import key_slot  # noqa: E402
from redis.exceptions import RedisClusterException  # noqa: E402

TIMEOUT = 30
GROUP_KEY_SUFFIXES = ('', '.j', '.t', '.s', '.d', '.c')


@pytest.fixture
def backend(app):
    backend = app.backend
    if not isinstance(getattr(backend, 'client', None), redis.RedisCluster):
        pytest.skip('Requires the Redis result backend on a Redis Cluster '
                    '(TEST_BACKEND_CLUSTER=1).')
    return backend


def task_ids_on_distinct_slots(backend, count):
    """Task ids whose result keys hash to ``count`` different slots."""
    ids, slots = [], set()
    while len(ids) < count:
        task_id = uuid()
        slot = key_slot(backend.get_key_for_task(task_id))
        if slot not in slots:
            slots.add(slot)
            ids.append(task_id)
    return ids


def wait_for_message(pubsub, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = pubsub.get_message(timeout=1.0)
        if message and message['type'] == 'message':
            return message
    raise AssertionError('no message was published')


def test_client_is_a_cluster_client(backend):
    assert isinstance(backend.client, redis.RedisCluster)
    assert not hasattr(backend.client, 'connection_pool')
    assert backend.client.cluster_info()['cluster_state'] == 'ok'


def test_chord_keys_for_one_group_share_a_cluster_slot(backend):
    gid = uuid()
    keys = [backend.get_key_for_group(gid, suffix)
            for suffix in GROUP_KEY_SUFFIXES]
    assert len({key_slot(key) for key in keys}) == 1
    assert keys[0].endswith(b'{' + gid.encode() + b'}')
    assert keys[1].endswith(b'{' + gid.encode() + b'}.j')
    # task keys are left alone: they are supposed to spread over the slots
    assert backend.get_key_for_task(gid).endswith(gid.encode())


def test_backend_mget_reads_keys_on_distinct_cluster_slots(backend):
    ids = task_ids_on_distinct_slots(backend, 3)
    keys = [backend.get_key_for_task(task_id) for task_id in ids]
    try:
        for task_id in ids:
            backend.store_result(task_id, {'task_id': task_id}, states.SUCCESS)

        # one MGET cannot span slots, so the client refuses it
        with pytest.raises(RedisClusterException):
            backend.client.mget(keys)

        metas = [backend.decode_result(value) for value in backend.mget(keys)]
        assert [meta['result'] for meta in metas] == [
            {'task_id': task_id} for task_id in ids]
        assert dict(backend.get_many(ids, timeout=TIMEOUT)) == {
            task_id: meta for task_id, meta in zip(ids, metas)}
    finally:
        for task_id in ids:
            backend.forget(task_id)


def test_store_result_round_trips_through_async_result(app, backend):
    task_id = uuid()
    key = backend.get_key_for_task(task_id)
    pubsub = backend.client.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(key)
    pubsub.get_message(timeout=TIMEOUT)  # the subscribe confirmation
    try:
        backend.store_result(task_id, {'answer': 42}, states.SUCCESS)

        result = app.AsyncResult(task_id)
        assert result.state == states.SUCCESS
        assert result.get(timeout=TIMEOUT) == {'answer': 42}
        assert 0 < backend.client.ttl(key) <= backend.expires
        # SET and PUBLISH cannot be pipelined on a cluster, but result
        # consumers still have to be woken up.
        message = wait_for_message(pubsub)
        assert message['channel'] == key
        assert backend.decode_result(message['data'])['status'] == states.SUCCESS
    finally:
        pubsub.close()
        backend.forget(task_id)


def test_result_consumer_reconnect_pubsub_handles_multi_slot_keys(backend):
    consumer = backend.result_consumer
    ids = task_ids_on_distinct_slots(backend, 3)
    running = ids[1]
    consumer.start(ids[0])
    for task_id in ids[1:]:
        consumer.consume_from(task_id)
    seen = []
    original = consumer.on_state_change

    def capture(meta, message):
        seen.append(meta['task_id'])
        return original(meta, message)

    try:
        backend.store_result(ids[0], 1, states.SUCCESS)
        backend.store_result(running, None, states.STARTED)
        backend.store_result(ids[2], 3, states.SUCCESS)

        # the connection was lost, and with it every subscription
        consumer._pubsub.close()
        consumer.on_state_change = capture
        try:
            consumer._reconnect_pubsub()
        finally:
            del consumer.on_state_change

        # every subscribed task was read back, across slots
        assert sorted(seen) == sorted(ids)
        # the finished ones are not subscribed again, the running one is
        running_key = backend.get_key_for_task(running)
        assert consumer.subscribed_to == {running_key}
        assert consumer._pubsub.subscribed
        consumer._pubsub.get_message(timeout=TIMEOUT)  # the confirmation
        channels = backend.client.pubsub_channels()
        assert running_key in channels
        assert not {backend.get_key_for_task(ids[0]),
                    backend.get_key_for_task(ids[2])} & set(channels)
    finally:
        consumer.cancel_for(running)
        consumer.stop()
        for task_id in ids:
            backend.forget(task_id)


def test_result_consumer_reconnect_pubsub_with_nothing_subscribed(backend):
    consumer = backend.result_consumer
    initial, later = uuid(), uuid()
    consumer.start(initial)
    consumer.cancel_for(initial)
    assert not consumer.subscribed_to
    try:
        consumer._reconnect_pubsub()

        assert consumer._pubsub is not None
        # the new pubsub is usable as soon as something is subscribed to
        consumer.consume_from(later)
        key = backend.get_key_for_task(later)
        assert consumer.subscribed_to == {key}
        consumer._pubsub.get_message(timeout=TIMEOUT)  # the confirmation
        assert key in backend.client.pubsub_channels()
    finally:
        consumer.cancel_for(later)
        consumer.stop()


def test_chord_body_receives_each_header_result_once(app, backend, manager):
    # Distinct header values, so a dropped or doubled header result fails.
    left, right = uuid(), uuid()
    result = chord([identity.s(left), identity.s(right)])(identity.s())
    gid = result.parent.id
    try:
        assert result.get(timeout=TIMEOUT) == [left, right]
        # the chord state was cleaned up; only the done marker remains
        assert backend.client.exists(*[
            backend.get_key_for_group(gid, suffix)
            for suffix in ('.j', '.t', '.s', '.d')
        ]) == 0
        assert backend.client.get(backend.get_key_for_group(gid, '.c')) == b'done'
    finally:
        result.forget()  # the header results with it
        backend.client.delete(backend.get_key_for_group(gid, '.c'))
