# -*- coding: utf-8 -*-
"""
    celery.backends.amqp
    ~~~~~~~~~~~~~~~~~~~~

    The AMQP result backend.

    This backend publishes results as messages.

"""
from __future__ import absolute_import

import socket

from collections import deque
from operator import itemgetter

from kombu import Exchange, Queue, Producer, Consumer

from celery import states
from celery.exceptions import TimeoutError
from celery.five import range, monotonic
from celery.utils.functional import dictfilter
from celery.utils.log import get_logger
from celery.utils.timeutils import maybe_s_to_ms

from .base import BaseBackend

__all__ = ['BacklogLimitExceeded', 'AMQPBackend']

logger = get_logger(__name__)


class BacklogLimitExceeded(Exception):
    """Too much state history to fast-forward."""


def repair_uuid(s):
    # Historically the dashes in UUIDS are removed from AMQ entity names,
    # but there is no known reason to.  Hopefully we'll be able to fix
    # this in v4.0.
    return '%s-%s-%s-%s-%s' % (s[:8], s[8:12], s[12:16], s[16:20], s[20:])


class NoCacheQueue(Queue):
    can_cache_declaration = False


class ResultConsumer(object):
    Consumer = Consumer

    def __init__(self, backend, app, accept, pending_results):
        self.backend = backend
        self.app = app
        self.accept = accept
        self._pending_results = pending_results
        self._consumer = None
        self._conn = None
        self.on_message = None
        self.bucket = None

    def consume(self, task_id, timeout=None, no_ack=True, on_interval=None):
        wait = self.drain_events
        with self.app.pool.acquire_channel(block=True) as (conn, channel):
            binding = self.backend._create_binding(task_id)
            with self.Consumer(channel, binding,
                               no_ack=no_ack, accept=self.accept) as consumer:
                while 1:
                    try:
                        return wait(
                            conn, consumer, timeout, on_interval)[task_id]
                    except KeyError:
                        continue

    def wait_for_pending(self, result,
                         callback=None, propagate=True, **kwargs):
        for _ in self._wait_for_pending(result, **kwargs):
            pass
        return result.maybe_throw(callback=callback, propagate=propagate)

    def _wait_for_pending(self, result, timeout=None, interval=0.5,
                 no_ack=True, on_interval=None, callback=None,
                 on_message=None, propagate=True):
        prev_on_m, self.on_message = self.on_message, on_message
        try:
            for _ in self.drain_events_until(
                    result.on_ready, timeout=timeout,
                    on_interval=on_interval):
                yield
        except socket.timeout:
            raise TimeoutError('The operation timed out.')
        finally:
            self.on_message = prev_on_m

    def collect_for_pending(self, result, bucket=None, **kwargs):
        prev_bucket, self.bucket = self.bucket, bucket
        try:
            for _ in self._wait_for_pending(result, **kwargs):
                yield
        finally:
            self.bucket = prev_bucket

    def start(self, initial_queue, no_ack=True):
        self._conn = self.app.connection()
        self._consumer = self.Consumer(
            self._conn.default_channel, [initial_queue],
            callbacks=[self.on_state_change], no_ack=no_ack,
            accept=self.accept)
        self._consumer.consume()

    def stop(self):
        try:
            self._consumer.cancel()
        finally:
            self._connection.close()

    def consume_from(self, queue):
        if self._consumer is None:
            return self.start(queue)
        if not self._consumer.consuming_from(queue):
            self._consumer.add_queue(queue)
            self._consumer.consume()

    def cancel_for(self, queue):
        self._consumer.cancel_by_queue(queue)

    def on_state_change(self, meta, message):
        if self.on_message:
            self.on_message(meta)
        if meta['status'] in states.READY_STATES:
            try:
                result = self._pending_results[meta['task_id']]
            except KeyError:
                return
            result._maybe_set_cache(meta)
            if self.bucket is not None:
                self.bucket.append(result)

    def drain_events_until(self, p, timeout=None, on_interval=None,
                           monotonic=monotonic, wait=None):
        wait = wait or self._conn.drain_events
        time_start = monotonic()

        while 1:
            # Total time spent may exceed a single call to wait()
            if timeout and monotonic() - time_start >= timeout:
                raise socket.timeout()
            try:
                yield wait(timeout=1)
            except socket.timeout:
                pass
            if on_interval:
                on_interval()
            if p.ready:  # got event on the wanted channel.
                break


class AMQPBackend(BaseBackend):
    """Publishes results by sending messages."""
    Exchange = Exchange
    Queue = NoCacheQueue
    Consumer = Consumer
    Producer = Producer
    ResultConsumer = ResultConsumer

    BacklogLimitExceeded = BacklogLimitExceeded

    persistent = True
    supports_autoexpire = True
    supports_native_join = True

    retry_policy = {
        'max_retries': 20,
        'interval_start': 0,
        'interval_step': 1,
        'interval_max': 1,
    }

    def __init__(self, app, connection=None, exchange=None, exchange_type=None,
                 persistent=None, serializer=None, auto_delete=True, **kwargs):
        super(AMQPBackend, self).__init__(app, **kwargs)
        conf = self.app.conf
        self._connection = connection
        self.persistent = self.prepare_persistent(persistent)
        self.delivery_mode = 2 if self.persistent else 1
        exchange = exchange or conf.result_exchange
        exchange_type = exchange_type or conf.result_exchange_type
        self.exchange = self._create_exchange(
            exchange, exchange_type, self.delivery_mode,
        )
        self.serializer = serializer or conf.result_serializer
        self.auto_delete = auto_delete
        self.queue_arguments = dictfilter({
            'x-expires': maybe_s_to_ms(self.expires),
        })
        self.result_consumer = self.ResultConsumer(
            self, self.app, self.accept, self._pending_results)

    def _create_exchange(self, name, type='direct', delivery_mode=2):
        return self.Exchange(name=name,
                             type=type,
                             delivery_mode=delivery_mode,
                             durable=self.persistent,
                             auto_delete=False)

    def _create_binding(self, task_id):
        name = self.rkey(task_id)
        return self.Queue(name=name,
                          exchange=self.exchange,
                          routing_key=name,
                          durable=self.persistent,
                          auto_delete=self.auto_delete,
                          queue_arguments=self.queue_arguments)

    def revive(self, channel):
        pass

    def rkey(self, task_id):
        return task_id.replace('-', '')

    def destination_for(self, task_id, request):
        if request:
            return self.rkey(task_id), request.correlation_id or task_id
        return self.rkey(task_id), task_id

    def store_result(self, task_id, result, state,
                     traceback=None, request=None, **kwargs):
        """Send task return value and state."""
        routing_key, correlation_id = self.destination_for(task_id, request)
        if not routing_key:
            return
        with self.app.amqp.producer_pool.acquire(block=True) as producer:
            producer.publish(
                {'task_id': task_id, 'status': state,
                 'result': self.encode_result(result, state),
                 'traceback': traceback,
                 'children': self.current_task_children(request)},
                exchange=self.exchange,
                routing_key=routing_key,
                correlation_id=correlation_id,
                serializer=self.serializer,
                retry=True, retry_policy=self.retry_policy,
                declare=self.on_reply_declare(task_id),
                delivery_mode=self.delivery_mode,
            )
        return result

    def on_reply_declare(self, task_id):
        return [self._create_binding(task_id)]

    def get_task_meta(self, task_id, backlog_limit=1000):
        # Polling and using basic_get
        with self.app.pool.acquire_channel(block=True) as (_, channel):
            binding = self._create_binding(task_id)(channel)
            binding.declare()

            prev = latest = acc = None
            for i in range(backlog_limit):  # spool ffwd
                acc = binding.get(
                    accept=self.accept, no_ack=False,
                )
                if not acc:  # no more messages
                    break
                if acc.payload['task_id'] == task_id:
                    prev, latest = latest, acc
                if prev:
                    # backends are not expected to keep history,
                    # so we delete everything except the most recent state.
                    prev.ack()
                    prev = None
            else:
                raise self.BacklogLimitExceeded(task_id)

            if latest:
                payload = self._cache[task_id] = self.meta_from_decoded(
                    latest.payload)
                latest.requeue()
                return payload
            else:
                # no new state, use previous
                try:
                    return self._cache[task_id]
                except KeyError:
                    # result probably pending.
                    return {'status': states.PENDING, 'result': None}
    poll = get_task_meta  # XXX compat

    def wait_for_pending(self, result, timeout=None, interval=0.5,
                 no_ack=True, on_interval=None, on_message=None,
                 callback=None, propagate=True):
        return self.result_consumer.wait_for_pending(
            result, timeout=timeout, interval=interval,
            no_ack=no_ack, on_interval=on_interval,
            callback=callback, on_message=on_message, propagate=propagate,
        )

    def collect_for_pending(self, result, bucket=None, timeout=None,
                            interval=0.5, no_ack=True, on_interval=None,
                            on_message=None, callback=None, propagate=True):
        return self.result_consumer.collect_for_pending(
            result, bucket=bucket, timeout=timeout, interval=interval,
            no_ack=no_ack, on_interval=on_interval,
            callback=callback, on_message=on_message, propagate=propagate,
        )

    def add_pending_result(self, result):
        if result.id not in self._pending_results:
            self._pending_results[result.id] = result
            self.result_consumer.consume_from(self._create_binding(result.id))

    def remove_pending_result(self, result):
        self._pending_results.pop(result.id, None)
        # XXX cancel queue after result consumed

    def _many_bindings(self, ids):
        return [self._create_binding(task_id) for task_id in ids]

    def xxx_get_many(self, task_ids, timeout=None, no_ack=True,
                 on_message=None, on_interval=None,
                 now=monotonic, getfields=itemgetter('status', 'task_id'),
                 READY_STATES=states.READY_STATES,
                 PROPAGATE_STATES=states.PROPAGATE_STATES, **kwargs):
        with self.app.pool.acquire_channel(block=True) as (conn, channel):
            ids = set(task_ids)
            cached_ids = set()
            mark_cached = cached_ids.add
            for task_id in ids:
                try:
                    cached = self._cache[task_id]
                except KeyError:
                    pass
                else:
                    if cached['status'] in READY_STATES:
                        yield task_id, cached
                        mark_cached(task_id)
            ids.difference_update(cached_ids)
            results = deque()
            push_result = results.append
            push_cache = self._cache.__setitem__
            decode_result = self.meta_from_decoded

            def _on_message(message):
                body = decode_result(message.decode())
                if on_message is not None:
                    on_message(body)
                state, uid = getfields(body)
                if state in READY_STATES:
                    push_result(body) \
                        if uid in task_ids else push_cache(uid, body)

            bindings = self._many_bindings(task_ids)
            with self.Consumer(channel, bindings, on_message=_on_message,
                               accept=self.accept, no_ack=no_ack):
                wait = conn.drain_events
                popleft = results.popleft
                while ids:
                    wait(timeout=timeout)
                    while results:
                        state = popleft()
                        task_id = state['task_id']
                        ids.discard(task_id)
                        push_cache(task_id, state)
                        yield task_id, state
                    if on_interval:
                        on_interval()

    def reload_task_result(self, task_id):
        raise NotImplementedError(
            'reload_task_result is not supported by this backend.')

    def reload_group_result(self, task_id):
        """Reload group result, even if it has been previously fetched."""
        raise NotImplementedError(
            'reload_group_result is not supported by this backend.')

    def save_group(self, group_id, result):
        raise NotImplementedError(
            'save_group is not supported by this backend.')

    def restore_group(self, group_id, cache=True):
        raise NotImplementedError(
            'restore_group is not supported by this backend.')

    def delete_group(self, group_id):
        raise NotImplementedError(
            'delete_group is not supported by this backend.')

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            connection=self._connection,
            exchange=self.exchange.name,
            exchange_type=self.exchange.type,
            persistent=self.persistent,
            serializer=self.serializer,
            auto_delete=self.auto_delete,
            expires=self.expires,
        )
        return super(AMQPBackend, self).__reduce__(args, kwargs)
