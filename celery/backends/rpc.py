# -*- coding: utf-8 -*-
"""The ``RPC`` result backend for AMQP brokers.

RPC-style result backend, using reply-to and one queue per client.
"""
from __future__ import absolute_import, unicode_literals

import kombu
import time

from kombu.common import maybe_declare
from kombu.utils.compat import register_after_fork
from kombu.utils.objects import cached_property

from celery import current_task
from celery import states
from celery._state import task_join_will_block
from celery.five import items, range

from . import base
from .async import AsyncBackendMixin, BaseResultConsumer

__all__ = ['BacklogLimitExceeded', 'BaseRPCBackend', 'RPCBackend']


class BacklogLimitExceeded(Exception):
    """Too much state history to fast-forward."""


class NoCacheQueue(kombu.Queue):
    can_cache_declaration = False


def _on_after_fork_cleanup_backend(backend):
    backend._after_fork()


class ResultConsumer(BaseResultConsumer):
    Consumer = kombu.Consumer

    _connection = None
    _consumer = None

    def __init__(self, *args, **kwargs):
        super(ResultConsumer, self).__init__(*args, **kwargs)
        self._create_binding = self.backend._create_binding

    def start(self, initial_task_id, no_ack=True, **kwargs):
        self._connection = self.app.connection()
        initial_queue = self._create_binding(initial_task_id)
        self._consumer = self.Consumer(
            self._connection.default_channel, [initial_queue],
            callbacks=[self.on_state_change], no_ack=no_ack,
            accept=self.accept)
        self._consumer.consume()

    def drain_events(self, timeout=None):
        if self._connection:
            return self._connection.drain_events(timeout=timeout)
        elif timeout:
            time.sleep(timeout)

    def stop(self):
        try:
            self._consumer.cancel()
        finally:
            self._connection.close()

    def on_after_fork(self):
        self._consumer = None
        if self._connection is not None:
            self._connection.collect()
            self._connection = None

    def consume_from(self, task_id):
        if self._consumer is None:
            return self.start(task_id)
        queue = self._create_binding(task_id)
        if not self._consumer.consuming_from(queue):
            self._consumer.add_queue(queue)
            self._consumer.consume()

    def cancel_for(self, task_id):
        if self._consumer:
            self._consumer.cancel_by_queue(self._create_binding(task_id).name)


class BaseRPCBackend(base.Backend, AsyncBackendMixin):
    """Base class for the RPC result backend."""

    Exchange = kombu.Exchange
    Queue = NoCacheQueue
    Consumer = kombu.Consumer
    Producer = kombu.Producer
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
        super(BaseRPCBackend, self).__init__(app, **kwargs)
        conf = self.app.conf
        self._connection = connection
        self._out_of_band = {}
        self.persistent = self.prepare_persistent(persistent)
        self.delivery_mode = 2 if self.persistent else 1
        exchange = exchange or conf.result_exchange
        exchange_type = exchange_type or conf.result_exchange_type
        self.exchange = self._create_exchange(
            exchange, exchange_type, self.delivery_mode,
        )
        self.serializer = serializer or conf.result_serializer
        self.auto_delete = auto_delete
        self.result_consumer = self.ResultConsumer(
            self, self.app, self.accept,
            self._pending_results, self._pending_messages,
        )
        if register_after_fork is not None:
            register_after_fork(self, _on_after_fork_cleanup_backend)

    def _after_fork(self):
        self._pending_results.clear()
        self.result_consumer._after_fork()

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

    def on_out_of_band_result(self, task_id, message):
        if self.result_consumer:
            self.result_consumer.on_out_of_band_result(message)
        self._out_of_band[task_id] = message

    def get_task_meta(self, task_id, backlog_limit=1000):
        buffered = self._out_of_band.pop(task_id, None)
        if buffered:
            return self._set_cache_by_message(task_id, buffered)

        # Polling and using basic_get
        latest_by_id = {}
        prev = None
        for acc in self._slurp_from_queue(task_id, self.accept, backlog_limit):
            tid = self._get_message_task_id(acc)
            prev, latest_by_id[tid] = latest_by_id.get(tid), acc
            if prev:
                # backends aren't expected to keep history,
                # so we delete everything except the most recent state.
                prev.ack()
                prev = None

        latest = latest_by_id.pop(task_id, None)
        for tid, msg in items(latest_by_id):
            self.on_out_of_band_result(tid, msg)

        if latest:
            latest.requeue()
            return self._set_cache_by_message(task_id, latest)
        else:
            # no new state, use previous
            try:
                return self._cache[task_id]
            except KeyError:
                # result probably pending.
                return {'status': states.PENDING, 'result': None}
    poll = get_task_meta  # XXX compat

    def _set_cache_by_message(self, task_id, message):
        payload = self._cache[task_id] = self.meta_from_decoded(
            message.payload)
        return payload

    def _slurp_from_queue(self, task_id, accept,
                          limit=1000, no_ack=False):
        with self.app.pool.acquire_channel(block=True) as (_, channel):
            binding = self._create_binding(task_id)(channel)
            binding.declare()

            for _ in range(limit):
                msg = binding.get(accept=accept, no_ack=no_ack)
                if not msg:
                    break
                yield msg
            else:
                raise self.BacklogLimitExceeded(task_id)

    def _get_message_task_id(self, message):
        try:
            # try property first so we don't have to deserialize
            # the payload.
            return message.properties['correlation_id']
        except (AttributeError, KeyError):
            # message sent by old Celery version, need to deserialize.
            return message.payload['task_id']

    def revive(self, channel):
        pass

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
        return super(BaseRPCBackend, self).__reduce__(args, dict(
            kwargs,
            connection=self._connection,
            exchange=self.exchange.name,
            exchange_type=self.exchange.type,
            persistent=self.persistent,
            serializer=self.serializer,
            auto_delete=self.auto_delete,
            expires=self.expires,
        ))


class RPCBackend(BaseRPCBackend):
    """RPC result backend."""

    persistent = False

    class Consumer(kombu.Consumer):
        """Consumer that requires manual declaration of queues."""

        auto_declare = False

    def _create_exchange(self, name, type='direct', delivery_mode=2):
        # uses direct to queue routing (anon exchange).
        return self.Exchange(None)

    def _create_binding(self, task_id):
        return self.binding

    def on_task_call(self, producer, task_id):
        if not task_join_will_block():
            maybe_declare(self.binding(producer.channel), retry=True)

    def rkey(self, task_id):
        return task_id

    def destination_for(self, task_id, request):
        # Request is a new argument for backends, so must still support
        # old code that rely on current_task
        try:
            request = request or current_task.request
        except AttributeError:
            raise RuntimeError(
                'RPC backend missing task request for {0!r}'.format(task_id))
        return request.reply_to, request.correlation_id or task_id

    def on_reply_declare(self, task_id):
        pass

    def on_result_fulfilled(self, result):
        pass

    def as_uri(self, include_password=True):
        return 'rpc://'

    @property
    def binding(self):
        return self.Queue(
            self.oid, self.exchange, self.oid,
            durable=False,
            auto_delete=True,
            expires=self.expires,
        )

    @cached_property
    def oid(self):
        return self.app.oid
