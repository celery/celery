# -*- coding: utf-8 -*-
"""
    ``celery.backends.amqp``
    ~~~~~~~~~~~~~~~~~~~~~~~~

    The AMQP result backend.

    This backend publishes results as messages.

"""
from __future__ import absolute_import, unicode_literals

from kombu import Exchange, Queue, Producer, Consumer
from kombu.utils import register_after_fork

from celery import states
from celery.five import range
from celery.utils.functional import dictfilter
from celery.utils.log import get_logger
from celery.utils.timeutils import maybe_s_to_ms

from . import base
from .async import AsyncBackendMixin, BaseResultConsumer

__all__ = ['BacklogLimitExceeded', 'AMQPBackend']

logger = get_logger(__name__)


class BacklogLimitExceeded(Exception):
    """Too much state history to fast-forward."""


def repair_uuid(s):
    # Historically the dashes in UUIDS are removed from AMQ entity names,
    # but there is no known reason to.  Hopefully we'll be able to fix
    # this in v4.0.
    return '%s-%s-%s-%s-%s' % (s[:8], s[8:12], s[12:16], s[16:20], s[20:])


def _on_after_fork_cleanup_backend(backend):
    backend._after_fork()


class NoCacheQueue(Queue):
    can_cache_declaration = False


class ResultConsumer(BaseResultConsumer):
    Consumer = Consumer

    _connection = None
    _consumer = None

    def __init__(self, *args, **kwargs):
        super(ResultConsumer, self).__init__(*args, **kwargs)
        self._create_binding = self.backend._create_binding

    def start(self, initial_task_id, no_ack=True):
        self._connection = self.app.connection()
        initial_queue = self._create_binding(initial_task_id)
        self._consumer = self.Consumer(
            self._connection.default_channel, [initial_queue],
            callbacks=[self.on_state_change], no_ack=no_ack,
            accept=self.accept)
        self._consumer.consume()

    def drain_events(self, timeout=None):
        return self._connection.drain_events(timeout=timeout)

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


class AMQPBackend(base.Backend, AsyncBackendMixin):
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
        self.queue_arguments = dictfilter({
            'x-expires': maybe_s_to_ms(self.expires),
        })
        self.result_consumer = self.ResultConsumer(
            self, self.app, self.accept, self._pending_results)
        if register_after_fork is not None:
            register_after_fork(self, _on_after_fork_cleanup_backend)

    def _after_fork(self):
        self._pending_results.clear()
        self.result_consumer._after_fork()

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

    def on_out_of_band_result(self, task_id, message):
        if self.result_consumer:
            self.result_consumer.on_out_of_band_result(message)
        self._out_of_band[task_id] = message

    def get_task_meta(self, task_id, backlog_limit=1000):
        try:
            buffered = self._out_of_band.pop(task_id)
        except KeyError:
            pass
        else:
            payload = self._cache[task_id] = self.meta_from_decoded(
                buffered.payload)
            return payload
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
                try:
                    message_task_id = acc.properties['correlation_id']
                except (AttributeError, KeyError):
                    message_task_id = acc.payload['task_id']
                if message_task_id == task_id:
                    prev, latest = latest, acc
                    if prev:
                        # backends are not expected to keep history,
                        # so we delete everything except the most recent state.
                        prev.ack()
                        prev = None
                else:
                    self.on_out_of_band_result(message_task_id, acc)
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

    def as_uri(self, include_password=True):
        return 'amqp://'

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
