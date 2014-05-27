# -*- coding: utf-8 -*-
"""
    celery.backends.amqp
    ~~~~~~~~~~~~~~~~~~~~

    The AMQP result backend.

    This backend publishes results as messages.

"""
from __future__ import absolute_import

import socket

from operator import itemgetter

from kombu import Exchange, Queue, Producer, Consumer

from celery import states
from celery.result import AsyncResult
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


class AMQPBackend(BaseBackend):
    """Publishes results by sending messages."""
    Exchange = Exchange
    Queue = NoCacheQueue
    Consumer = Consumer
    Producer = Producer

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
        exchange = exchange or conf.CELERY_RESULT_EXCHANGE
        exchange_type = exchange_type or conf.CELERY_RESULT_EXCHANGE_TYPE
        self.exchange = self._create_exchange(
            exchange, exchange_type, self.delivery_mode,
        )
        self.serializer = serializer or conf.CELERY_RESULT_SERIALIZER
        self.auto_delete = auto_delete

        self.expires = None
        if 'expires' not in kwargs or kwargs['expires'] is not None:
            self.expires = self.prepare_expires(kwargs.get('expires'))
        self.queue_arguments = dictfilter({
            'x-expires': maybe_s_to_ms(self.expires),
        })

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

    def store_result(self, task_id, result, status,
                     traceback=None, request=None, **kwargs):
        """Send task return value and status."""
        routing_key, correlation_id = self.destination_for(task_id, request)
        if not routing_key:
            return
        with self.app.amqp.producer_pool.acquire(block=True) as producer:
            producer.publish(
                {'task_id': task_id, 'status': status,
                 'result': self.encode_result(result, status),
                 'traceback': traceback,
                 'children': self.current_task_children(request),
                 'hostname': request.hostname if request else None},
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

    def wait_for(self, task_id, timeout=None, cache=True, propagate=True,
                 no_ack=True, on_interval=None,
                 READY_STATES=states.READY_STATES,
                 PROPAGATE_STATES=states.PROPAGATE_STATES,
                 **kwargs):
        #XXX mark as deprecated
        result = task_id
        if not isinstance(task_id, AsyncResult):
            result = AsyncResult(task_id)
        reply = next(self.wait_until_complete(
            (result,),
            timeout=timeout,
            propagate=propagate,
            no_ack=no_ack,
            on_interval=on_interval,
        ))
        return reply.result

    def join_native(self, results, timeout=None, propagate=True,
                    no_ack=True, interval=0.5, on_interval=None,
                    now=monotonic):
        results = {result.id: result for result in results
                   if not result.ready()}
        unclaimed = self._unclaimed

        # Workaround for missing nonlocal in py2
        current_result = []

        def callback(meta, message):
            # XXX: uncomment when only py3 will be supported and use None
            # nonlocal current_result
            task_id = meta['task_id']
            result = results.get(task_id)
            if result:
                result.send(meta)
                if result.ready():
                    current_result.append(result)
                    del results[task_id]  # Don't need to wait
            else:
                unclaimed[task_id].append(meta)

        bindings = self._many_bindings(id for id in results)
        with self.app.pool.acquire_channel(block=True) as (conn, channel):
            with self.Consumer(channel, bindings, no_ack=no_ack,
                               accept=self.accept) as consumer:
                wait = conn.drain_events
                consumer.callbacks[:] = [callback]
                time_start = now()
                while results:
                    if timeout and now() - time_start >= timeout:
                        raise socket.timeout()
                    try:
                        wait(timeout=interval)  # XXX: does interval fit this?
                    except socket.timeout:
                        pass
                    if on_interval:
                        on_interval()
                    if current_result:
                        yield current_result.pop()

    def get_task_meta(self, result, backlog_limit=1000):
        # Polling and using basic_get
        task_id = result.id
        unclaimed = self._unclaimed.get(task_id)
        if unclaimed:
            meta = unclaimed[-1]
            result.send(meta)
            return meta
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
                    result.send(acc.payload)
                if prev:
                    # backends are not expected to keep history,
                    # so we delete everything except the most recent state.
                    prev.ack()
                    prev = None
            else:
                raise self.BacklogLimitExceeded(task_id)

            if latest:
                latest.requeue()
                return latest.payload
            else:
                return result._cache
    poll = get_task_meta  # XXX compat

    def _many_bindings(self, ids):
        return [self._create_binding(task_id) for task_id in ids]

    def get_many(self, task_ids, timeout=None, no_ack=True,
                 now=monotonic, getfields=itemgetter('status', 'task_id'),
                 READY_STATES=states.READY_STATES,
                 PROPAGATE_STATES=states.PROPAGATE_STATES, **kwargs):
        #XXX mark as deprecated
        results = []
        for task_id in task_ids:
            if not isinstance(task_id, AsyncResult):
                task_id = AsyncResult(task_id)
            results.append(task_id)
        return self.wait_until_complete(results, timeout=timeout,
                                        no_ack=no_ack, now=now)

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
