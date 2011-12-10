# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import with_statement

import socket
import threading
import time

from itertools import count

from kombu.entity import Exchange, Queue
from kombu.messaging import Consumer, Producer

from .. import states
from ..exceptions import TimeoutError

from .base import BaseDictBackend


class BacklogLimitExceeded(Exception):
    """Too much state history to fast-forward."""


def repair_uuid(s):
    # Historically the dashes in UUIDS are removed from AMQ entity names,
    # but there is no known reason to.  Hopefully we'll be able to fix
    # this in v3.0.
    return "%s-%s-%s-%s-%s" % (s[:8], s[8:12], s[12:16], s[16:20], s[20:])


class AMQPBackend(BaseDictBackend):
    """Publishes results by sending messages."""
    Exchange = Exchange
    Queue = Queue
    Consumer = Consumer
    Producer = Producer

    BacklogLimitExceeded = BacklogLimitExceeded

    supports_native_join = True

    def __init__(self, connection=None, exchange=None, exchange_type=None,
            persistent=None, serializer=None, auto_delete=True,
            **kwargs):
        super(AMQPBackend, self).__init__(**kwargs)
        conf = self.app.conf
        self._connection = connection
        self.queue_arguments = {}
        self.persistent = (conf.CELERY_RESULT_PERSISTENT if persistent is None
                                                         else persistent)
        delivery_mode = persistent and "persistent" or "transient"
        exchange = exchange or conf.CELERY_RESULT_EXCHANGE
        exchange_type = exchange_type or conf.CELERY_RESULT_EXCHANGE_TYPE
        self.exchange = self.Exchange(name=exchange,
                                      type=exchange_type,
                                      delivery_mode=delivery_mode,
                                      durable=self.persistent,
                                      auto_delete=False)
        self.serializer = serializer or conf.CELERY_RESULT_SERIALIZER
        self.auto_delete = auto_delete

        # AMQP_TASK_RESULT_EXPIRES setting is deprecated and will be
        # removed in version 3.0.
        dexpires = conf.CELERY_AMQP_TASK_RESULT_EXPIRES

        self.expires = None
        if "expires" in kwargs:
            if kwargs["expires"] is not None:
                self.expires = self.prepare_expires(kwargs["expires"])
        else:
            self.expires = self.prepare_expires(dexpires)

        if self.expires:
            self.queue_arguments["x-expires"] = int(self.expires * 1000)
        self.mutex = threading.Lock()

    def _create_binding(self, task_id):
        name = task_id.replace("-", "")
        return self.Queue(name=name,
                          exchange=self.exchange,
                          routing_key=name,
                          durable=self.persistent,
                          auto_delete=self.auto_delete,
                          queue_arguments=self.queue_arguments)

    def _create_producer(self, task_id, channel):
        self._create_binding(task_id)(channel).declare()
        return self.Producer(channel, exchange=self.exchange,
                             routing_key=task_id.replace("-", ""),
                             serializer=self.serializer)

    def _create_consumer(self, bindings, channel):
        return self.Consumer(channel, bindings, no_ack=True)

    def _publish_result(self, connection, task_id, meta):
        # cache single channel
        if connection._default_channel is not None and \
                connection._default_channel.connection is None:
            connection.maybe_close_channel(connection._default_channel)
        channel = connection.default_channel

        self._create_producer(task_id, channel).publish(meta)

    def revive(self, channel):
        pass

    def _store_result(self, task_id, result, status, traceback=None,
            max_retries=20, interval_start=0, interval_step=1,
            interval_max=1):
        """Send task return value and status."""
        with self.mutex:
            with self.app.pool.acquire(block=True) as conn:

                def errback(error, delay):
                    print("Couldn't send result for %r: %r. Retry in %rs." % (
                            task_id, error, delay))

                send = conn.ensure(self, self._publish_result,
                            max_retries=max_retries,
                            errback=errback,
                            interval_start=interval_start,
                            interval_step=interval_step,
                            interval_max=interval_max)
                send(conn, task_id, {"task_id": task_id, "status": status,
                                "result": self.encode_result(result, status),
                                "traceback": traceback})
        return result

    def get_task_meta(self, task_id, cache=True):
        return self.poll(task_id)

    def wait_for(self, task_id, timeout=None, cache=True, propagate=True,
            **kwargs):
        cached_meta = self._cache.get(task_id)
        if cache and cached_meta and \
                cached_meta["status"] in states.READY_STATES:
            meta = cached_meta
        else:
            try:
                meta = self.consume(task_id, timeout=timeout)
            except socket.timeout:
                raise TimeoutError("The operation timed out.")

        state = meta["status"]
        if state == states.SUCCESS:
            return meta["result"]
        elif state in states.PROPAGATE_STATES:
            if propagate:
                raise self.exception_to_python(meta["result"])
            return meta["result"]
        else:
            return self.wait_for(task_id, timeout, cache)

    def poll(self, task_id, backlog_limit=100):
        with self.app.pool.acquire_channel(block=True) as (_, channel):
            binding = self._create_binding(task_id)(channel)
            binding.declare()
            latest, acc = None, None
            for i in count():  # fast-forward
                latest, acc = acc, binding.get(no_ack=True)
                if not acc:
                    break
                if i > backlog_limit:
                    raise self.BacklogLimitExceeded(task_id)
            if latest:
                payload = self._cache[task_id] = latest.payload
                return payload
            elif task_id in self._cache:  # use previously received state.
                return self._cache[task_id]
            return {"status": states.PENDING, "result": None}

    def drain_events(self, connection, consumer, timeout=None, now=time.time):
        wait = connection.drain_events
        results = {}

        def callback(meta, message):
            if meta["status"] in states.READY_STATES:
                uuid = repair_uuid(message.delivery_info["routing_key"])
                results[uuid] = meta

        consumer.callbacks[:] = [callback]
        time_start = now()

        while 1:
            # Total time spent may exceed a single call to wait()
            if timeout and now() - time_start >= timeout:
                raise socket.timeout()
            wait(timeout=timeout)
            if results:  # got event on the wanted channel.
                break
        self._cache.update(results)
        return results

    def consume(self, task_id, timeout=None):
        with self.app.pool.acquire_channel(block=True) as (conn, channel):
            binding = self._create_binding(task_id)
            with self._create_consumer(binding, channel) as consumer:
                return self.drain_events(conn, consumer, timeout).values()[0]

    def get_many(self, task_ids, timeout=None, **kwargs):
        with self.app.pool.acquire_channel(block=True) as (conn, channel):
            ids = set(task_ids)
            cached_ids = set()
            for task_id in ids:
                try:
                    cached = self._cache[task_id]
                except KeyError:
                    pass
                else:
                    if cached["status"] in states.READY_STATES:
                        yield task_id, cached
                        cached_ids.add(task_id)

            ids ^= cached_ids
            bindings = [self._create_binding(task_id) for task_id in task_ids]
            with self._create_consumer(bindings, channel) as consumer:
                while ids:
                    r = self.drain_events(conn, consumer, timeout)
                    ids ^= set(r)
                    for ready_id, ready_meta in r.iteritems():
                        yield ready_id, ready_meta

    def reload_task_result(self, task_id):
        raise NotImplementedError(
                "reload_task_result is not supported by this backend.")

    def reload_taskset_result(self, task_id):
        """Reload taskset result, even if it has been previously fetched."""
        raise NotImplementedError(
                "reload_taskset_result is not supported by this backend.")

    def save_taskset(self, taskset_id, result):
        raise NotImplementedError(
                "save_taskset is not supported by this backend.")

    def restore_taskset(self, taskset_id, cache=True):
        raise NotImplementedError(
                "restore_taskset is not supported by this backend.")

    def delete_taskset(self, taskset_id):
        raise NotImplementedError(
                "delete_taskset is not supported by this backend.")

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            dict(connection=self._connection,
                 exchange=self.exchange.name,
                 exchange_type=self.exchange.type,
                 persistent=self.persistent,
                 serializer=self.serializer,
                 auto_delete=self.auto_delete,
                 expires=self.expires))
        return super(AMQPBackend, self).__reduce__(args, kwargs)
