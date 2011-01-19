# -*- coding: utf-8 -*-
import os
import socket
import time

from datetime import timedelta

from kombu.entity import Exchange, Queue
from kombu.messaging import Consumer, Producer

from celery import states
from celery.backends.base import BaseDictBackend
from celery.exceptions import TimeoutError
from celery.utils import timeutils


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

    _pool = None
    _pool_owner_pid = None

    def __init__(self, connection=None, exchange=None, exchange_type=None,
            persistent=None, serializer=None, auto_delete=True,
            expires=None, connection_max=None, **kwargs):
        super(AMQPBackend, self).__init__(**kwargs)
        conf = self.app.conf
        self._connection = connection
        self.queue_arguments = {}
        if persistent is None:
            persistent = conf.CELERY_RESULT_PERSISTENT
        self.persistent = persistent
        delivery_mode = persistent and "persistent" or "transient"
        exchange = exchange or conf.CELERY_RESULT_EXCHANGE
        exchange_type = exchange_type or conf.CELERY_RESULT_EXCHANGE_TYPE
        self.exchange = self.Exchange(name=exchange,
                                      type=exchange_type,
                                      delivery_mode=delivery_mode,
                                      durable=self.persistent,
                                      auto_delete=auto_delete)
        self.serializer = serializer or conf.CELERY_RESULT_SERIALIZER
        self.auto_delete = auto_delete
        self.expires = expires
        if self.expires is None:
            self.expires = conf.CELERY_AMQP_TASK_RESULT_EXPIRES
        if isinstance(self.expires, timedelta):
            self.expires = timeutils.timedelta_seconds(self.expires)
        if self.expires is not None:
            self.expires = int(self.expires)
            # requires RabbitMQ 2.1.0 or higher.
            self.queue_arguments["x-expires"] = int(self.expires * 1000.0)
        self.connection_max = (connection_max or
                               conf.CELERY_AMQP_TASK_RESULT_CONNECTION_MAX)

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
        if hasattr(connection, "_result_producer_chan") and \
                connection._result_producer_chan is not None and \
                connection._result_producer_chan.connection is not None:
            channel = connection._result_producer_chan
        else:
            channel = connection._result_producer_chan = connection.channel()

        try:
            self._create_producer(task_id, channel).publish(meta)
        finally:
            channel.close()

    def revive(self, channel):
        pass

    def _store_result(self, task_id, result, status, traceback=None,
            max_retries=20, interval_start=0, interval_step=1,
            interval_max=1):
        """Send task return value and status."""
        conn = self.pool.acquire(block=True)
        try:
            send = conn.ensure(self, self._publish_result,
                        max_retries=max_retries,
                        interval_start=interval_start,
                        interval_step=interval_step,
                        interval_max=interval_max)
            send(conn, task_id, {"task_id": task_id, "status": status,
                                 "result": self.encode_result(result, status),
                                 "traceback": traceback})
        finally:
            conn.release()

        return result

    def get_task_meta(self, task_id, cache=True):
        if cache and task_id in self._cache:
            return self._cache[task_id]
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

    def poll(self, task_id):
        conn = self.pool.acquire(block=True)
        channel = conn.channel()
        try:
            binding = self._create_binding(task_id)(channel)
            binding.declare()
            result = binding.get()
            if result:
                payload = self._cache[task_id] = result.payload
                return payload
            elif task_id in self._cache:  # use previously received state.
                return self._cache[task_id]
            return {"status": states.PENDING, "result": None}
        finally:
            channel.close()
            conn.release()

    def drain_events(self, connection, consumer, timeout=None, now=time.time):
        wait = connection.drain_events
        results = {}

        def callback(meta, message):
            if meta["status"] in states.READY_STATES:
                uuid = repair_uuid(message.delivery_info["routing_key"])
                results[uuid] = meta
        consumer.register_callback(callback)

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
        conn = self.pool.acquire(block=True)
        channel = conn.channel()
        try:
            binding = self._create_binding(task_id)
            consumer = self._create_consumer(binding, channel)
            consumer.consume()
            try:
                return self.drain_events(conn, consumer, timeout).values()[0]
            finally:
                consumer.cancel()
        finally:
            channel.close()
            conn.release()

    def get_many(self, task_ids, timeout=None):
        conn = self.pool.acquire(block=True)
        channel = conn.channel()
        try:
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
            consumer = self._create_consumer(bindings, channel)
            consumer.consume()
            try:
                while ids:
                    r = self.drain_events(conn, consumer, timeout)
                    ids ^= set(r.keys())
                    for ready_id, ready_meta in r.items():
                        yield ready_id, ready_meta
            except:   # ☹ Py2.4 — Cannot yield inside try: finally: block
                consumer.cancel()
                raise
            consumer.cancel()

        except:  # … ☹
            channel.close()
            conn.release()
            raise
        channel.close()
        conn.release()

    def reload_task_result(self, task_id):
        raise NotImplementedError(
                "reload_task_result is not supported by this backend.")

    def reload_taskset_result(self, task_id):
        """Reload taskset result, even if it has been previously fetched."""
        raise NotImplementedError(
                "reload_taskset_result is not supported by this backend.")

    def save_taskset(self, taskset_id, result):
        """Store the result and status of a task."""
        raise NotImplementedError(
                "save_taskset is not supported by this backend.")

    def restore_taskset(self, taskset_id, cache=True):
        """Get the result of a taskset."""
        raise NotImplementedError(
                "restore_taskset is not supported by this backend.")

    def _set_pool(self):
        self._pool = self.app.broker_connection().Pool(self.connection_max)
        self._pool_owner_pid = os.getpid()

    def _reset_after_fork(self):
        self._pool.force_close_all()

    @property
    def pool(self):
        if self._pool is None:
            self._set_pool()
        elif os.getpid() != self._pool_owner_pid:
            print("--- RESET POOL AFTER FORK --- ")
            self._reset_after_fork()
            self._set_pool()
        return self._pool
