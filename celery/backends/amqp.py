"""celery.backends.amqp"""
import socket
import time
import warnings

from datetime import timedelta

from kombu.entity import Exchange, Queue
from kombu.messaging import Consumer, Producer

from celery import states
from celery.backends.base import BaseDictBackend
from celery.exceptions import TimeoutError
from celery.utils import timeutils


class AMQResultWarning(UserWarning):
    pass


class AMQPBackend(BaseDictBackend):
    """AMQP backend. Publish results by sending messages to the broker
    using the task id as routing key.

    **NOTE:** Results published using this backend is read-once only.
    After the result has been read, the result is deleted. (however, it's
    still cached locally by the backend instance).

    """

    _connection = None
    _channel = None

    def __init__(self, connection=None, exchange=None, exchange_type=None,
            persistent=None, serializer=None, auto_delete=True,
            expires=None, **kwargs):
        super(AMQPBackend, self).__init__(**kwargs)
        conf = self.app.conf
        self._connection = connection
        self.queue_arguments = {}
        exchange = exchange or conf.CELERY_RESULT_EXCHANGE
        exchange_type = exchange_type or conf.CELERY_RESULT_EXCHANGE_TYPE
        if persistent is None:
            persistent = conf.CELERY_RESULT_PERSISTENT
        self.persistent = persistent
        delivery_mode = persistent and "persistent" or "transient"
        self.exchange = Exchange(name=exchange,
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
            # WARNING: Requires RabbitMQ 2.1.0 or higher.
            # x-expires must be a signed-int, or long describing
            # the expiry time in milliseconds.
            self.queue_arguments["x-expires"] = int(self.expires * 1000.0)

    def _create_binding(self, task_id):
        name = task_id.replace("-", "")
        return Queue(name=name,
                     exchange=self.exchange,
                     routing_key=name,
                     durable=self.persistent,
                     auto_delete=self.auto_delete)

    def _create_producer(self, task_id):
        binding = self._create_binding(task_id)
        binding(self.channel).declare()

        return Producer(self.channel, exchange=self.exchange,
                        routing_key=task_id.replace("-", ""),
                        serializer=self.serializer)

    def _create_consumer(self, task_id):
        binding = self._create_binding(task_id)
        return Consumer(self.channel, [binding], no_ack=True)

    def store_result(self, task_id, result, status, traceback=None,
            max_retries=20, retry_delay=0.2):
        """Send task return value and status."""
        result = self.encode_result(result, status)

        meta = {"task_id": task_id,
                "result": result,
                "status": status,
                "traceback": traceback}

        for i in range(max_retries + 1):
            try:
                self._create_producer(task_id).publish(meta)
            except Exception, exc:
                if not max_retries:
                    raise
                self._channel = None
                self._connection = None
                warnings.warn(AMQResultWarning(
                    "Error sending result %s: %r" % (task_id, exc)))
                time.sleep(retry_delay)
            break

        return result

    def get_task_meta(self, task_id, cache=True):
        return self.poll(task_id)

    def wait_for(self, task_id, timeout=None, cache=True):
        cached_meta = self._cache.get(task_id)

        if cached_meta and cached_meta["status"] in states.READY_STATES:
            meta = cached_meta
        else:
            try:
                meta = self.consume(task_id, timeout=timeout)
            except socket.timeout:
                raise TimeoutError("The operation timed out.")

        if meta["status"] == states.SUCCESS:
            return meta["result"]
        elif meta["status"] in states.PROPAGATE_STATES:
            raise self.exception_to_python(meta["result"])
        else:
            return self.wait_for(task_id, timeout, cache)

    def poll(self, task_id):
        binding = self._create_binding(task_id)(self.channel)
        result = binding.get()
        if result:
            payload = self._cache[task_id] = result.payload
            return payload
        elif task_id in self._cache:
            return self._cache[task_id]     # use previously received state.
        return {"status": states.PENDING, "result": None}

    def consume(self, task_id, timeout=None):
        results = []

        def callback(meta, message):
            if meta["status"] in states.READY_STATES:
                results.append(meta)

        wait = self.connection.drain_events
        consumer = self._create_consumer(task_id)
        consumer.register_callback(callback)

        consumer.consume()
        try:
            time_start = time.time()
            while True:
                # Total time spent may exceed a single call to wait()
                if timeout and time.time() - time_start >= timeout:
                    raise socket.timeout()
                wait(timeout=timeout)
                if results:
                    # Got event on the wanted channel.
                    break
        finally:
            consumer.cancel()

        self._cache[task_id] = results[0]
        return results[0]

    def close(self):
        if self._channel is not None:
            self._channel.close()
            self._channel = None
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    def connection(self):
        if not self._connection:
            self._connection = self.app.broker_connection()
        return self._connection

    @property
    def channel(self):
        if not self._channel:
            self._channel = self.connection.channel()
        return self._channel

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
