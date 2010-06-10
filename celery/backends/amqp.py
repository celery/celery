"""celery.backends.amqp"""
import socket

from carrot.messaging import Consumer, Publisher

from celery import conf
from celery import states
from celery.exceptions import TimeoutError
from celery.backends.base import BaseDictBackend
from celery.messaging import establish_connection


class ResultPublisher(Publisher):
    exchange = conf.RESULT_EXCHANGE
    exchange_type = conf.RESULT_EXCHANGE_TYPE
    delivery_mode = conf.RESULT_PERSISTENT and 2 or 1
    serializer = conf.RESULT_SERIALIZER
    durable = conf.RESULT_PERSISTENT

    def __init__(self, connection, task_id, **kwargs):
        super(ResultPublisher, self).__init__(connection,
                        routing_key=task_id.replace("-", ""),
                        **kwargs)


class ResultConsumer(Consumer):
    exchange = conf.RESULT_EXCHANGE
    exchange_type = conf.RESULT_EXCHANGE_TYPE
    durable = conf.RESULT_PERSISTENT
    no_ack = True
    auto_delete = True

    def __init__(self, connection, task_id, **kwargs):
        routing_key = task_id.replace("-", "")
        super(ResultConsumer, self).__init__(connection,
                queue=routing_key, routing_key=routing_key, **kwargs)


class AMQPBackend(BaseDictBackend):
    """AMQP backend. Publish results by sending messages to the broker
    using the task id as routing key.

    **NOTE:** Results published using this backend is read-once only.
    After the result has been read, the result is deleted. (however, it's
    still cached locally by the backend instance).

    """

    exchange = conf.RESULT_EXCHANGE
    exchange_type = conf.RESULT_EXCHANGE_TYPE
    persistent = conf.RESULT_PERSISTENT
    serializer = conf.RESULT_SERIALIZER
    _connection = None

    def __init__(self, *args, **kwargs):
        self._connection = kwargs.get("connection", None)
        super(AMQPBackend, self).__init__(*args, **kwargs)

    def _create_publisher(self, task_id, connection):
        delivery_mode = self.persistent and 2 or 1

        # Declares the queue.
        self._create_consumer(task_id, connection).close()

        return ResultPublisher(connection, task_id,
                               exchange=self.exchange,
                               exchange_type=self.exchange_type,
                               delivery_mode=delivery_mode,
                               serializer=self.serializer)

    def _create_consumer(self, task_id, connection):
        return ResultConsumer(connection, task_id,
                              exchange=self.exchange,
                              exchange_type=self.exchange_type,
                              durable=self.persistent)

    def store_result(self, task_id, result, status, traceback=None):
        """Send task return value and status."""
        result = self.encode_result(result, status)

        meta = {"task_id": task_id,
                "result": result,
                "status": status,
                "traceback": traceback}

        publisher = self._create_publisher(task_id, self.connection)
        try:
            publisher.send(meta)
        finally:
            publisher.close()

        return result

    def wait_for(self, task_id, timeout=None, cache=True):
        if task_id in self._cache:
            meta = self._cache[task_id]
        else:
            try:
                meta = self.consume(task_id, timeout=timeout)
            except socket.timeout:
                raise TimeoutError("The operation timed out.")

        if meta["status"] == states.SUCCESS:
            return meta["result"]
        elif meta["status"] in states.PROPAGATE_STATES:
            raise self.exception_to_python(meta["result"])

    def poll(self, task_id):
        routing_key = task_id.replace("-", "")
        consumer = self._create_consumer(task_id, self.connection)
        result = consumer.fetch()
        payload = None
        if result:
            payload = self._cache[task_id] = result.payload
            consumer.backend.queue_delete(routing_key)
        else:
            # Use previously received status if any.
            if task_id in self._cache:
                payload = self._cache[task_id]
            else:
                payload = {"status": states.PENDING, "result": None}

        consumer.close()
        return payload

    def consume(self, task_id, timeout=None):
        results = []

        def callback(message_data, message):
            results.append(message_data)

        routing_key = task_id.replace("-", "")

        wait = self.connection.connection.wait_multi
        consumer = self._create_consumer(task_id, self.connection)
        consumer.register_callback(callback)

        consumer.consume()
        try:
            wait([consumer.backend.channel], timeout=timeout)
        finally:
            consumer.backend.queue_delete(routing_key)
            consumer.close()

        self._cache[task_id] = results[0]
        return results[0]

    def get_task_meta(self, task_id, cache=True):
        return self.poll(task_id)

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

    def close(self):
        if self._connection is not None:
            self._connection.close()

    @property
    def connection(self):
        if not self._connection:
            self._connection = establish_connection()
        return self._connection
