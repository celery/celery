"""celery.backends.amqp"""
from carrot.messaging import Consumer, Publisher

from celery import conf
from celery import states
from celery.backends.base import BaseDictBackend
from celery.messaging import establish_connection
from celery.datastructures import LocalCache


class AMQPBackend(BaseDictBackend):
    """AMQP backend. Publish results by sending messages to the broker
    using the task id as routing key.

    **NOTE:** Results published using this backend is read-once only.
    After the result has been read, the result is deleted. (however, it's
    still cached locally by the backend instance).

    """

    exchange = conf.RESULT_EXCHANGE
    capabilities = ["ResultStore"]
    _connection = None
    _use_debug_tracking = False
    _seen = set()

    def __init__(self, *args, **kwargs):
        super(AMQPBackend, self).__init__(*args, **kwargs)

    @property
    def connection(self):
        if not self._connection:
            self._connection = establish_connection()
        return self._connection

    def _declare_queue(self, task_id, connection):
        routing_key = task_id.replace("-", "")
        backend = connection.create_backend()
        backend.queue_declare(queue=routing_key, durable=True,
                                exclusive=False, auto_delete=True)
        backend.exchange_declare(exchange=self.exchange,
                                 type="direct",
                                 durable=True,
                                 auto_delete=False)
        backend.queue_bind(queue=routing_key, exchange=self.exchange,
                           routing_key=routing_key)
        backend.close()

    def _publisher_for_task_id(self, task_id, connection):
        routing_key = task_id.replace("-", "")
        self._declare_queue(task_id, connection)
        p = Publisher(connection, exchange=self.exchange,
                      exchange_type="direct",
                      routing_key=routing_key)
        return p

    def _consumer_for_task_id(self, task_id, connection):
        routing_key = task_id.replace("-", "")
        self._declare_queue(task_id, connection)
        return Consumer(connection, queue=routing_key,
                        exchange=self.exchange,
                        exchange_type="direct",
                        no_ack=False, auto_ack=False,
                        auto_delete=True,
                        routing_key=routing_key)

    def store_result(self, task_id, result, status, traceback=None):
        """Send task return value and status."""
        result = self.encode_result(result, status)

        meta = {"task_id": task_id,
                "result": result,
                "status": status,
                "traceback": traceback}

        connection = self.connection
        publisher = self._publisher_for_task_id(task_id, connection)
        publisher.send(meta, serializer="pickle")
        publisher.close()

        return result

    def _get_task_meta_for(self, task_id):
        assert task_id not in self._seen
        self._use_debug_tracking and self._seen.add(task_id)

        results = []

        def callback(message_data, message):
            results.append(message_data)
            message.ack()

        routing_key = task_id.replace("-", "")

        connection = self.connection
        consumer = self._consumer_for_task_id(task_id, connection)
        consumer.register_callback(callback)

        did_exc = None

        try:
            consumer.iterconsume().next()
        except Exception, e:
            did_exc = e

        consumer.backend.channel.queue_delete(routing_key)
        consumer.close()

        if did_exc:
            raise did_exc

        self._cache[task_id] = results[0]
        return results[0]

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
