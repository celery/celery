"""celery.backends.amqp"""
from carrot.connection import DjangoBrokerConnection
from carrot.messaging import Consumer, Publisher
from celery.backends.base import BaseBackend

RESULTSTORE_EXCHANGE = "celeryresults"


class Backend(BaseBackend):
    """AMQP backend. Publish results by sending messages to the broker
    using the task id as routing key.

    **NOTE:** Results published using this backend is read-once only.
    After the result has been read, the result is deleted. (however, it's
    still cached locally by the backend instance).

    """

    capabilities = ["ResultStore"]

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self.connection = DjangoBrokerConnection()
        self._cache = {}

    def _declare_queue(self, task_id, connection):
        routing_key = task_id.replace("-", "")
        backend = connection.create_backend()
        backend.queue_declare(queue=routing_key, durable=True,
                                exclusive=False, auto_delete=True)
        backend.exchange_declare(exchange=RESULTSTORE_EXCHANGE,
                                 type="direct",
                                 durable=True,
                                 auto_delete=False)
        backend.queue_bind(queue=routing_key, exchange=RESULTSTORE_EXCHANGE,
                           routing_key=routing_key)
        backend.close()

    def _publisher_for_task_id(self, task_id, connection):
        routing_key = task_id.replace("-", "")
        self._declare_queue(task_id, connection)
        p = Publisher(connection, exchange=RESULTSTORE_EXCHANGE,
                      exchange_type="direct",
                      routing_key=routing_key)
        return p

    def _consumer_for_task_id(self, task_id, connection):
        routing_key = task_id.replace("-", "")
        self._declare_queue(task_id, connection)
        return Consumer(connection, queue=routing_key,
                        exchange=RESULTSTORE_EXCHANGE,
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

    def is_successful(self, task_id):
        """Returns ``True`` if task with ``task_id`` has been executed."""
        return self.get_status(task_id) == "SUCCESS"

    def get_status(self, task_id):
        """Get the status of a task."""
        return self._get_task_meta_for(task_id)["status"]

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        return self._get_task_meta_for(task_id)["traceback"]

    def _get_task_meta_for(self, task_id):

        if task_id in self._cache:
            return self._cache[task_id]

        results = []

        def callback(message_data, message):
            results.append(message_data)
            message.ack()

        routing_key = task_id.replace("-", "")

        connection = self.connection
        consumer = self._consumer_for_task_id(task_id, connection)
        consumer.register_callback(callback)

        try:
            consumer.iterconsume().next()
        finally:
            consumer.backend.channel.queue_delete(routing_key)
            consumer.close()

        self._cache[task_id] = results[0]
        return results[0]

    def get_result(self, task_id):
        """Get the result for a task."""
        result = self._get_task_meta_for(task_id)
        if result["status"] == "FAILURE":
            return self.exception_to_python(result["result"])
        else:
            return result["result"]
