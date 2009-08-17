"""celery.backends.amqp"""
from carrot.connection import DjangoBrokerConnection
from carrot.messaging import Consumer, Publisher
from celery.backends.base import BaseBackend

RESULTSTORE_EXCHANGE = "celres"


class Backend(BaseBackend):
    """AMQP backend. Publish results by sending messages to the broker
    using the task id as routing key.

    Note that results published using this backend is read-once only.
    After the result has been read, the result is deleted.

    """

    capabilities = ["ResultStore"]

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self._cache = {}

    def _publisher_for_task_id(self, task_id, connection):
        routing_key = task_id.replace("-", "")
        return Publisher(connection, exchange=RESULTSTORE_EXCHANGE,
                         exchange_type="direct",
                         routing_key="%s" % routing_key)

    def _consumer_for_task_id(self, task_id, connection):
        routing_key = task_id.replace("-", "")
        return Consumer(connection, queue=task_id,
                        exchange=RESULTSTORE_EXCHANGE,
                        exchange_type="direct",
                        routing_key="%s" % routing_key)

    def store_result(self, task_id, result, status):
        """Mark task as done (executed)."""
        if status == "DONE":
            result = self.prepare_result(result)
        elif status == "FAILURE":
            result = self.prepare_exception(result)

        meta = {"task_id": task_id,
                "result": result,
                "status": status}

        connection = DjangoBrokerConnection()
        publisher = self._publisher_for_task_id(task_id, connection)
        consumer = self._consumer_for_task_id(task_id, connection)
        c.fetch()
        publisher.send(meta, serializer="pickle", immediate=False)
        publisher.close()
        connection.close()
                        
        return result

    def is_done(self, task_id):
        """Returns ``True`` if task with ``task_id`` has been executed."""
        return self.get_status(task_id) == "DONE"

    def get_status(self, task_id):
        """Get the status of a task."""
        return self._get_task_meta_for(task_id)["status"]

    def _get_task_meta_for(self, task_id):
        results = []

        def callback(message_data, message):
            results.append(message_data)
            message.ack()

        routing_key = task_id.replace("-", "")

        connection = DjangoBrokerConnection()
        consumer = self._consumer_for_task_id(task_id, connection)
        consumer.register_callback(callback)
        
        try:
            consumer.iterconsume().next()
        finally:
            consumer.close()
            connection.close()

        return results[0]

    def get_result(self, task_id):
        """Get the result for a task."""
        result = self._get_task_meta_for(task_id) 
        if result["status"] == "FAILURE":
            return self.exception_to_python(result["result"])
        else:
            return result["result"]

    def cleanup(self):
        """Delete expired metadata."""
        pass
