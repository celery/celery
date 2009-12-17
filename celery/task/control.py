from celery import conf
from celery.messaging import TaskConsumer, BroadcastPublisher, with_connection


@with_connection
def discard_all(connection=None, connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
    """Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    :returns: the number of tasks discarded.

    """
    consumer = TaskConsumer(connection=connection)
    try:
        return consumer.discard_all()
    finally:
        consumer.close()


@with_connection
def revoke(task_id, connection=None,
        connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
    """Revoke a task by id.

    If a task is revoked, the workers will ignore the task and not execute
    it after all.

    """
    broadcast = BroadcastPublisher(connection)
    try:
        broadcast.revoke(task_id)
    finally:
        broadcast.close()
