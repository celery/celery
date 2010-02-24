from celery import conf
from celery.messaging import BroadcastPublisher
from celery.messaging import with_connection, get_consumer_set


@with_connection
def discard_all(connection=None,
        connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
    """Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    :returns: the number of tasks discarded.

    """
    consumers = get_consumer_set(connection=connection)
    did_exc = None
    try:
        return consumers.discard_all()
    except Exception, e:
        did_exc = e

    consumers.close()

    if did_exc:
        raise did_exc


def revoke(task_id, destination=None, connection=None,
        connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
    """Revoke a task by id.

    If a task is revoked, the workers will ignore the task and not execute
    it after all.

    :param task_id: Id of the task to revoke.
    :keyword destination: If set, a list of the hosts to send the command to,
        when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword connect_timeout: Timeout for new connection if a custom
        connection is not provided.

    """
    return broadcast("revoke", destination=destination,
                               arguments={"task_id": task_id})


def rate_limit(task_name, rate_limit, destination=None, connection=None,
        connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
    """Set rate limit for task by type.

    :param task_name: Type of task to change rate limit for.
    :param rate_limit: The rate limit as tasks per second, or a rate limit
      string (``"100/m"``, etc. see :attr:`celery.task.base.Task.rate_limit`
      for more information).
    :keyword destination: If set, a list of the hosts to send the command to,
        when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword connect_timeout: Timeout for new connection if a custom
        connection is not provided.

    """
    return broadcast("rate_limit", destination=destination,
                                   arguments={"task_name": task_name,
                                              "rate_limit": rate_limit})


@with_connection
def broadcast(command, arguments=None, destination=None, connection=None,
        connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
    """Broadcast a control command to the celery workers.

    :param command: Name of command to send.
    :param arguments: Keyword arguments for the command.
    :keyword destination: If set, a list of the hosts to send the command to,
        when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword connect_timeout: Timeout for new connection if a custom
        connection is not provided.

    """
    arguments = arguments or {}

    broadcast = BroadcastPublisher(connection)
    did_exc = None
    try:
        broadcast.send(command, arguments, destination=destination)
    except Exception, e:
        did_exc = e

    broadcast.close()
    
    if did_exc:
        raise did_exc

