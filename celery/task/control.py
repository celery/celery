from celery import conf
from celery.utils import gen_unique_id
from celery.messaging import BroadcastPublisher, ControlReplyConsumer
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
    try:
        return consumers.discard_all()
    finally:
        consumers.close()


def revoke(task_id, destination=None, **kwargs):
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
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    """
    return broadcast("revoke", destination=destination,
                               arguments={"task_id": task_id}, **kwargs)


def ping(destination=None, timeout=1, **kwargs):
    """Ping workers.

    Returns answer from alive workers.

    :keyword destination: If set, a list of the hosts to send the command to,
        when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword connect_timeout: Timeout for new connection if a custom
        connection is not provided.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    """
    return broadcast("ping", reply=True, destination=destination,
                     timeout=timeout, **kwargs)


def rate_limit(task_name, rate_limit, destination=None, **kwargs):
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
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.

    """
    return broadcast("rate_limit", destination=destination,
                                   arguments={"task_name": task_name,
                                              "rate_limit": rate_limit},
                                   **kwargs)


def flatten_reply(reply):
    nodes = {}
    for item in reply:
        nodes.update(item)
    return nodes


class inspect(object):

    def __init__(self, destination=None, timeout=1, callback=None):
        self.destination = destination
        self.timeout = timeout
        self.callback = callback

    def _prepare(self, reply):
        if not reply:
            return
        by_node = flatten_reply(reply)
        if self.destination and \
                not isinstance(self.destination, (list, tuple)):
            return by_node.get(self.destination)
        return by_node

    def _request(self, command, **kwargs):
        return self._prepare(broadcast(command, arguments=kwargs,
                                      destination=self.destination,
                                      callback=self.callback,
                                      timeout=self.timeout, reply=True))

    def active(self, safe=False):
        return self._request("dump_active", safe=safe)

    def scheduled(self, safe=False):
        return self._request("dump_schedule", safe=safe)

    def reserved(self, safe=False):
        return self._request("dump_reserved", safe=safe)

    def stats(self):
        return self._request("stats")

    def revoked(self):
        return self._request("dump_revoked")

    def registered_tasks(self):
        return self._request("dump_tasks")

    def enable_events(self):
        return self._request("enable_events")

    def disable_events(self):
        return self._request("disable_events")

    def diagnose(self):
        diagnose_timeout = self.timeout * 0.85              # 15% of timeout
        return self._request("diagnose", timeout=diagnose_timeout)

    def ping(self):
        return self._request("ping")


@with_connection
def broadcast(command, arguments=None, destination=None, connection=None,
        connect_timeout=conf.BROKER_CONNECTION_TIMEOUT, reply=False,
        timeout=1, limit=None, callback=None):
    """Broadcast a control command to the celery workers.

    :param command: Name of command to send.
    :param arguments: Keyword arguments for the command.
    :keyword destination: If set, a list of the hosts to send the command to,
        when empty broadcast to all workers.
    :keyword connection: Custom broker connection to use, if not set,
        a connection will be established automatically.
    :keyword connect_timeout: Timeout for new connection if a custom
        connection is not provided.
    :keyword reply: Wait for and return the reply.
    :keyword timeout: Timeout in seconds to wait for the reply.
    :keyword limit: Limit number of replies.
    :keyword callback: Callback called immediately for each reply
        received.

    """
    arguments = arguments or {}
    reply_ticket = reply and gen_unique_id() or None

    if destination is not None and not isinstance(destination, (list, tuple)):
        raise ValueError("destination must be a list/tuple not %s" % (
                type(destination)))

    # Set reply limit to number of destinations (if specificed)
    if limit is None and destination:
        limit = destination and len(destination) or None

    broadcast = BroadcastPublisher(connection)
    try:
        broadcast.send(command, arguments, destination=destination,
                       reply_ticket=reply_ticket)
    finally:
        broadcast.close()

    if reply_ticket:
        crq = ControlReplyConsumer(connection, reply_ticket)
        try:
            return crq.collect(limit=limit, timeout=timeout,
                               callback=callback)
        finally:
            crq.close()
