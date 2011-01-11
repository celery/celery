from kombu.pidbox import Mailbox

from celery.app import app_or_default


def flatten_reply(reply):
    nodes = {}
    for item in reply:
        nodes.update(item)
    return nodes


class Inspect(object):

    def __init__(self, control, destination=None, timeout=1, callback=None,):
        self.destination = destination
        self.timeout = timeout
        self.callback = callback
        self.control = control

    def _prepare(self, reply):
        if not reply:
            return
        by_node = flatten_reply(reply)
        if self.destination and \
                not isinstance(self.destination, (list, tuple)):
            return by_node.get(self.destination)
        return by_node

    def _request(self, command, **kwargs):
        return self._prepare(self.control.broadcast(command,
                                      arguments=kwargs,
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

    def ping(self):
        return self._request("ping")

    def add_consumer(self, queue, exchange=None, exchange_type="direct",
            routing_key=None, **options):
        return self._request("add_consumer", queue=queue, exchange=exchange,
                             exchange_type=exchange_type,
                             routing_key=routing_key, **options)

    def cancel_consumer(self, queue, **kwargs):
        return self._request("cancel_consumer", queue=queue, **kwargs)

    def active_queues(self):
        return self._request("active_queues")


class Control(object):
    Mailbox = Mailbox

    def __init__(self, app):
        self.app = app
        self.mailbox = self.Mailbox("celeryd", type="fanout")

    def inspect(self, destination=None, timeout=1, callback=None):
        return Inspect(self, destination=destination, timeout=timeout,
                             callback=callback)

    def discard_all(self, connection=None, connect_timeout=None):
        """Discard all waiting tasks.

        This will ignore all tasks waiting for execution, and they will
        be deleted from the messaging server.

        :returns: the number of tasks discarded.

        """

        def _do_discard(connection=None, connect_timeout=None):
            consumer = self.app.amqp.get_task_consumer(connection=connection)
            try:
                return consumer.discard_all()
            finally:
                consumer.close()

        return self.app.with_default_connection(_do_discard)(
                connection=connection, connect_timeout=connect_timeout)

    def revoke(self, task_id, destination=None, terminate=False,
            signal="SIGTERM", **kwargs):
        """Revoke a task by id.

        If a task is revoked, the workers will ignore the task and
        not execute it after all.

        :param task_id: Id of the task to revoke.
        :keyword terminate: Also terminate the process currently working
            on the task (if any).
        :keyword signal: Name of signal to send to process if terminate.
            Default is TERM.
        :keyword destination: If set, a list of the hosts to send the
            command to, when empty broadcast to all workers.
        :keyword connection: Custom broker connection to use, if not set,
            a connection will be established automatically.
        :keyword connect_timeout: Timeout for new connection if a custom
            connection is not provided.
        :keyword reply: Wait for and return the reply.
        :keyword timeout: Timeout in seconds to wait for the reply.
        :keyword limit: Limit number of replies.

        """
        return self.broadcast("revoke", destination=destination,
                              arguments={"task_id": task_id,
                                         "terminate": terminate,
                                         "signal": signal}, **kwargs)

    def ping(self, destination=None, timeout=1, **kwargs):
        """Ping workers.

        Returns answer from alive workers.

        :keyword destination: If set, a list of the hosts to send the
            command to, when empty broadcast to all workers.
        :keyword connection: Custom broker connection to use, if not set,
            a connection will be established automatically.
        :keyword connect_timeout: Timeout for new connection if a custom
            connection is not provided.
        :keyword reply: Wait for and return the reply.
        :keyword timeout: Timeout in seconds to wait for the reply.
        :keyword limit: Limit number of replies.

        """
        return self.broadcast("ping", reply=True, destination=destination,
                              timeout=timeout, **kwargs)

    def rate_limit(self, task_name, rate_limit, destination=None, **kwargs):
        """Set rate limit for task by type.

        :param task_name: Type of task to change rate limit for.
        :param rate_limit: The rate limit as tasks per second, or a rate limit
            string (`"100/m"`, etc.
            see :attr:`celery.task.base.Task.rate_limit` for
            more information).
        :keyword destination: If set, a list of the hosts to send the
            command to, when empty broadcast to all workers.
        :keyword connection: Custom broker connection to use, if not set,
            a connection will be established automatically.
        :keyword connect_timeout: Timeout for new connection if a custom
            connection is not provided.
        :keyword reply: Wait for and return the reply.
        :keyword timeout: Timeout in seconds to wait for the reply.
        :keyword limit: Limit number of replies.

        """
        return self.broadcast("rate_limit", destination=destination,
                              arguments={"task_name": task_name,
                                         "rate_limit": rate_limit},
                              **kwargs)

    def broadcast(self, command, arguments=None, destination=None,
            connection=None, connect_timeout=None, reply=False, timeout=1,
            limit=None, callback=None, channel=None):
        """Broadcast a control command to the celery workers.

        :param command: Name of command to send.
        :param arguments: Keyword arguments for the command.
        :keyword destination: If set, a list of the hosts to send the
            command to, when empty broadcast to all workers.
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
        def _do_broadcast(connection=None, connect_timeout=None,
                          channel=None):
            return self.mailbox(connection)._broadcast(command, arguments,
                                                       destination, reply,
                                                       timeout, limit,
                                                       callback,
                                                       channel=channel)

        if channel:
            return _do_broadcast(connection, connect_timeout, channel)
        else:
            return self.app.with_default_connection(_do_broadcast)(
                    connection=connection, connect_timeout=connect_timeout)


_default_control = Control(app_or_default())
broadcast = _default_control.broadcast
rate_limit = _default_control.rate_limit
ping = _default_control.ping
revoke = _default_control.revoke
discard_all = _default_control.discard_all
inspect = _default_control.inspect
