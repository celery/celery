"""The ``celery list bindings`` command, used to inspect queue bindings."""
from __future__ import absolute_import, unicode_literals
from celery.bin.base import Command


class list_(Command):
    """Get info from broker.

    Note:
       For RabbitMQ the management plugin is required.

    Example:
        .. code-block:: console

            $ celery list bindings
    """

    args = '[bindings]'

    def list_bindings(self, management):
        try:
            bindings = management.get_bindings()
        except NotImplementedError:
            raise self.Error('Your transport cannot list bindings.')

        def fmt(q, e, r):
            return self.out('{0:<28} {1:<28} {2}'.format(q, e, r))
        fmt('Queue', 'Exchange', 'Routing Key')
        fmt('-' * 16, '-' * 16, '-' * 16)
        for b in bindings:
            fmt(b['destination'], b['source'], b['routing_key'])

    def run(self, what=None, *_, **kw):
        topics = {'bindings': self.list_bindings}
        available = ', '.join(topics)
        if not what:
            raise self.UsageError(
                'Missing argument, specify one of: {0}'.format(available))
        if what not in topics:
            raise self.UsageError(
                'unknown topic {0!r} (choose one of: {1})'.format(
                    what, available))
        with self.app.connection() as conn:
            self.app.amqp.TaskConsumer(conn).declare()
            topics[what](conn.manager)
