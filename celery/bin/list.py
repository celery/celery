"""The ``celery list bindings`` command, used to inspect queue bindings."""
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
            return self.out(f'{q:<28} {e:<28} {r}')
        fmt('Queue', 'Exchange', 'Routing Key')
        fmt('-' * 16, '-' * 16, '-' * 16)
        for b in bindings:
            fmt(b['destination'], b['source'], b['routing_key'])

    def run(self, what=None, *_, **kw):
        topics = {'bindings': self.list_bindings}
        available = ', '.join(topics)
        if not what:
            raise self.UsageError(
                f'Missing argument, specify one of: {available}')
        if what not in topics:
            raise self.UsageError(
                'unknown topic {!r} (choose one of: {})'.format(
                    what, available))
        with self.app.connection() as conn:
            self.app.amqp.TaskConsumer(conn).declare()
            topics[what](conn.manager)
