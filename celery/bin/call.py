"""The ``celery call`` program used to send tasks from the command-line."""
from __future__ import absolute_import, unicode_literals

from kombu.utils.json import loads

from celery.bin.base import Command
from celery.five import string_t
from celery.utils.time import maybe_iso8601


class call(Command):
    """Call a task by name.

    Examples:
        .. code-block:: console

            $ celery call tasks.add --args='[2, 2]'
            $ celery call tasks.add --args='[2, 2]' --countdown=10
    """

    args = '<task_name>'

    # since we have an argument --args, we need to name this differently.
    args_name = 'posargs'

    def add_arguments(self, parser):
        group = parser.add_argument_group('Calling Options')
        group.add_argument('--args', '-a',
                           help='positional arguments (json).')
        group.add_argument('--kwargs', '-k',
                           help='keyword arguments (json).')
        group.add_argument('--eta',
                           help='scheduled time (ISO-8601).')
        group.add_argument(
            '--countdown', type=float,
            help='eta in seconds from now (float/int).',
        )
        group.add_argument(
            '--expires',
            help='expiry time (ISO-8601/float/int).',
        ),
        group.add_argument(
            '--serializer', default='json',
            help='defaults to json.'),

        ropts = parser.add_argument_group('Routing Options')
        ropts.add_argument('--queue', help='custom queue name.')
        ropts.add_argument('--exchange', help='custom exchange name.')
        ropts.add_argument('--routing-key', help='custom routing key.')

    def run(self, name, *_, **kwargs):
        self._send_task(name, **kwargs)

    def _send_task(self, name, args=None, kwargs=None,
                   countdown=None, serializer=None,
                   queue=None, exchange=None, routing_key=None,
                   eta=None, expires=None, **_):
        # arguments
        args = loads(args) if isinstance(args, string_t) else args
        kwargs = loads(kwargs) if isinstance(kwargs, string_t) else kwargs

        # Expires can be int/float.
        try:
            expires = float(expires)
        except (TypeError, ValueError):
            # or a string describing an ISO 8601 datetime.
            try:
                expires = maybe_iso8601(expires)
            except (TypeError, ValueError):
                raise

        # send the task and print the id.
        self.out(self.app.send_task(
            name,
            args=args or (), kwargs=kwargs or {},
            countdown=countdown,
            serializer=serializer,
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            eta=maybe_iso8601(eta),
            expires=expires,
        ).id)
