"""The ``celery control``, ``. inspect`` and ``. status`` programs."""
from __future__ import absolute_import, unicode_literals
from kombu.utils.json import dumps
from kombu.utils.objects import cached_property
from celery.five import items, string_t
from celery.bin.base import Command
from celery.platforms import EX_UNAVAILABLE, EX_USAGE
from celery.utils import text


class _RemoteControl(Command):

    name = None
    leaf = False
    control_group = None

    def __init__(self, *args, **kwargs):
        self.show_body = kwargs.pop('show_body', True)
        self.show_reply = kwargs.pop('show_reply', True)
        super(_RemoteControl, self).__init__(*args, **kwargs)

    def add_arguments(self, parser):
        group = parser.add_argument_group('Remote Control Options')
        group.add_argument(
            '--timeout', '-t', type=float,
            help='Timeout in seconds (float) waiting for reply',
        )
        group.add_argument(
            '--destination', '-d',
            help='Comma separated list of destination node names.')
        group.add_argument(
            '--json', '-j', action='store_true', default=False,
            help='Use json as output format.',
        )

    @classmethod
    def get_command_info(cls, command,
                         indent=0, prefix='', color=None,
                         help=False, app=None, choices=None):
        if choices is None:
            choices = cls._choices_by_group(app)
        meta = choices[command]
        if help:
            help = '|' + text.indent(meta.help, indent + 4)
        else:
            help = None
        return text.join([
            '|' + text.indent('{0}{1} {2}'.format(
                prefix, color(command), meta.signature or ''), indent),
            help,
        ])

    @classmethod
    def list_commands(cls, indent=0, prefix='',
                      color=None, help=False, app=None):
        choices = cls._choices_by_group(app)
        color = color if color else lambda x: x
        prefix = prefix + ' ' if prefix else ''
        return '\n'.join(
            cls.get_command_info(c, indent, prefix, color, help,
                                 app=app, choices=choices)
            for c in sorted(choices))

    def usage(self, command):
        return '%(prog)s {0} [options] {1} <command> [arg1 .. argN]'.format(
            command, self.args)

    def call(self, *args, **kwargs):
        raise NotImplementedError('call')

    def run(self, *args, **kwargs):
        if not args:
            raise self.UsageError(
                'Missing {0.name} method.  See --help'.format(self))
        return self.do_call_method(args, **kwargs)

    def _ensure_fanout_supported(self):
        with self.app.connection_for_write() as conn:
            if not conn.supports_exchange_type('fanout'):
                raise self.Error(
                    'Broadcast not supported by transport {0!r}'.format(
                        conn.info()['transport']))

    def do_call_method(self, args,
                       timeout=None, destination=None, json=False, **kwargs):
        method = args[0]
        if method == 'help':
            raise self.Error("Did you mean '{0.name} --help'?".format(self))
        try:
            meta = self.choices[method]
        except KeyError:
            raise self.UsageError(
                'Unknown {0.name} method {1}'.format(self, method))

        self._ensure_fanout_supported()

        timeout = timeout or meta.default_timeout
        if destination and isinstance(destination, string_t):
            destination = [dest.strip() for dest in destination.split(',')]

        replies = self.call(
            method,
            arguments=self.compile_arguments(meta, method, args[1:]),
            timeout=timeout,
            destination=destination,
            callback=None if json else self.say_remote_command_reply,
        )
        if not replies:
            raise self.Error('No nodes replied within time constraint.',
                             status=EX_UNAVAILABLE)
        if json:
            self.out(dumps(replies))
        return replies

    def compile_arguments(self, meta, method, args):
        args = list(args)
        kw = {}
        if meta.args:
            kw.update({
                k: v for k, v in self._consume_args(meta, method, args)
            })
        if meta.variadic:
            kw.update({meta.variadic: args})
        if not kw and args:
            raise self.Error(
                'Command {0!r} takes no arguments.'.format(method),
                status=EX_USAGE)
        return kw or {}

    def _consume_args(self, meta, method, args):
        i = 0
        try:
            for i, arg in enumerate(args):
                try:
                    name, typ = meta.args[i]
                except IndexError:
                    if meta.variadic:
                        break
                    raise self.Error(
                        'Command {0!r} takes arguments: {1}'.format(
                            method, meta.signature),
                        status=EX_USAGE)
                else:
                    yield name, typ(arg) if typ is not None else arg
        finally:
            args[:] = args[i:]

    @classmethod
    def _choices_by_group(cls, app):
        from celery.worker.control import Panel
        # need to import task modules for custom user-remote control commands.
        app.loader.import_default_modules()

        return {
            name: info for name, info in items(Panel.meta)
            if info.type == cls.control_group and info.visible
        }

    @cached_property
    def choices(self):
        return self._choices_by_group(self.app)

    @property
    def epilog(self):
        return '\n'.join([
            '[Commands]',
            self.list_commands(indent=4, help=True, app=self.app)
        ])


class inspect(_RemoteControl):
    """Inspect the worker at runtime.

    Availability: RabbitMQ (AMQP) and Redis transports.

    Examples:
        .. code-block:: console

            $ celery inspect active --timeout=5
            $ celery inspect scheduled -d worker1@example.com
            $ celery inspect revoked -d w1@e.com,w2@e.com
    """

    name = 'inspect'
    control_group = 'inspect'

    def call(self, method, arguments, **options):
        return self.app.control.inspect(**options)._request(
            method, **arguments)


class control(_RemoteControl):
    """Workers remote control.

    Availability: RabbitMQ (AMQP), Redis, and MongoDB transports.

    Examples:
        .. code-block:: console

            $ celery control enable_events --timeout=5
            $ celery control -d worker1@example.com enable_events
            $ celery control -d w1.e.com,w2.e.com enable_events

            $ celery control -d w1.e.com add_consumer queue_name
            $ celery control -d w1.e.com cancel_consumer queue_name

            $ celery control add_consumer queue exchange direct rkey
    """

    name = 'control'
    control_group = 'control'

    def call(self, method, arguments, **options):
        return self.app.control.broadcast(
            method, arguments=arguments, reply=True, **options)


class status(Command):
    """Show list of workers that are online."""

    option_list = inspect.option_list

    def run(self, *args, **kwargs):
        I = inspect(
            app=self.app,
            no_color=kwargs.get('no_color', False),
            stdout=self.stdout, stderr=self.stderr,
            show_reply=False, show_body=False, quiet=True,
        )
        replies = I.run('ping', **kwargs)
        if not replies:
            raise self.Error('No nodes replied within time constraint',
                             status=EX_UNAVAILABLE)
        nodecount = len(replies)
        if not kwargs.get('quiet', False):
            self.out('\n{0} {1} online.'.format(
                nodecount, text.pluralize(nodecount, 'node')))
