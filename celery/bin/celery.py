# -*- coding: utf-8 -*-
"""The :program:`celery` umbrella command.

.. program:: celery

.. _preload-options:

Preload Options
---------------

These options are supported by all commands,
and usually parsed before command-specific arguments.

.. cmdoption:: -A, --app

    app instance to use (e.g., ``module.attr_name``)

.. cmdoption:: -b, --broker

    URL to broker.  default is ``amqp://guest@localhost//``

.. cmdoption:: --loader

    name of custom loader class to use.

.. cmdoption:: --config

    Name of the configuration module

.. cmdoption:: -C, --no-color

    Disable colors in output.

.. cmdoption:: -q, --quiet

    Give less verbose output (behavior depends on the sub command).

.. cmdoption:: --help

    Show help and exit.

.. _daemon-options:

Daemon Options
--------------

These options are supported by commands that can detach
into the background (daemon).  They will be present
in any command that also has a `--detach` option.

.. cmdoption:: -f, --logfile

    Path to log file.  If no logfile is specified, `stderr` is used.

.. cmdoption:: --pidfile

    Optional file used to store the process pid.

    The program won't start if this file already exists
    and the pid is still alive.

.. cmdoption:: --uid

    User id, or user name of the user to run as after detaching.

.. cmdoption:: --gid

    Group id, or group name of the main group to change to after
    detaching.

.. cmdoption:: --umask

    Effective umask (in octal) of the process after detaching.  Inherits
    the umask of the parent process by default.

.. cmdoption:: --workdir

    Optional directory to change to after detaching.

.. cmdoption:: --executable

    Executable to use for the detached process.

``celery inspect``
------------------

.. program:: celery inspect

.. cmdoption:: -t, --timeout

    Timeout in seconds (float) waiting for reply

.. cmdoption:: -d, --destination

    Comma separated list of destination node names.

.. cmdoption:: -j, --json

    Use json as output format.

``celery control``
------------------

.. program:: celery control

.. cmdoption:: -t, --timeout

    Timeout in seconds (float) waiting for reply

.. cmdoption:: -d, --destination

    Comma separated list of destination node names.

.. cmdoption:: -j, --json

    Use json as output format.

``celery migrate``
------------------

.. program:: celery migrate

.. cmdoption:: -n, --limit

    Number of tasks to consume (int).

.. cmdoption:: -t, -timeout

    Timeout in seconds (float) waiting for tasks.

.. cmdoption:: -a, --ack-messages

    Ack messages from source broker.

.. cmdoption:: -T, --tasks

    List of task names to filter on.

.. cmdoption:: -Q, --queues

    List of queues to migrate.

.. cmdoption:: -F, --forever

    Continually migrate tasks until killed.

``celery upgrade``
------------------

.. program:: celery upgrade

.. cmdoption:: --django

    Upgrade a Django project.

.. cmdoption:: --compat

    Maintain backwards compatibility.

.. cmdoption:: --no-backup

    Don't backup original files.

``celery shell``
----------------

.. program:: celery shell

.. cmdoption:: -I, --ipython

    Force :pypi:`iPython` implementation.

.. cmdoption:: -B, --bpython

    Force :pypi:`bpython` implementation.

.. cmdoption:: -P, --python

    Force default Python shell.

.. cmdoption:: -T, --without-tasks

    Don't add tasks to locals.

.. cmdoption:: --eventlet

    Use :pypi:`eventlet` monkey patches.

.. cmdoption:: --gevent

    Use :pypi:`gevent` monkey patches.


``celery result``
-----------------

.. program:: celery result

.. cmdoption:: -t, --task

    Name of task (if custom backend).

.. cmdoption:: --traceback

    Show traceback if any.

``celery purge``
----------------

.. program:: celery purge

.. cmdoption:: -f, --force

    Don't prompt for verification before deleting messages (DANGEROUS)

``celery call``
---------------

.. program:: celery call

.. cmdoption:: -a, --args

    Positional arguments (json format).

.. cmdoption:: -k, --kwargs

    Keyword arguments (json format).

.. cmdoption:: --eta

    Scheduled time in ISO-8601 format.

.. cmdoption:: --countdown

    ETA in seconds from now (float/int).

.. cmdoption:: --expires

    Expiry time in float/int seconds, or a ISO-8601 date.

.. cmdoption:: --serializer

    Specify serializer to use (default is json).

.. cmdoption:: --queue

    Destination queue.

.. cmdoption:: --exchange

    Destination exchange (defaults to the queue exchange).

.. cmdoption:: --routing-key

    Destination routing key (defaults to the queue routing key).
"""
from __future__ import absolute_import, unicode_literals, print_function

import codecs
import numbers
import os
import sys

from functools import partial
from importlib import import_module

from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property

from celery.app import defaults
from celery.five import items, keys, string_t, values
from celery.platforms import EX_OK, EX_FAILURE, EX_UNAVAILABLE, EX_USAGE
from celery.utils import term
from celery.utils import text
from celery.utils.functional import pass1
from celery.utils.text import str_to_list
from celery.utils.time import maybe_iso8601

# Cannot use relative imports here due to a Windows issue (#1111).
from celery.bin.base import Command, Extensions

# Import commands from other modules
from celery.bin.amqp import amqp
from celery.bin.beat import beat
from celery.bin.events import events
from celery.bin.graph import graph
from celery.bin.logtool import logtool
from celery.bin.worker import worker

__all__ = ['CeleryCommand', 'main']

HELP = """
---- -- - - ---- Commands- -------------- --- ------------

{commands}
---- -- - - --------- -- - -------------- --- ------------

Type '{prog_name} <command> --help' for help using a specific command.
"""

MIGRATE_PROGRESS_FMT = """\
Migrating task {state.count}/{state.strtotal}: \
{body[task]}[{body[id]}]\
"""

command_classes = [
    ('Main', ['worker', 'events', 'beat', 'shell', 'multi', 'amqp'], 'green'),
    ('Remote Control', ['status', 'inspect', 'control'], 'blue'),
    ('Utils',
     ['purge', 'list', 'call', 'result', 'migrate', 'graph', 'upgrade'],
     None),
    ('Debugging', ['report', 'logtool'], 'red'),
]


def determine_exit_status(ret):
    if isinstance(ret, numbers.Integral):
        return ret
    return EX_OK if ret else EX_FAILURE


def main(argv=None):
    """Start celery umbrella command."""
    # Fix for setuptools generated scripts, so that it will
    # work with multiprocessing fork emulation.
    # (see multiprocessing.forking.get_preparation_data())
    try:
        if __name__ != '__main__':  # pragma: no cover
            sys.modules['__main__'] = sys.modules[__name__]
        cmd = CeleryCommand()
        cmd.maybe_patch_concurrency()
        from billiard import freeze_support
        freeze_support()
        cmd.execute_from_commandline(argv)
    except KeyboardInterrupt:
        pass


class multi(Command):
    """Start multiple worker instances."""

    respects_app_option = False

    def run_from_argv(self, prog_name, argv, command=None):
        from celery.bin.multi import MultiTool
        cmd = MultiTool(quiet=self.quiet, no_color=self.no_color)
        return cmd.execute_from_commandline([command] + argv)


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


class call(Command):
    """Call a task by name.

    Examples:
        .. code-block:: console

            $ celery call tasks.add --args='[2, 2]'
            $ celery call tasks.add --args='[2, 2]' --countdown=10
    """

    args = '<task_name>'

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


class purge(Command):
    """Erase all messages from all known task queues.

    Warning:
        There's no undo operation for this command.
    """

    warn_prelude = (
        '{warning}: This will remove all tasks from {queues}: {names}.\n'
        '         There is no undo for this operation!\n\n'
        '(to skip this prompt use the -f option)\n'
    )
    warn_prompt = 'Are you sure you want to delete all tasks'

    fmt_purged = 'Purged {mnum} {messages} from {qnum} known task {queues}.'
    fmt_empty = 'No messages purged from {qnum} {queues}'

    def add_arguments(self, parser):
        group = parser.add_argument_group('Purging Options')
        group.add_argument(
            '--force', '-f', action='store_true',
            help="Don't prompt for verification",
        )
        group.add_argument(
            '--queues', '-Q', default=[],
            help='Comma separated list of queue names to purge.',
        )
        group.add_argument(
            '--exclude-queues', '-X', default=[],
            help='Comma separated list of queues names not to purge.',
        )

    def run(self, force=False, queues=None, exclude_queues=None, **kwargs):
        queues = set(str_to_list(queues or []))
        exclude = set(str_to_list(exclude_queues or []))
        names = (queues or set(keys(self.app.amqp.queues))) - exclude
        qnum = len(names)

        messages = None
        if names:
            if not force:
                self.out(self.warn_prelude.format(
                    warning=self.colored.red('WARNING'),
                    queues=text.pluralize(qnum, 'queue'),
                    names=', '.join(sorted(names)),
                ))
                if self.ask(self.warn_prompt, ('yes', 'no'), 'no') != 'yes':
                    return
            with self.app.connection_for_write() as conn:
                messages = sum(self._purge(conn, queue) for queue in names)
        fmt = self.fmt_purged if messages else self.fmt_empty
        self.out(fmt.format(
            mnum=messages, qnum=qnum,
            messages=text.pluralize(messages, 'message'),
            queues=text.pluralize(qnum, 'queue')))

    def _purge(self, conn, queue):
        try:
            return conn.default_channel.queue_purge(queue) or 0
        except conn.channel_errors:
            return 0


class result(Command):
    """Gives the return value for a given task id.

    Examples:
        .. code-block:: console

            $ celery result 8f511516-e2f5-4da4-9d2f-0fb83a86e500
            $ celery result 8f511516-e2f5-4da4-9d2f-0fb83a86e500 -t tasks.add
            $ celery result 8f511516-e2f5-4da4-9d2f-0fb83a86e500 --traceback
    """

    args = '<task_id>'

    def add_arguments(self, parser):
        group = parser.add_argument_group('Result Options')
        group.add_argument(
            '--task', '-t', help='name of task (if custom backend)',
        )
        group.add_argument(
            '--traceback', action='store_true',
            help='show traceback instead',
        )

    def run(self, task_id, *args, **kwargs):
        result_cls = self.app.AsyncResult
        task = kwargs.get('task')
        traceback = kwargs.get('traceback', False)

        if task:
            result_cls = self.app.tasks[task].AsyncResult
        task_result = result_cls(task_id)
        if traceback:
            value = task_result.traceback
        else:
            value = task_result.get()
        self.out(self.pretty(value)[1])


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
            '--json', '-j', action='store_true',
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


class migrate(Command):
    """Migrate tasks from one broker to another.

    Warning:
        This command is experimental, make sure you have a backup of
        the tasks before you continue.

    Example:
        .. code-block:: console

            $ celery migrate amqp://A.example.com amqp://guest@B.example.com//
            $ celery migrate redis://localhost amqp://guest@localhost//
    """

    args = '<source_url> <dest_url>'
    progress_fmt = MIGRATE_PROGRESS_FMT

    def add_arguments(self, parser):
        group = parser.add_argument_group('Migration Options')
        group.add_argument(
            '--limit', '-n', type=int,
            help='Number of tasks to consume (int)',
        )
        group.add_argument(
            '--timeout', '-t', type=float, default=1.0,
            help='Timeout in seconds (float) waiting for tasks',
        )
        group.add_argument(
            '--ack-messages', '-a', action='store_true',
            help='Ack messages from source broker.',
        )
        group.add_argument(
            '--tasks', '-T',
            help='List of task names to filter on.',
        )
        group.add_argument(
            '--queues', '-Q',
            help='List of queues to migrate.',
        )
        group.add_argument(
            '--forever', '-F', action='store_true',
            help='Continually migrate tasks until killed.',
        )

    def on_migrate_task(self, state, body, message):
        self.out(self.progress_fmt.format(state=state, body=body))

    def run(self, source, destination, **kwargs):
        from kombu import Connection
        from celery.contrib.migrate import migrate_tasks

        migrate_tasks(Connection(source),
                      Connection(destination),
                      callback=self.on_migrate_task,
                      **kwargs)


class shell(Command):  # pragma: no cover
    """Start shell session with convenient access to celery symbols.

    The following symbols will be added to the main globals:

        - ``celery``:  the current application.
        - ``chord``, ``group``, ``chain``, ``chunks``,
          ``xmap``, ``xstarmap`` ``subtask``, ``Task``
        - all registered tasks.
    """

    def add_arguments(self, parser):
        group = parser.add_argument_group('Shell Options')
        group.add_argument(
            '--ipython', '-I',
            action='store_true', help='force iPython.',
        )
        group.add_argument(
            '--bpython', '-B',
            action='store_true', help='force bpython.',
        )
        group.add_argument(
            '--python',
            action='store_true', help='force default Python shell.',
        )
        group.add_argument(
            '--without-tasks', '-T',
            action='store_true', help="don't add tasks to locals.",
        )
        group.add_argument(
            '--eventlet',
            action='store_true', help='use eventlet.',
        )
        group.add_argument(
            '--gevent', action='store_true', help='use gevent.',
        )

    def run(self, *args, **kwargs):
        if args:
            raise self.UsageError(
                'shell command does not take arguments: {0}'.format(args))
        return self._run(**kwargs)

    def _run(self, ipython=False, bpython=False,
             python=False, without_tasks=False, eventlet=False,
             gevent=False, **kwargs):
        sys.path.insert(0, os.getcwd())
        if eventlet:
            import_module('celery.concurrency.eventlet')
        if gevent:
            import_module('celery.concurrency.gevent')
        import celery
        import celery.task.base
        self.app.loader.import_default_modules()

        # pylint: disable=attribute-defined-outside-init
        self.locals = {
            'app': self.app,
            'celery': self.app,
            'Task': celery.Task,
            'chord': celery.chord,
            'group': celery.group,
            'chain': celery.chain,
            'chunks': celery.chunks,
            'xmap': celery.xmap,
            'xstarmap': celery.xstarmap,
            'subtask': celery.subtask,
            'signature': celery.signature,
        }

        if not without_tasks:
            self.locals.update({
                task.__name__: task for task in values(self.app.tasks)
                if not task.name.startswith('celery.')
            })

        if python:
            return self.invoke_fallback_shell()
        elif bpython:
            return self.invoke_bpython_shell()
        elif ipython:
            return self.invoke_ipython_shell()
        return self.invoke_default_shell()

    def invoke_default_shell(self):
        try:
            import IPython  # noqa
        except ImportError:
            try:
                import bpython  # noqa
            except ImportError:
                return self.invoke_fallback_shell()
            else:
                return self.invoke_bpython_shell()
        else:
            return self.invoke_ipython_shell()

    def invoke_fallback_shell(self):
        import code
        try:
            import readline
        except ImportError:
            pass
        else:
            import rlcompleter
            readline.set_completer(
                rlcompleter.Completer(self.locals).complete)
            readline.parse_and_bind('tab:complete')
        code.interact(local=self.locals)

    def invoke_ipython_shell(self):
        for ip in (self._ipython, self._ipython_pre_10,
                   self._ipython_terminal, self._ipython_010,
                   self._no_ipython):
            try:
                return ip()
            except ImportError:
                pass

    def _ipython(self):
        from IPython import start_ipython
        start_ipython(argv=[], user_ns=self.locals)

    def _ipython_pre_10(self):  # pragma: no cover
        from IPython.frontend.terminal.ipapp import TerminalIPythonApp
        app = TerminalIPythonApp.instance()
        app.initialize(argv=[])
        app.shell.user_ns.update(self.locals)
        app.start()

    def _ipython_terminal(self):  # pragma: no cover
        from IPython.terminal import embed
        embed.TerminalInteractiveShell(user_ns=self.locals).mainloop()

    def _ipython_010(self):  # pragma: no cover
        from IPython.Shell import IPShell
        IPShell(argv=[], user_ns=self.locals).mainloop()

    def _no_ipython(self):  # pragma: no cover
        raise ImportError('no suitable ipython found')

    def invoke_bpython_shell(self):
        import bpython
        bpython.embed(self.locals)


class upgrade(Command):
    """Perform upgrade between versions."""

    choices = {'settings'}

    def add_arguments(self, parser):
        group = parser.add_argument_group('Upgrading Options')
        group.add_argument(
            '--django', action='store_true',
            help='Upgrade Django project',
        )
        group.add_argument(
            '--compat', action='store_true',
            help='Maintain backwards compatibility',
        )
        group.add_argument(
            '--no-backup', action='store_true',
            help='Dont backup original files',
        )

    def usage(self, command):
        return '%(prog)s <command> settings [filename] [options]'

    def run(self, *args, **kwargs):
        try:
            command = args[0]
        except IndexError:
            raise self.UsageError('missing upgrade type')
        if command not in self.choices:
            raise self.UsageError('unknown upgrade type: {0}'.format(command))
        return getattr(self, command)(*args, **kwargs)

    def settings(self, command, filename,
                 no_backup=False, django=False, compat=False, **kwargs):
        lines = self._slurp(filename) if no_backup else self._backup(filename)
        keyfilter = self._compat_key if django or compat else pass1
        print('processing {0}...'.format(filename), file=self.stderr)
        with codecs.open(filename, 'w', 'utf-8') as write_fh:
            for line in lines:
                write_fh.write(self._to_new_key(line, keyfilter))

    def _slurp(self, filename):
        with codecs.open(filename, 'r', 'utf-8') as read_fh:
            return [line for line in read_fh]

    def _backup(self, filename, suffix='.orig'):
        lines = []
        backup_filename = ''.join([filename, suffix])
        print('writing backup to {0}...'.format(backup_filename),
              file=self.stderr)
        with codecs.open(filename, 'r', 'utf-8') as read_fh:
            with codecs.open(backup_filename, 'w', 'utf-8') as backup_fh:
                for line in read_fh:
                    backup_fh.write(line)
                    lines.append(line)
        return lines

    def _to_new_key(self, line, keyfilter=pass1, source=defaults._TO_NEW_KEY):
        # sort by length to avoid, for example, broker_transport overriding
        # broker_transport_options.
        for old_key in reversed(sorted(source, key=lambda x: len(x))):
            new_line = line.replace(old_key, keyfilter(source[old_key]))
            if line != new_line:
                return new_line  # only one match per line.
        return line

    def _compat_key(self, key, namespace='CELERY'):
        key = key.upper()
        if not key.startswith(namespace):
            key = '_'.join([namespace, key])
        return key


class help(Command):
    """Show help screen and exit."""

    def usage(self, command):
        return '%(prog)s <command> [options] {0.args}'.format(self)

    def run(self, *args, **kwargs):
        self.parser.print_help()
        self.out(HELP.format(
            prog_name=self.prog_name,
            commands=CeleryCommand.list_commands(
                colored=self.colored, app=self.app),
        ))

        return EX_USAGE


class report(Command):
    """Shows information useful to include in bug-reports."""

    def run(self, *args, **kwargs):
        self.out(self.app.bugreport())
        return EX_OK


class CeleryCommand(Command):
    """Base class for commands."""

    commands = {
        'amqp': amqp,
        'beat': beat,
        'call': call,
        'control': control,
        'events': events,
        'graph': graph,
        'help': help,
        'inspect': inspect,
        'list': list_,
        'logtool': logtool,
        'migrate': migrate,
        'multi': multi,
        'purge': purge,
        'report': report,
        'result': result,
        'shell': shell,
        'status': status,
        'upgrade': upgrade,
        'worker': worker,
    }
    ext_fmt = '{self.namespace}.commands'
    enable_config_from_cmdline = True
    prog_name = 'celery'
    namespace = 'celery'

    @classmethod
    def register_command(cls, fun, name=None):
        cls.commands[name or fun.__name__] = fun
        return fun

    def execute(self, command, argv=None):
        try:
            cls = self.commands[command]
        except KeyError:
            cls, argv = self.commands['help'], ['help']
        cls = self.commands.get(command) or self.commands['help']
        try:
            return cls(
                app=self.app, on_error=self.on_error,
                no_color=self.no_color, quiet=self.quiet,
                on_usage_error=partial(self.on_usage_error, command=command),
            ).run_from_argv(self.prog_name, argv[1:], command=argv[0])
        except self.UsageError as exc:
            self.on_usage_error(exc)
            return exc.status
        except self.Error as exc:
            self.on_error(exc)
            return exc.status

    def on_usage_error(self, exc, command=None):
        if command:
            helps = '{self.prog_name} {command} --help'
        else:
            helps = '{self.prog_name} --help'
        self.error(self.colored.magenta('Error: {0}'.format(exc)))
        self.error("""Please try '{0}'""".format(helps.format(
            self=self, command=command,
        )))

    def _relocate_args_from_start(self, argv, index=0):
        if argv:
            rest = []
            while index < len(argv):
                value = argv[index]
                if value.startswith('--'):
                    rest.append(value)
                elif value.startswith('-'):
                    # we eat the next argument even though we don't know
                    # if this option takes an argument or not.
                    # instead we'll assume what's the command name in the
                    # return statements below.
                    try:
                        nxt = argv[index + 1]
                        if nxt.startswith('-'):
                            # is another option
                            rest.append(value)
                        else:
                            # is (maybe) a value for this option
                            rest.extend([value, nxt])
                            index += 1
                    except IndexError:  # pragma: no cover
                        rest.append(value)
                        break
                else:
                    break
                index += 1
            if argv[index:]:  # pragma: no cover
                # if there are more arguments left then divide and swap
                # we assume the first argument in argv[i:] is the command
                # name.
                return argv[index:] + rest
            # if there are no more arguments then the last arg in rest'
            # must be the command.
            [rest.pop()] + rest
        return []

    def prepare_prog_name(self, name):
        if name == '__main__.py':
            return sys.modules['__main__'].__file__
        return name

    def handle_argv(self, prog_name, argv, **kwargs):
        self.prog_name = self.prepare_prog_name(prog_name)
        argv = self._relocate_args_from_start(argv)
        _, argv = self.prepare_args(None, argv)
        try:
            command = argv[0]
        except IndexError:
            command, argv = 'help', ['help']
        return self.execute(command, argv)

    def execute_from_commandline(self, argv=None):
        argv = sys.argv if argv is None else argv
        if 'multi' in argv[1:3]:  # Issue 1008
            self.respects_app_option = False
        try:
            sys.exit(determine_exit_status(
                super(CeleryCommand, self).execute_from_commandline(argv)))
        except KeyboardInterrupt:
            sys.exit(EX_FAILURE)

    @classmethod
    def get_command_info(cls, command, indent=0,
                         color=None, colored=None, app=None):
        colored = term.colored() if colored is None else colored
        colored = colored.names[color] if color else lambda x: x
        obj = cls.commands[command]
        cmd = 'celery {0}'.format(colored(command))
        if obj.leaf:
            return '|' + text.indent(cmd, indent)
        return text.join([
            ' ',
            '|' + text.indent('{0} --help'.format(cmd), indent),
            obj.list_commands(indent, 'celery {0}'.format(command), colored,
                              app=app),
        ])

    @classmethod
    def list_commands(cls, indent=0, colored=None, app=None):
        colored = term.colored() if colored is None else colored
        white = colored.white
        ret = []
        for command_cls, commands, color in command_classes:
            ret.extend([
                text.indent('+ {0}: '.format(white(command_cls)), indent),
                '\n'.join(
                    cls.get_command_info(
                        command, indent + 4, color, colored, app=app)
                    for command in commands),
                ''
            ])
        return '\n'.join(ret).strip()

    def with_pool_option(self, argv):
        if len(argv) > 1 and 'worker' in argv[0:3]:
            # this command supports custom pools
            # that may have to be loaded as early as possible.
            return (['-P'], ['--pool'])

    def on_concurrency_setup(self):
        self.load_extension_commands()

    def load_extension_commands(self):
        names = Extensions(self.ext_fmt.format(self=self),
                           self.register_command).load()
        if names:
            command_classes.append(('Extensions', names, 'magenta'))


if __name__ == '__main__':          # pragma: no cover
    main()
