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
from __future__ import absolute_import, print_function, unicode_literals

import numbers
import sys
from functools import partial

# Import commands from other modules
from celery.bin.amqp import amqp
# Cannot use relative imports here due to a Windows issue (#1111).
from celery.bin.base import Command, Extensions
from celery.bin.beat import beat
from celery.bin.call import call
from celery.bin.control import _RemoteControl  # noqa
from celery.bin.control import control, inspect, status
from celery.bin.events import events
from celery.bin.graph import graph
from celery.bin.list import list_
from celery.bin.logtool import logtool
from celery.bin.migrate import migrate
from celery.bin.purge import purge
from celery.bin.result import result
from celery.bin.shell import shell
from celery.bin.upgrade import upgrade
from celery.bin.worker import worker
from celery.platforms import EX_FAILURE, EX_OK, EX_USAGE
from celery.utils import term, text

__all__ = ('CeleryCommand', 'main')

HELP = """
---- -- - - ---- Commands- -------------- --- ------------

{commands}
---- -- - - --------- -- - -------------- --- ------------

Type '{prog_name} <command> --help' for help using a specific command.
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

    def __init__(self, *args, **kwargs):
        """Custom initialization for report command.

        We need this custom initialization to make sure that
        everything is loaded when running a report.
        There has been some issues when printing Django's
        settings because Django is not properly setup when
        running the report.
        """
        super(report, self).__init__(*args, **kwargs)
        self.app.loader.import_default_modules()

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
