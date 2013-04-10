# -*- coding: utf-8 -*-
"""

.. _preload-options:

Preload Options
---------------

These options are supported by all commands,
and usually parsed before command-specific arguments.

.. cmdoption:: -A, --app

    app instance to use (e.g. module.attr_name)

.. cmdoption:: -b, --broker

    url to broker.  default is 'amqp://guest@localhost//'

.. cmdoption:: --loader

    name of custom loader class to use.

.. cmdoption:: --config

    Name of the configuration module

.. _daemon-options:

Daemon Options
--------------

These options are supported by commands that can detach
into the background (daemon).  They will be present
in any command that also has a `--detach` option.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, `stderr` is used.

.. cmdoption:: --pidfile

    Optional file used to store the process pid.

    The program will not start if this file already exists
    and the pid is still alive.

.. cmdoption:: --uid

    User id, or user name of the user to run as after detaching.

.. cmdoption:: --gid

    Group id, or group name of the main group to change to after
    detaching.

.. cmdoption:: --umask

    Effective umask of the process after detaching. Default is 0.

.. cmdoption:: --workdir

    Optional directory to change to after detaching.

"""
from __future__ import absolute_import

import os
import re
import sys
import warnings

from collections import defaultdict
from optparse import OptionParser, IndentedHelpFormatter, make_option as Option
from types import ModuleType

import celery
from celery.exceptions import CDeprecationWarning, CPendingDeprecationWarning
from celery.platforms import EX_FAILURE, EX_USAGE, maybe_patch_concurrency
from celery.utils import text
from celery.utils.imports import symbol_by_name, import_from_cwd

# always enable DeprecationWarnings, so our users can see them.
for warning in (CDeprecationWarning, CPendingDeprecationWarning):
    warnings.simplefilter('once', warning, 0)

ARGV_DISABLED = """
Unrecognized command line arguments: %s

Try --help?
"""

find_long_opt = re.compile(r'.+?(--.+?)(?:\s|,|$)')
find_rst_ref = re.compile(r':\w+:`(.+?)`')


class HelpFormatter(IndentedHelpFormatter):

    def format_epilog(self, epilog):
        if epilog:
            return '\n%s\n\n' % epilog
        return ''

    def format_description(self, description):
        return text.ensure_2lines(text.fill_paragraphs(
            text.dedent(description), self.width))


class Command(object):
    """Base class for command line applications.

    :keyword app: The current app.
    :keyword get_app: Callable returning the current app if no app provided.

    """
    Parser = OptionParser

    #: Arg list used in help.
    args = ''

    #: Application version.
    version = celery.VERSION_BANNER

    #: If false the parser will raise an exception if positional
    #: args are provided.
    supports_args = True

    #: List of options (without preload options).
    option_list = ()

    # module Rst documentation to parse help from (if any)
    doc = None

    # Some programs (multi) does not want to load the app specified
    # (Issue #1008).
    respects_app_option = True

    #: List of options to parse before parsing other options.
    preload_options = (
        Option('-A', '--app', default=None),
        Option('-b', '--broker', default=None),
        Option('--loader', default=None),
        Option('--config', default=None),
        Option('--workdir', default=None, dest='working_directory'),
    )

    #: Enable if the application should support config from the cmdline.
    enable_config_from_cmdline = False

    #: Default configuration namespace.
    namespace = 'celery'

    #: Text to print at end of --help
    epilog = None

    #: Text to print in --help before option list.
    description = ''

    #: Set to true if this command doesn't have subcommands
    leaf = True

    def __init__(self, app=None, get_app=None):
        self.app = app
        self.get_app = get_app or self._get_default_app

    def run(self, *args, **options):
        """This is the body of the command called by :meth:`handle_argv`."""
        raise NotImplementedError('subclass responsibility')

    def execute_from_commandline(self, argv=None):
        """Execute application from command line.

        :keyword argv: The list of command line arguments.
                       Defaults to ``sys.argv``.

        """
        if argv is None:
            argv = list(sys.argv)
        # Should we load any special concurrency environment?
        self.maybe_patch_concurrency(argv)
        self.on_concurrency_setup()

        # Dump version and exit if '--version' arg set.
        self.early_version(argv)
        argv = self.setup_app_from_commandline(argv)
        prog_name = os.path.basename(argv[0])
        return self.handle_argv(prog_name, argv[1:])

    def run_from_argv(self, prog_name, argv=None):
        return self.handle_argv(prog_name, sys.argv if argv is None else argv)

    def maybe_patch_concurrency(self, argv=None):
        argv = argv or sys.argv
        pool_option = self.with_pool_option(argv)
        if pool_option:
            maybe_patch_concurrency(argv, *pool_option)
            short_opts, long_opts = pool_option

    def on_concurrency_setup(self):
        pass

    def usage(self, command):
        """Returns the command line usage string for this app."""
        return '%%prog [options] %s' % (self.args, )

    def get_options(self):
        """Get supported command line options."""
        return self.option_list

    def expanduser(self, value):
        if isinstance(value, basestring):
            return os.path.expanduser(value)
        return value

    def handle_argv(self, prog_name, argv):
        """Parses command line arguments from ``argv`` and dispatches
        to :meth:`run`.

        :param prog_name: The program name (``argv[0]``).
        :param argv: Command arguments.

        Exits with an error message if :attr:`supports_args` is disabled
        and ``argv`` contains positional arguments.

        """
        options, args = self.prepare_args(*self.parse_options(prog_name, argv))
        return self.run(*args, **options)

    def prepare_args(self, options, args):
        if options:
            options = dict((k, self.expanduser(v))
                           for k, v in vars(options).iteritems()
                           if not k.startswith('_'))
        args = [self.expanduser(arg) for arg in args]
        self.check_args(args)
        return options, args

    def check_args(self, args):
        if not self.supports_args and args:
            self.die(ARGV_DISABLED % (', '.join(args, )), EX_USAGE)

    def die(self, msg, status=EX_FAILURE):
        sys.stderr.write(msg + '\n')
        sys.exit(status)

    def early_version(self, argv):
        if '--version' in argv:
            sys.stdout.write('%s\n' % self.version)
            sys.exit(0)

    def parse_options(self, prog_name, arguments):
        """Parse the available options."""
        # Don't want to load configuration to just print the version,
        # so we handle --version manually here.
        parser = self.create_parser(prog_name)
        return parser.parse_args(arguments)

    def create_parser(self, prog_name, command=None):
        return self.prepare_parser(self.Parser(
            prog=prog_name,
            usage=self.usage(command),
            version=self.version,
            epilog=self.epilog,
            formatter=HelpFormatter(),
            description=self.description,
            option_list=(self.preload_options + self.get_options())))

    def prepare_parser(self, parser):
        docs = [self.parse_doc(doc) for doc in (self.doc, __doc__) if doc]
        for doc in docs:
            for long_opt, help in doc.iteritems():
                option = parser.get_option(long_opt)
                if option is not None:
                    option.help = ' '.join(help) % {'default': option.default}
        return parser

    def setup_app_from_commandline(self, argv):
        preload_options = self.parse_preload_options(argv)
        workdir = preload_options.get('working_directory')
        if workdir:
            os.chdir(workdir)
        app = (preload_options.get('app') or
               os.environ.get('CELERY_APP') or
               self.app)
        preload_loader = preload_options.get('loader')
        if preload_loader:
            # Default app takes loader from this env (Issue #1066).
            os.environ['CELERY_LOADER'] = preload_loader
        loader = (preload_loader,
                  os.environ.get('CELERY_LOADER') or
                  'default')
        broker = preload_options.get('broker', None)
        if broker:
            os.environ['CELERY_BROKER_URL'] = broker
        config = preload_options.get('config')
        if config:
            os.environ['CELERY_CONFIG_MODULE'] = config
        if self.respects_app_option:
            if app and self.respects_app_option:
                self.app = self.find_app(app)
            elif self.app is None:
                self.app = self.get_app(loader=loader)
            if self.enable_config_from_cmdline:
                argv = self.process_cmdline_config(argv)
        else:
            self.app = celery.Celery()
        return argv

    def find_app(self, app):
        try:
            sym = self.symbol_by_name(app)
        except AttributeError:
            # last part was not an attribute, but a module
            sym = import_from_cwd(app)
        if isinstance(sym, ModuleType):
            if getattr(sym, '__path__', None):
                return self.find_app('%s.celery:' % (app.replace(':', ''), ))
            return sym.celery
        return sym

    def symbol_by_name(self, name):
        return symbol_by_name(name, imp=import_from_cwd)
    get_cls_by_name = symbol_by_name  # XXX compat

    def process_cmdline_config(self, argv):
        try:
            cargs_start = argv.index('--')
        except ValueError:
            return argv
        argv, cargs = argv[:cargs_start], argv[cargs_start + 1:]
        self.app.config_from_cmdline(cargs, namespace=self.namespace)
        return argv

    def parse_preload_options(self, args):
        acc = {}
        opts = {}
        for opt in self.preload_options:
            for t in (opt._long_opts, opt._short_opts):
                opts.update(dict(zip(t, [opt.dest] * len(t))))
        index = 0
        length = len(args)
        while index < length:
            arg = args[index]
            if arg.startswith('--') and '=' in arg:
                key, value = arg.split('=', 1)
                dest = opts.get(key)
                if dest:
                    acc[dest] = value
            elif arg.startswith('-'):
                dest = opts.get(arg)
                if dest:
                    acc[dest] = args[index + 1]
                    index += 1
            index += 1
        return acc

    def parse_doc(self, doc):
        options, in_option = defaultdict(list), None
        for line in doc.splitlines():
            if line.startswith('.. cmdoption::'):
                m = find_long_opt.match(line)
                if m:
                    in_option = m.groups()[0].strip()
                assert in_option, 'missing long opt'
            elif in_option and line.startswith(' ' * 4):
                options[in_option].append(
                    find_rst_ref.sub(r'\1', line.strip()).replace('`', ''))
        return options

    def with_pool_option(self, argv):
        """Returns tuple of ``(short_opts, long_opts)`` if the command
        supports a pool argument, and used to monkey patch eventlet/gevent
        environments as early as possible.

        E.g::
              has_pool_option = (['-P'], ['--pool'])
        """
        pass

    def _get_default_app(self, *args, **kwargs):
        from celery._state import get_current_app
        return get_current_app()  # omit proxy


def daemon_options(default_pidfile=None, default_logfile=None):
    return (
        Option('-f', '--logfile', default=default_logfile),
        Option('--pidfile', default=default_pidfile),
        Option('--uid', default=None),
        Option('--gid', default=None),
        Option('--umask', default=0, type='int'),
    )
