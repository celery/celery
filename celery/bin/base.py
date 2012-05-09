# -*- coding: utf-8 -*-
"""

.. _preload-options:

Preload Options
---------------

.. cmdoption:: --app

    Fully qualified name of the app instance to use.

.. cmdoption:: -b, --broker

    Broker URL.  Default is 'amqp://guest:guest@localhost:5672//'

.. cmdoption:: --loader

    Name of the loader class to use.
    Taken from the environment variable :envvar:`CELERY_LOADER`
    or 'default' if that is not set.

.. cmdoption:: --config

    Name of the module to read configuration from,
    default is 'celeryconfig'.

.. _daemon-options:

Daemon Options
--------------

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
from optparse import OptionParser, make_option as Option
from types import ModuleType

from celery import Celery, __version__
from celery.exceptions import CDeprecationWarning, CPendingDeprecationWarning
from celery.platforms import EX_FAILURE, EX_USAGE
from celery.utils.imports import symbol_by_name, import_from_cwd

# always enable DeprecationWarnings, so our users can see them.
for warning in (CDeprecationWarning, CPendingDeprecationWarning):
    warnings.simplefilter("once", warning, 0)

ARGV_DISABLED = """
Unrecognized command line arguments: %s

Try --help?
"""

find_long_opt = re.compile(r'.+?(--.+?)(?:\s|,|$)')
find_rst_ref = re.compile(r':\w+:`(.+?)`')


class Command(object):
    """Base class for command line applications.

    :keyword app: The current app.
    :keyword get_app: Callable returning the current app if no app provided.

    """
    #: Arg list used in help.
    args = ''

    #: Application version.
    version = __version__

    #: If false the parser will raise an exception if positional
    #: args are provided.
    supports_args = True

    #: List of options (without preload options).
    option_list = ()

    # module Rst documentation to parse help from (if any)
    doc = None

    #: List of options to parse before parsing other options.
    preload_options = (
        Option("--app", default=None),
        Option("-b", "--broker", default=None),
        Option("--loader", default=None),
        Option("--config", default="celeryconfig", dest="config_module"),
    )

    #: Enable if the application should support config from the cmdline.
    enable_config_from_cmdline = False

    #: Default configuration namespace.
    namespace = "celery"

    Parser = OptionParser

    def __init__(self, app=None, get_app=None):
        self.app = app
        self.get_app = get_app or self._get_default_app

    def run(self, *args, **options):
        """This is the body of the command called by :meth:`handle_argv`."""
        raise NotImplementedError("subclass responsibility")

    def execute_from_commandline(self, argv=None):
        """Execute application from command line.

        :keyword argv: The list of command line arguments.
                       Defaults to ``sys.argv``.

        """
        if argv is None:
            argv = list(sys.argv)
        argv = self.setup_app_from_commandline(argv)
        prog_name = os.path.basename(argv[0])
        return self.handle_argv(prog_name, argv[1:])

    def usage(self, command):
        """Returns the command-line usage string for this app."""
        return "%%prog [options] %s" % (self.args, )

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
        args = map(self.expanduser, args)
        self.check_args(args)
        return options, args

    def check_args(self, args):
        if not self.supports_args and args:
            self.die(ARGV_DISABLED % (', '.join(args, )), EX_USAGE)

    def die(self, msg, status=EX_FAILURE):
        sys.stderr.write(msg + "\n")
        sys.exit(status)

    def parse_options(self, prog_name, arguments):
        """Parse the available options."""
        # Don't want to load configuration to just print the version,
        # so we handle --version manually here.
        if "--version" in arguments:
            sys.stdout.write("%s\n" % self.version)
            sys.exit(0)
        parser = self.create_parser(prog_name)
        return parser.parse_args(arguments)

    def create_parser(self, prog_name, command=None):
        return self.prepare_parser(self.Parser(prog=prog_name,
                           usage=self.usage(command),
                           version=self.version,
                           option_list=(self.preload_options +
                                        self.get_options())))

    def prepare_parser(self, parser):
        docs = [self.parse_doc(doc) for doc in (self.doc, __doc__) if doc]
        for doc in docs:
            for long_opt, help in doc.iteritems():
                option = parser.get_option(long_opt)
                if option is not None:
                    option.help = ' '.join(help) % {"default": option.default}
        return parser

    def prepare_preload_options(self, options):
        """Optional handler to do additional processing of preload options.

        Configuration must not have been initialized
        until after this is called.

        """
        pass

    def setup_app_from_commandline(self, argv):
        preload_options = self.parse_preload_options(argv)
        self.prepare_preload_options(preload_options)
        app = (preload_options.get("app") or
               os.environ.get("CELERY_APP") or
               self.app)
        loader = (preload_options.get("loader") or
                  os.environ.get("CELERY_LOADER") or
                  "default")
        broker = preload_options.get("broker", None)
        if broker:
            os.environ["CELERY_BROKER_URL"] = broker
        config_module = preload_options.get("config_module")
        if config_module:
            os.environ["CELERY_CONFIG_MODULE"] = config_module
        if app:
            self.app = self.find_app(app)
        else:
            self.app = self.get_app(loader=loader)
        if self.enable_config_from_cmdline:
            argv = self.process_cmdline_config(argv)
        return argv

    def find_app(self, app):
        sym = self.symbol_by_name(app)
        if isinstance(sym, ModuleType):
            if getattr(sym, "__path__", None):
                return self.find_app("%s.celery:" % (app.replace(":", ""), ))
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
            if line.startswith(".. cmdoption::"):
                m = find_long_opt.match(line)
                if m:
                    in_option = m.groups()[0].strip()
                assert in_option, "missing long opt"
            elif in_option and line.startswith(' ' * 4):
                options[in_option].append(find_rst_ref.sub(r'\1',
                    line.strip()).replace('`', ''))
        return options

    def _get_default_app(self, *args, **kwargs):
        return Celery(*args, **kwargs)


def daemon_options(default_pidfile=None, default_logfile=None):
    return (
        Option("-f", "--logfile", default=default_logfile),
        Option("--pidfile", default=default_pidfile),
        Option("--uid", default=None),
        Option("--gid", default=None),
        Option("--umask", default=0, type="int"),
        Option("--workdir", default=None, dest="working_directory"),
    )
