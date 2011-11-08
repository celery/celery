# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import sys
import warnings

from optparse import OptionParser, make_option as Option

from .. import __version__, Celery
from ..exceptions import CDeprecationWarning, CPendingDeprecationWarning


# always enable DeprecationWarnings, so our users can see them.
for warning in (CDeprecationWarning, CPendingDeprecationWarning):
    warnings.simplefilter("once", warning, 0)


class Command(object):
    """Base class for command line applications.

    :keyword app: The current app.
    :keyword get_app: Callable returning the current app if no app provided.

    """
    _default_broker_url = r'amqp://guest:guest@localhost:5672//'
    #: Arg list used in help.
    args = ''

    #: Application version.
    version = __version__

    #: If false the parser will raise an exception if positional
    #: args are provided.
    supports_args = True

    #: List of options (without preload options).
    option_list = ()

    #: List of options to parse before parsing other options.
    preload_options = (
            Option("--app",
                    default=None, action="store", dest="app",
                    help="Name of the app instance to use. "),
            Option("-b", "--broker",
                    default=None, action="store", dest="broker",
                    help="Broker URL.  Default is %s" % (
                            _default_broker_url, )),
            Option("--loader",
                   default=None, action="store", dest="loader",
                   help="Name of the loader class to use. "
                        "Taken from the environment variable CELERY_LOADER, "
                        "or 'default' if that is not set."),
            Option("--config",
                    default="celeryconfig", action="store",
                    dest="config_module",
                    help="Name of the module to read configuration from."),
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

    def usage(self):
        """Returns the command-line usage string for this app."""
        return "%%prog [options] %s" % (self.args, )

    def get_options(self):
        """Get supported command line options."""
        return self.option_list

    def handle_argv(self, prog_name, argv):
        """Parses command line arguments from ``argv`` and dispatches
        to :meth:`run`.

        :param prog_name: The program name (``argv[0]``).
        :param argv: Command arguments.

        Exits with an error message if :attr:`supports_args` is disabled
        and ``argv`` contains positional arguments.

        """
        options, args = self.parse_options(prog_name, argv)
        if not self.supports_args and args:
            sys.stderr.write(
                "\nUnrecognized command line arguments: %s\n" % (
                    ", ".join(args), ))
            sys.stderr.write("\nTry --help?\n")
            sys.exit(1)
        return self.run(*args, **vars(options))

    def parse_options(self, prog_name, arguments):
        """Parse the available options."""
        # Don't want to load configuration to just print the version,
        # so we handle --version manually here.
        if "--version" in arguments:
            print(self.version)
            sys.exit(0)
        parser = self.create_parser(prog_name)
        options, args = parser.parse_args(arguments)
        return options, args

    def create_parser(self, prog_name):
        return self.Parser(prog=prog_name,
                           usage=self.usage(),
                           version=self.version,
                           option_list=(self.preload_options +
                                        self.get_options()))

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
            self.app = self.get_cls_by_name(app)
        else:
            self.app = self.get_app(loader=loader)
        if self.enable_config_from_cmdline:
            argv = self.process_cmdline_config(argv)
        return argv

    def get_cls_by_name(self, name):
        from ..utils import get_cls_by_name, import_from_cwd
        return get_cls_by_name(name, imp=import_from_cwd)

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

    def _get_default_app(self, *args, **kwargs):
        return Celery(*args, **kwargs)


def daemon_options(default_pidfile=None, default_logfile=None):
    return (
        Option('-f', '--logfile', default=default_logfile,
               action="store", dest="logfile",
               help="Path to the logfile"),
        Option('--pidfile', default=default_pidfile,
               action="store", dest="pidfile",
               help="Path to the pidfile."),
        Option('--uid', default=None,
               action="store", dest="uid",
               help="Effective user id to run as when detached."),
        Option('--gid', default=None,
               action="store", dest="gid",
               help="Effective group id to run as when detached."),
        Option('--umask', default=0,
               action="store", type="int", dest="umask",
               help="Umask of the process when detached."),
        Option('--workdir', default=None,
               action="store", dest="working_directory",
               help="Directory to change to when detached."),
)
