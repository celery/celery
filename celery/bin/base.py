# -*- coding: utf-8 -*-
"""Base command-line interface."""
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import json
import os
import random
import re
import sys
import warnings
from collections import defaultdict
from heapq import heappush
from pprint import pformat

from celery import VERSION_BANNER, Celery, maybe_patch_concurrency, signals
from celery.exceptions import CDeprecationWarning, CPendingDeprecationWarning
from celery.five import (PY2, getfullargspec, items, long_t,
                         python_2_unicode_compatible, string, string_t,
                         text_t)
from celery.platforms import EX_FAILURE, EX_OK, EX_USAGE, isatty
from celery.utils import imports, term, text
from celery.utils.functional import dictfilter
from celery.utils.nodenames import host_format, node_format
from celery.utils.objects import Bunch

# Option is here for backwards compatibility, as third-party commands
# may import it from here.
try:
    from optparse import Option  # pylint: disable=deprecated-module
except ImportError:  # pragma: no cover
    Option = None  # noqa

try:
    input = raw_input
except NameError:  # pragma: no cover
    pass

__all__ = (
    'Error', 'UsageError', 'Extensions', 'Command', 'Option', 'daemon_options',
)

# always enable DeprecationWarnings, so our users can see them.
for warning in (CDeprecationWarning, CPendingDeprecationWarning):
    warnings.simplefilter('once', warning, 0)

# TODO: Remove this once we drop support for Python < 3.6
if sys.version_info < (3, 6):
    ModuleNotFoundError = ImportError

ARGV_DISABLED = """
Unrecognized command-line arguments: {0}

Try --help?
"""

UNABLE_TO_LOAD_APP_MODULE_NOT_FOUND = """
Unable to load celery application.
The module {0} was not found.
"""

UNABLE_TO_LOAD_APP_APP_MISSING = """
Unable to load celery application.
{0}
"""

find_long_opt = re.compile(r'.+?(--.+?)(?:\s|,|$)')
find_rst_ref = re.compile(r':\w+:`(.+?)`')
find_rst_decl = re.compile(r'^\s*\.\. .+?::.+$')


def _optparse_callback_to_type(option, callback):
    parser = Bunch(values=Bunch())

    def _on_arg(value):
        callback(option, None, value, parser)
        return getattr(parser.values, option.dest)
    return _on_arg


def _add_optparse_argument(parser, opt, typemap=None):
    typemap = {
        'string': text_t,
        'int': int,
        'long': long_t,
        'float': float,
        'complex': complex,
        'choice': None} if not typemap else typemap
    if opt.callback:
        opt.type = _optparse_callback_to_type(opt, opt.type)
    # argparse checks for existence of this kwarg
    if opt.action == 'callback':
        opt.action = None
    # store_true sets value to "('NO', 'DEFAULT')" for some
    # crazy reason, so not to set a sane default here.
    if opt.action == 'store_true' and opt.default is None:
        opt.default = False
    parser.add_argument(
        *opt._long_opts + opt._short_opts,
        **dictfilter({
            'action': opt.action,
            'type': typemap.get(opt.type, opt.type),
            'dest': opt.dest,
            'nargs': opt.nargs,
            'choices': opt.choices,
            'help': opt.help,
            'metavar': opt.metavar,
            'default': opt.default}))


def _add_compat_options(parser, options):
    for option in options or ():
        if callable(option):
            option(parser)
        else:
            _add_optparse_argument(parser, option)


@python_2_unicode_compatible
class Error(Exception):
    """Exception raised by commands."""

    status = EX_FAILURE

    def __init__(self, reason, status=None):
        self.reason = reason
        self.status = status if status is not None else self.status
        super(Error, self).__init__(reason, status)

    def __str__(self):
        return self.reason


class UsageError(Error):
    """Exception raised for malformed arguments."""

    status = EX_USAGE


class Extensions(object):
    """Loads extensions from setuptools entrypoints."""

    def __init__(self, namespace, register):
        self.names = []
        self.namespace = namespace
        self.register = register

    def add(self, cls, name):
        heappush(self.names, name)
        self.register(cls, name=name)

    def load(self):
        for name, cls in imports.load_extension_classes(self.namespace):
            self.add(cls, name)
        return self.names


class Command(object):
    """Base class for command-line applications.

    Arguments:
        app (Celery): The app to use.
        get_app (Callable): Fucntion returning the current app
            when no app provided.
    """

    Error = Error
    UsageError = UsageError
    Parser = argparse.ArgumentParser

    #: Arg list used in help.
    args = ''

    #: Application version.
    version = VERSION_BANNER

    #: If false the parser will raise an exception if positional
    #: args are provided.
    supports_args = True

    #: List of options (without preload options).
    option_list = None

    # module Rst documentation to parse help from (if any)
    doc = None

    # Some programs (multi) does not want to load the app specified
    # (Issue #1008).
    respects_app_option = True

    #: Enable if the application should support config from the cmdline.
    enable_config_from_cmdline = False

    #: Default configuration name-space.
    namespace = None

    #: Text to print at end of --help
    epilog = None

    #: Text to print in --help before option list.
    description = ''

    #: Set to true if this command doesn't have sub-commands
    leaf = True

    # used by :meth:`say_remote_command_reply`.
    show_body = True
    # used by :meth:`say_chat`.
    show_reply = True

    prog_name = 'celery'

    #: Name of argparse option used for parsing positional args.
    args_name = 'args'

    def __init__(self, app=None, get_app=None, no_color=False,
                 stdout=None, stderr=None, quiet=False, on_error=None,
                 on_usage_error=None):
        self.app = app
        self.get_app = get_app or self._get_default_app
        self.stdout = stdout or sys.stdout
        self.stderr = stderr or sys.stderr
        self._colored = None
        self._no_color = no_color
        self.quiet = quiet
        if not self.description:
            self.description = self._strip_restructeredtext(self.__doc__)
        if on_error:
            self.on_error = on_error
        if on_usage_error:
            self.on_usage_error = on_usage_error

    def run(self, *args, **options):
        raise NotImplementedError('subclass responsibility')

    def on_error(self, exc):
        # pylint: disable=method-hidden
        #   on_error argument to __init__ may override this method.
        self.error(self.colored.red('Error: {0}'.format(exc)))

    def on_usage_error(self, exc):
        # pylint: disable=method-hidden
        #   on_usage_error argument to __init__ may override this method.
        self.handle_error(exc)

    def on_concurrency_setup(self):
        pass

    def __call__(self, *args, **kwargs):
        random.seed()  # maybe we were forked.
        self.verify_args(args)
        try:
            ret = self.run(*args, **kwargs)
            return ret if ret is not None else EX_OK
        except self.UsageError as exc:
            self.on_usage_error(exc)
            return exc.status
        except self.Error as exc:
            self.on_error(exc)
            return exc.status

    def verify_args(self, given, _index=0):
        S = getfullargspec(self.run)
        _index = 1 if S.args and S.args[0] == 'self' else _index
        required = S.args[_index:-len(S.defaults) if S.defaults else None]
        missing = required[len(given):]
        if missing:
            raise self.UsageError('Missing required {0}: {1}'.format(
                text.pluralize(len(missing), 'argument'),
                ', '.join(missing)
            ))

    def execute_from_commandline(self, argv=None):
        """Execute application from command-line.

        Arguments:
            argv (List[str]): The list of command-line arguments.
                Defaults to ``sys.argv``.
        """
        if argv is None:
            argv = list(sys.argv)
        # Should we load any special concurrency environment?
        self.maybe_patch_concurrency(argv)
        self.on_concurrency_setup()

        # Dump version and exit if '--version' arg set.
        self.early_version(argv)
        try:
            argv = self.setup_app_from_commandline(argv)
        except ModuleNotFoundError as e:
            # In Python 2.7 and below, there is no name instance for exceptions
            # TODO: Remove this once we drop support for Python 2.7
            if PY2:
                package_name = e.message.replace("No module named ", "")
            else:
                package_name = e.name
            self.on_error(UNABLE_TO_LOAD_APP_MODULE_NOT_FOUND.format(package_name))
            return EX_FAILURE
        except AttributeError as e:
            msg = e.args[0].capitalize()
            self.on_error(UNABLE_TO_LOAD_APP_APP_MISSING.format(msg))
            return EX_FAILURE

        self.prog_name = os.path.basename(argv[0])
        return self.handle_argv(self.prog_name, argv[1:])

    def run_from_argv(self, prog_name, argv=None, command=None):
        return self.handle_argv(prog_name,
                                sys.argv if argv is None else argv, command)

    def maybe_patch_concurrency(self, argv=None):
        argv = argv or sys.argv
        pool_option = self.with_pool_option(argv)
        if pool_option:
            maybe_patch_concurrency(argv, *pool_option)

    def usage(self, command):
        return '%(prog)s {0} [options] {self.args}'.format(command, self=self)

    def add_arguments(self, parser):
        pass

    def get_options(self):
        # This is for optparse options, please use add_arguments.
        return self.option_list

    def add_preload_arguments(self, parser):
        group = parser.add_argument_group('Global Options')
        group.add_argument('-A', '--app', default=None)
        group.add_argument('-b', '--broker', default=None)
        group.add_argument('--result-backend', default=None)
        group.add_argument('--loader', default=None)
        group.add_argument('--config', default=None)
        group.add_argument('--workdir', default=None)
        group.add_argument(
            '--no-color', '-C', action='store_true', default=None)
        group.add_argument('--quiet', '-q', action='store_true')

    def _add_version_argument(self, parser):
        parser.add_argument(
            '--version', action='version', version=self.version,
        )

    def prepare_arguments(self, parser):
        pass

    def expanduser(self, value):
        if isinstance(value, string_t):
            return os.path.expanduser(value)
        return value

    def ask(self, q, choices, default=None):
        """Prompt user to choose from a tuple of string values.

        If a default is not specified the question will be repeated
        until the user gives a valid choice.

        Matching is case insensitive.

        Arguments:
            q (str): the question to ask (don't include questionark)
            choice (Tuple[str]): tuple of possible choices, must be lowercase.
            default (Any): Default value if any.
        """
        schoices = choices
        if default is not None:
            schoices = [c.upper() if c == default else c.lower()
                        for c in choices]
        schoices = '/'.join(schoices)

        p = '{0} ({1})? '.format(q.capitalize(), schoices)
        while 1:
            val = input(p).lower()
            if val in choices:
                return val
            elif default is not None:
                break
        return default

    def handle_argv(self, prog_name, argv, command=None):
        """Parse arguments from argv and dispatch to :meth:`run`.

        Warning:
            Exits with an error message if :attr:`supports_args` is disabled
            and ``argv`` contains positional arguments.

        Arguments:
            prog_name (str): The program name (``argv[0]``).
            argv (List[str]): Rest of command-line arguments.
        """
        options, args = self.prepare_args(
            *self.parse_options(prog_name, argv, command))
        return self(*args, **options)

    def prepare_args(self, options, args):
        if options:
            options = {
                k: self.expanduser(v)
                for k, v in items(options) if not k.startswith('_')
            }
        args = [self.expanduser(arg) for arg in args]
        self.check_args(args)
        return options, args

    def check_args(self, args):
        if not self.supports_args and args:
            self.die(ARGV_DISABLED.format(', '.join(args)), EX_USAGE)

    def error(self, s):
        self.out(s, fh=self.stderr)

    def out(self, s, fh=None):
        print(s, file=fh or self.stdout)

    def die(self, msg, status=EX_FAILURE):
        self.error(msg)
        sys.exit(status)

    def early_version(self, argv):
        if '--version' in argv:
            print(self.version, file=self.stdout)
            sys.exit(0)

    def parse_options(self, prog_name, arguments, command=None):
        """Parse the available options."""
        # Don't want to load configuration to just print the version,
        # so we handle --version manually here.
        self.parser = self.create_parser(prog_name, command)
        options = vars(self.parser.parse_args(arguments))
        return options, options.pop(self.args_name, None) or []

    def create_parser(self, prog_name, command=None):
        # for compatibility with optparse usage.
        usage = self.usage(command).replace('%prog', '%(prog)s')
        parser = self.Parser(
            prog=prog_name,
            usage=usage,
            epilog=self._format_epilog(self.epilog),
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self._format_description(self.description),
        )
        self._add_version_argument(parser)
        self.add_preload_arguments(parser)
        self.add_arguments(parser)
        self.add_compat_options(parser, self.get_options())
        self.add_compat_options(parser, self.app.user_options['preload'])

        if self.supports_args:
            # for backward compatibility with optparse, we automatically
            # add arbitrary positional args.
            parser.add_argument(self.args_name, nargs='*')
        return self.prepare_parser(parser)

    def _format_epilog(self, epilog):
        if epilog:
            return '\n{0}\n\n'.format(epilog)
        return ''

    def _format_description(self, description):
        width = argparse.HelpFormatter('prog')._width
        return text.ensure_newlines(
            text.fill_paragraphs(text.dedent(description), width))

    def add_compat_options(self, parser, options):
        _add_compat_options(parser, options)

    def prepare_parser(self, parser):
        docs = [self.parse_doc(doc) for doc in (self.doc, __doc__) if doc]
        for doc in docs:
            for long_opt, help in items(doc):
                option = parser._option_string_actions[long_opt]
                if option is not None:
                    option.help = ' '.join(help).format(default=option.default)
        return parser

    def setup_app_from_commandline(self, argv):
        preload_options = self.parse_preload_options(argv)
        quiet = preload_options.get('quiet')
        if quiet is not None:
            self.quiet = quiet
        try:
            self.no_color = preload_options['no_color']
        except KeyError:
            pass
        workdir = preload_options.get('workdir')
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
        result_backend = preload_options.get('result_backend', None)
        if result_backend:
            os.environ['CELERY_RESULT_BACKEND'] = result_backend
        config = preload_options.get('config')
        if config:
            os.environ['CELERY_CONFIG_MODULE'] = config
        if self.respects_app_option:
            if app:
                self.app = self.find_app(app)
            elif self.app is None:
                self.app = self.get_app(loader=loader)
            if self.enable_config_from_cmdline:
                argv = self.process_cmdline_config(argv)
        else:
            self.app = Celery(fixups=[])

        self._handle_user_preload_options(argv)

        return argv

    def _handle_user_preload_options(self, argv):
        user_preload = tuple(self.app.user_options['preload'] or ())
        if user_preload:
            user_options = self._parse_preload_options(argv, user_preload)
            signals.user_preload_options.send(
                sender=self, app=self.app, options=user_options,
            )

    def find_app(self, app):
        from celery.app.utils import find_app
        return find_app(app, symbol_by_name=self.symbol_by_name)

    def symbol_by_name(self, name, imp=imports.import_from_cwd):
        return imports.symbol_by_name(name, imp=imp)
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
        return self._parse_preload_options(args, [self.add_preload_arguments])

    def _parse_preload_options(self, args, options):
        args = [arg for arg in args if arg not in ('-h', '--help')]
        parser = self.Parser()
        self.add_compat_options(parser, options)
        namespace, _ = parser.parse_known_args(args)
        return vars(namespace)

    def add_append_opt(self, acc, opt, value):
        default = opt.default or []

        if opt.dest not in acc:
            acc[opt.dest] = default

        acc[opt.dest].append(value)

    def parse_doc(self, doc):
        options, in_option = defaultdict(list), None
        for line in doc.splitlines():
            if line.startswith('.. cmdoption::'):
                m = find_long_opt.match(line)
                if m:
                    in_option = m.groups()[0].strip()
                assert in_option, 'missing long opt'
            elif in_option and line.startswith(' ' * 4):
                if not find_rst_decl.match(line):
                    options[in_option].append(
                        find_rst_ref.sub(
                            r'\1', line.strip()).replace('`', ''))
        return options

    def _strip_restructeredtext(self, s):
        return '\n'.join(
            find_rst_ref.sub(r'\1', line.replace('`', ''))
            for line in (s or '').splitlines()
            if not find_rst_decl.match(line)
        )

    def with_pool_option(self, argv):
        """Return tuple of ``(short_opts, long_opts)``.

        Returns only if the command
        supports a pool argument, and used to monkey patch eventlet/gevent
        environments as early as possible.

        Example:
              >>> has_pool_option = (['-P'], ['--pool'])
        """

    def node_format(self, s, nodename, **extra):
        return node_format(s, nodename, **extra)

    def host_format(self, s, **extra):
        return host_format(s, **extra)

    def _get_default_app(self, *args, **kwargs):
        from celery._state import get_current_app
        return get_current_app()  # omit proxy

    def pretty_list(self, n):
        c = self.colored
        if not n:
            return '- empty -'
        return '\n'.join(
            str(c.reset(c.white('*'), ' {0}'.format(item))) for item in n
        )

    def pretty_dict_ok_error(self, n):
        c = self.colored
        try:
            return (c.green('OK'),
                    text.indent(self.pretty(n['ok'])[1], 4))
        except KeyError:
            pass
        return (c.red('ERROR'),
                text.indent(self.pretty(n['error'])[1], 4))

    def say_remote_command_reply(self, replies):
        c = self.colored
        node = next(iter(replies))  # <-- take first.
        reply = replies[node]
        status, preply = self.pretty(reply)
        self.say_chat('->', c.cyan(node, ': ') + status,
                      text.indent(preply, 4) if self.show_reply else '')

    def pretty(self, n):
        OK = str(self.colored.green('OK'))
        if isinstance(n, list):
            return OK, self.pretty_list(n)
        if isinstance(n, dict):
            if 'ok' in n or 'error' in n:
                return self.pretty_dict_ok_error(n)
            else:
                return OK, json.dumps(n, sort_keys=True, indent=4)
        if isinstance(n, string_t):
            return OK, string(n)
        return OK, pformat(n)

    def say_chat(self, direction, title, body=''):
        c = self.colored
        if direction == '<-' and self.quiet:
            return
        dirstr = not self.quiet and c.bold(c.white(direction), ' ') or ''
        self.out(c.reset(dirstr, title))
        if body and self.show_body:
            self.out(body)

    @property
    def colored(self):
        if self._colored is None:
            self._colored = term.colored(
                enabled=isatty(self.stdout) and not self.no_color)
        return self._colored

    @colored.setter
    def colored(self, obj):
        self._colored = obj

    @property
    def no_color(self):
        return self._no_color

    @no_color.setter
    def no_color(self, value):
        self._no_color = value
        if self._colored is not None:
            self._colored.enabled = not self._no_color


def daemon_options(parser, default_pidfile=None, default_logfile=None):
    """Add daemon options to argparse parser."""
    group = parser.add_argument_group('Daemonization Options')
    group.add_argument('-f', '--logfile', default=default_logfile),
    group.add_argument('--pidfile', default=default_pidfile),
    group.add_argument('--uid', default=None),
    group.add_argument('--gid', default=None),
    group.add_argument('--umask', default=None),
    group.add_argument('--executable', default=None),
