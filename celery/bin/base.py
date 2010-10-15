import os
import sys

from optparse import OptionParser, make_option as Option

import celery


class Command(object):
    args = ''
    version = celery.__version__
    option_list = ()
    preload_options = (
            Option("--app",
                    default=None, action="store", dest="app",
                    help="Name of the app instance to use. "),
            Option("--loader",
                   default=None, action="store", dest="loader",
                   help="Name of the loader class to use. "
                        "Taken from the environment variable CELERY_LOADER, "
                        "or 'default' if that is not set."),
            Option("--config",
                    default="celeryconfig", action="store",
                    dest="config_module",
                    help="Name of the module to read configuration from.")
    )
    enable_config_from_cmdline = False
    namespace = "celery"

    Parser = OptionParser

    def __init__(self, app=None, get_app=None):
        self.app = app
        self.get_app = get_app or self._get_default_app

    def usage(self):
        return "%%prog [options] %s" % (self.args, )

    def get_options(self):
        return self.option_list

    def handle_argv(self, prog_name, argv):
        options, args = self.parse_options(prog_name, argv)
        return self.run(*args, **vars(options))

    def run(self, *args, **options):
        raise NotImplementedError("subclass responsibility")

    def execute_from_commandline(self, argv=None):
        if argv is None:
            argv = list(sys.argv)
        argv = self.setup_app_from_commandline(argv)
        prog_name = os.path.basename(argv[0])
        return self.handle_argv(prog_name, argv[1:])

    def parse_options(self, prog_name, arguments):
        """Parse the available options."""
        parser = self.create_parser(prog_name)
        options, args = parser.parse_args(arguments)
        return options, args

    def create_parser(self, prog_name):
        return self.Parser(prog=prog_name,
                           usage=self.usage(),
                           version=self.version,
                           option_list=(self.preload_options +
                                        self.get_options()))

    def setup_app_from_commandline(self, argv):
        preload_options = self.parse_preload_options(argv)
        app = (preload_options.pop("app", None) or
               os.environ.get("CELERY_APP"))
        loader = (preload_options.pop("loader", None) or
                  os.environ.get("CELERY_LOADER") or
                  "default")
        config_module = preload_options.pop("config_module", None)
        if config_module:
            os.environ["CELERY_CONFIG_MODULE"] = config_module
        if app:
            self.app = self.get_cls_by_name(app)
        elif not self.app:
            self.app = self.get_app(loader=loader)
        if self.enable_config_from_cmdline:
            argv = self.process_cmdline_config(argv)
        return argv

    def get_cls_by_name(self, name):
        from celery.utils import get_cls_by_name
        return get_cls_by_name(name)

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
        preload_options = dict((opt._long_opts[0], opt.dest)
                                for opt in self.preload_options)
        for arg in args:
            if arg.startswith('--') and '=' in arg:
                key, value = arg.split('=', 1)
                dest = preload_options.get(key)
                if dest:
                    acc[dest] = value
        return acc

    def _get_default_app(self, *args, **kwargs):
        return celery.Celery(*args, **kwargs)
