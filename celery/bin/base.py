import os
import sys

from optparse import OptionParser, make_option as Option

from celery import __version__
from celery.defaults import app_or_default


class Command(object):
    args = ''
    version = __version__
    option_list = ()

    Parser = OptionParser

    def __init__(self, app=None):
        self.app = app_or_default(app)

    def parse_options(self, prog_name, arguments):
        """Parse the available options."""
        parser = self.create_parser(prog_name)
        options, args = parser.parse_args(arguments)
        return options, args

    def create_parser(self, prog_name):
        return self.Parser(prog=prog_name,
                           usage=self.usage(),
                           version=self.version,
                           option_list=self.get_options())

    def execute_from_commandline(self, argv=None):
        if argv is None:
            argv = list(sys.argv)
        prog_name = os.path.basename(argv[0])
        options, args = self.parse_options(prog_name, argv[1:])
        return self.run(*args, **vars(options))

    def usage(self):
        return "%%prog [options] %s" % (self.args, )

    def get_options(self):
        return self.option_list

    def run(self, *args, **options):
        raise NotImplementedError("subclass responsibility")
