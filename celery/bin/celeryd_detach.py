import os
import sys

from optparse import OptionParser, BadOptionError

from celery import __version__
from celery.bin.base import daemon_options
from celery.platforms import create_daemon_context

OPTION_LIST = daemon_options(default_pidfile="celeryd.pid")


class detached(object):

    def __init__(self, path, argv, logfile=None, pidfile=None, uid=None,
            gid=None, umask=0, working_directory=None):
        self.path = path
        self.argv = argv
        self.logfile = logfile
        self.pidfile = pidfile
        self.uid = uid
        self.gid = gid
        self.umask = umask
        self.working_directory = working_directory

    def start(self):
        context, on_stop = create_daemon_context(
                                logfile=self.logfile,
                                pidfile=self.pidfile,
                                uid=self.uid,
                                gid=self.gid,
                                umask=self.umask,
                                working_directory=self.working_directory)
        context.open()
        try:
            try:
                os.execv(self.path, [self.path] + self.argv)
            except Exception:
                import logging
                from celery.log import setup_logger
                logger = setup_logger(logfile=self.logfile,
                                      loglevel=logging.ERROR)
                logger.critical("Can't exec %r" % (
                    " ".join([self.path] + self.argv), ),
                    exc_info=sys.exc_info())
        finally:
            on_stop()


class PartialOptionParser(OptionParser):

    def __init__(self, *args, **kwargs):
        self.leftovers = []
        OptionParser.__init__(self, *args, **kwargs)

    def _process_long_opt(self, rargs, values):
        arg = rargs.pop(0)

        if "=" in arg:
            opt, next_arg = arg.split("=", 1)
            rargs.insert(0, next_arg)
            had_explicit_value = True
        else:
            opt = arg
            had_explicit_value = False

        try:
            opt = self._match_long_opt(opt)
            option = self._long_opt.get(opt)
        except BadOptionError:
            option = None

        if option:
            if option.takes_value():
                nargs = option.nargs
                if len(rargs) < nargs:
                    if nargs == 1:
                        self.error("%s option requires an argument" % opt)
                    else:
                        self.error("%s option requires %d arguments" % (
                                    opt, nargs))
                elif nargs == 1:
                    value = rargs.pop(0)
                else:
                    value = tuple(rargs[0:nargs])
                    del rargs[0:nargs]

            elif had_explicit_value:
                self.error("%s option does not take a value" % opt)
            else:
                value = None
            option.process(opt, value, values, self)
        else:
            self.leftovers.append(arg)

    def _process_short_opts(self, rargs, values):
        arg = rargs[0]
        try:
            OptionParser._process_short_opts(self, rargs, values)
        except BadOptionError:
            self.leftovers.append(arg)
            if rargs and not rargs[0][0] == "-":
                self.leftovers.append(rargs.pop(0))


class detached_celeryd(object):
    option_list = OPTION_LIST
    usage = "%prog [options] [celeryd options]"
    version = __version__
    description = ("Detaches Celery worker nodes.  See `celeryd --help` "
                   "for the list of supported worker arguments.")
    command = sys.executable
    execv_path = sys.executable
    execv_argv = ["-m", "celery.bin.celeryd"]

    def Parser(self, prog_name):
        return PartialOptionParser(prog=prog_name,
                                   option_list=self.option_list,
                                   usage=self.usage,
                                   description=self.description,
                                   version=self.version)

    def parse_options(self, prog_name, argv):
        parser = self.Parser(prog_name)
        options, values = parser.parse_args(argv)
        if options.logfile:
            parser.leftovers.append("--logfile=%s" % (options.logfile, ))
        if options.pidfile:
            parser.leftovers.append("--pidfile=%s" % (options.pidfile, ))
        return options, values, parser.leftovers

    def execute_from_commandline(self, argv=None):
        if argv is None:
            argv = sys.argv
        prog_name = os.path.basename(argv[0])
        options, values, leftovers = self.parse_options(prog_name, argv[1:])
        detached(path=self.execv_path,
                 argv=self.execv_argv + leftovers,
                 **vars(options)).start()


def main():
    detached_celeryd().execute_from_commandline()

if __name__ == "__main__":
    main()
