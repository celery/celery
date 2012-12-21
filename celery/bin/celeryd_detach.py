# -*- coding: utf-8 -*-
"""
    celery.bin.celeryd_detach
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Program used to daemonize celeryd.

    Using :func:`os.execv` because forking and multiprocessing
    leads to weird issues (it was a long time ago now, but it
    could have something to do with the threading mutex bug)

"""
from __future__ import absolute_import
from __future__ import with_statement

import celery
import os
import sys

from optparse import OptionParser, BadOptionError

from celery.platforms import EX_FAILURE, detached
from celery.utils.log import get_logger

from celery.bin.base import daemon_options, Option

logger = get_logger(__name__)

OPTION_LIST = daemon_options(default_pidfile='celeryd.pid') + (
    Option('--fake',
           default=False, action='store_true', dest='fake',
           help="Don't fork (for debugging purposes)"),
)


def detach(path, argv, logfile=None, pidfile=None, uid=None,
           gid=None, umask=0, working_directory=None, fake=False, ):
    with detached(logfile, pidfile, uid, gid, umask, working_directory, fake):
        try:
            os.execv(path, [path] + argv)
        except Exception:
            from celery import current_app
            current_app.log.setup_logging_subsystem('ERROR', logfile)
            logger.critical("Can't exec %r", ' '.join([path] + argv),
                            exc_info=True)
        return EX_FAILURE


class PartialOptionParser(OptionParser):

    def __init__(self, *args, **kwargs):
        self.leftovers = []
        OptionParser.__init__(self, *args, **kwargs)

    def _process_long_opt(self, rargs, values):
        arg = rargs.pop(0)

        if '=' in arg:
            opt, next_arg = arg.split('=', 1)
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
                        self.error('%s option requires an argument' % opt)
                    else:
                        self.error('%s option requires %d arguments' % (
                            opt, nargs))
                elif nargs == 1:
                    value = rargs.pop(0)
                else:
                    value = tuple(rargs[0:nargs])
                    del rargs[0:nargs]

            elif had_explicit_value:
                self.error('%s option does not take a value' % opt)
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
            if rargs and not rargs[0][0] == '-':
                self.leftovers.append(rargs.pop(0))


class detached_celeryd(object):
    option_list = OPTION_LIST
    usage = '%prog [options] [celeryd options]'
    version = celery.VERSION_BANNER
    description = ('Detaches Celery worker nodes.  See `celeryd --help` '
                   'for the list of supported worker arguments.')
    command = sys.executable
    execv_path = sys.executable
    execv_argv = ['-m', 'celery.bin.celeryd']

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
            parser.leftovers.append('--logfile=%s' % (options.logfile, ))
        if options.pidfile:
            parser.leftovers.append('--pidfile=%s' % (options.pidfile, ))
        return options, values, parser.leftovers

    def execute_from_commandline(self, argv=None):
        if argv is None:
            argv = sys.argv
        config = []
        seen_cargs = 0
        for arg in argv:
            if seen_cargs:
                config.append(arg)
            else:
                if arg == '--':
                    seen_cargs = 1
                    config.append(arg)
        prog_name = os.path.basename(argv[0])
        options, values, leftovers = self.parse_options(prog_name, argv[1:])
        sys.exit(detach(path=self.execv_path,
                 argv=self.execv_argv + leftovers + config,
                 **vars(options)))


def main():
    detached_celeryd().execute_from_commandline()

if __name__ == '__main__':  # pragma: no cover
    main()
