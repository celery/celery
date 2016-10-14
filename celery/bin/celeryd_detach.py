# -*- coding: utf-8 -*-
"""Program used to daemonize the worker.

Using :func:`os.execv` as forking and multiprocessing
leads to weird issues (it was a long time ago now, but it
could have something to do with the threading mutex bug)
"""
from __future__ import absolute_import, unicode_literals

import argparse
import celery
import os
import sys

from celery.platforms import EX_FAILURE, detached
from celery.utils.log import get_logger
from celery.utils.nodenames import default_nodename, node_format

from celery.bin.base import daemon_options

__all__ = ['detached_celeryd', 'detach']

logger = get_logger(__name__)

C_FAKEFORK = os.environ.get('C_FAKEFORK')


def detach(path, argv, logfile=None, pidfile=None, uid=None,
           gid=None, umask=None, workdir=None, fake=False, app=None,
           executable=None, hostname=None):
    """Detach program by argv'."""
    hostname = default_nodename(hostname)
    logfile = node_format(logfile, hostname)
    pidfile = node_format(pidfile, hostname)
    fake = 1 if C_FAKEFORK else fake
    with detached(logfile, pidfile, uid, gid, umask, workdir, fake,
                  after_forkers=False):
        try:
            if executable is not None:
                path = executable
            os.execv(path, [path] + argv)
        except Exception:  # pylint: disable=broad-except
            if app is None:
                from celery import current_app
                app = current_app
            app.log.setup_logging_subsystem(
                'ERROR', logfile, hostname=hostname)
            logger.critical("Can't exec %r", ' '.join([path] + argv),
                            exc_info=True)
        return EX_FAILURE


class detached_celeryd(object):
    """Daemonize the celery worker process."""

    usage = '%(prog)s [options] [celeryd options]'
    version = celery.VERSION_BANNER
    description = ('Detaches Celery worker nodes.  See `celery worker --help` '
                   'for the list of supported worker arguments.')
    command = sys.executable
    execv_path = sys.executable
    execv_argv = ['-m', 'celery', 'worker']

    def __init__(self, app=None):
        self.app = app

    def create_parser(self, prog_name):
        parser = argparse.ArgumentParser(
            prog=prog_name,
            usage=self.usage,
            description=self.description,
        )
        self._add_version_argument(parser)
        self.add_arguments(parser)
        return parser

    def _add_version_argument(self, parser):
        parser.add_argument(
            '--version', action='version', version=self.version,
        )

    def parse_options(self, prog_name, argv):
        parser = self.create_parser(prog_name)
        options, leftovers = parser.parse_known_args(argv)
        if options.logfile:
            leftovers.append('--logfile={0}'.format(options.logfile))
        if options.pidfile:
            leftovers.append('--pidfile={0}'.format(options.pidfile))
        if options.hostname:
            leftovers.append('--hostname={0}'.format(options.hostname))
        return options, leftovers

    def execute_from_commandline(self, argv=None):
        argv = sys.argv if argv is None else argv
        prog_name = os.path.basename(argv[0])
        config, argv = self._split_command_line_config(argv)
        options, leftovers = self.parse_options(prog_name, argv[1:])
        sys.exit(detach(
            app=self.app, path=self.execv_path,
            argv=self.execv_argv + leftovers + config,
            **vars(options)
        ))

    def _split_command_line_config(self, argv):
        config = list(self._extract_command_line_config(argv))
        try:
            argv = argv[:argv.index('--')]
        except ValueError:
            pass
        return config, argv

    def _extract_command_line_config(self, argv):
        # Extracts command-line config appearing after '--':
        #    celery worker -l info -- worker.prefetch_multiplier=10
        # This to make sure argparse doesn't gobble it up.
        seen_cargs = 0
        for arg in argv:
            if seen_cargs:
                yield arg
            else:
                if arg == '--':
                    seen_cargs = 1
                    yield arg

    def add_arguments(self, parser):
        daemon_options(parser, default_pidfile='celeryd.pid')
        parser.add_argument('--workdir', default=None)
        parser.add_argument('-n', '--hostname')
        parser.add_argument(
            '--fake',
            default=False, action='store_true',
            help="Don't fork (for debugging purposes)",
        )


def main(app=None):
    detached_celeryd(app).execute_from_commandline()

if __name__ == '__main__':  # pragma: no cover
    main()
