# -*- coding: utf-8 -*-
"""

The :program:`celery events` command.

.. program:: celery events

.. seealso::

    See :ref:`preload-options` and :ref:`daemon-options`.

.. cmdoption:: -d, --dump

    Dump events to stdout.

.. cmdoption:: -c, --camera

    Take snapshots of events using this camera.

.. cmdoption:: --detach

    Camera: Detach and run in the background as a daemon.

.. cmdoption:: -F, --freq, --frequency

    Camera: Shutter frequency.  Default is every 1.0 seconds.

.. cmdoption:: -r, --maxrate

    Camera: Optional shutter rate limit (e.g. 10/m).

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.  Default is INFO.

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

    Effective umask (in octal) of the process after detaching.  Inherits
    the umask of the parent process by default.

.. cmdoption:: --workdir

    Optional directory to change to after detaching.

.. cmdoption:: --executable

    Executable to use for the detached process.

"""
from __future__ import absolute_import, unicode_literals

import sys

from functools import partial

from celery.platforms import detached, set_process_title, strargv
from celery.bin.base import Command, daemon_options

__all__ = ['events']


class events(Command):
    """Event-stream utilities.

    Commands::

        celery events --app=proj
            start graphical monitor (requires curses)
        celery events -d --app=proj
            dump events to screen.
        celery events -b amqp://
        celery events -c <camera> [options]
            run snapshot camera.

    Examples::

        celery events
        celery events -d
        celery events -c mod.attr -F 1.0 --detach --maxrate=100/m -l info
    """
    doc = __doc__
    supports_args = False

    def run(self, dump=False, camera=None, frequency=1.0, maxrate=None,
            loglevel='INFO', logfile=None, prog_name='celery events',
            pidfile=None, uid=None, gid=None, umask=None,
            working_directory=None, detach=False, **kwargs):
        self.prog_name = prog_name

        if dump:
            return self.run_evdump()
        if camera:
            return self.run_evcam(camera, freq=frequency, maxrate=maxrate,
                                  loglevel=loglevel, logfile=logfile,
                                  pidfile=pidfile, uid=uid, gid=gid,
                                  umask=umask,
                                  working_directory=working_directory,
                                  detach=detach)
        return self.run_evtop()

    def run_evdump(self):
        from celery.events.dumper import evdump
        self.set_process_status('dump')
        return evdump(app=self.app)

    def run_evtop(self):
        from celery.events.cursesmon import evtop
        self.set_process_status('top')
        return evtop(app=self.app)

    def run_evcam(self, camera, logfile=None, pidfile=None, uid=None,
                  gid=None, umask=None, working_directory=None,
                  detach=False, **kwargs):
        from celery.events.snapshot import evcam
        workdir = working_directory
        self.set_process_status('cam')
        kwargs['app'] = self.app
        cam = partial(evcam, camera,
                      logfile=logfile, pidfile=pidfile, **kwargs)

        if detach:
            with detached(logfile, pidfile, uid, gid, umask, workdir):
                return cam()
        else:
            return cam()

    def set_process_status(self, prog, info=''):
        prog = '{0}:{1}'.format(self.prog_name, prog)
        info = '{0} {1}'.format(info, strargv(sys.argv))
        return set_process_title(prog, info=info)

    def prepare_arguments(self, parser):
        parser.add_option('-d', '--dump', action='store_true')
        parser.add_option('-c', '--camera')
        parser.add_option('--detach', action='store_true')
        parser.add_option('-F', '--frequency', '--freq',
                          type='float', default=1.0)
        parser.add_option('-r', '--maxrate')
        parser.add_option('-l', '--loglevel', default='INFO')
        daemon_options(parser, default_pidfile='celeryev.pid')
        parser.add_options(self.app.user_options['events'])


def main():
    ev = events()
    ev.execute_from_commandline()

if __name__ == '__main__':              # pragma: no cover
    main()
