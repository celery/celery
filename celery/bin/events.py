# -*- coding: utf-8 -*-
"""The :program:`celery events` command.

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

    Camera: Optional shutter rate limit (e.g., 10/m).

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.  Default is INFO.

.. cmdoption:: -f, --logfile

    Path to log file.  If no logfile is specified, `stderr` is used.

.. cmdoption:: --pidfile

    Optional file used to store the process pid.

    The program won't start if this file already exists
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

HELP = __doc__


class events(Command):
    """Event-stream utilities.

    Notes:
        .. code-block:: console

            # - Start graphical monitor (requires curses)
            $ celery events --app=proj
            $ celery events -d --app=proj
            # - Dump events to screen.
            $ celery events -b amqp://
            # - Run snapshot camera.
            $ celery events -c <camera> [options]

    Examples:
        .. code-block:: console

            $ celery events
            $ celery events -d
            $ celery events -c mod.attr -F 1.0 --detach --maxrate=100/m -l info
    """

    doc = HELP
    supports_args = False

    def run(self, dump=False, camera=None, frequency=1.0, maxrate=None,
            loglevel='INFO', logfile=None, prog_name='celery events',
            pidfile=None, uid=None, gid=None, umask=None,
            workdir=None, detach=False, **kwargs):
        self.prog_name = prog_name

        if dump:
            return self.run_evdump()
        if camera:
            return self.run_evcam(camera, freq=frequency, maxrate=maxrate,
                                  loglevel=loglevel, logfile=logfile,
                                  pidfile=pidfile, uid=uid, gid=gid,
                                  umask=umask,
                                  workdir=workdir,
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
                  gid=None, umask=None, workdir=None,
                  detach=False, **kwargs):
        from celery.events.snapshot import evcam
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

    def add_arguments(self, parser):
        dopts = parser.add_argument_group('Dumper')
        dopts.add_argument('-d', '--dump', action='store_true')

        copts = parser.add_argument_group('Snapshot')
        copts.add_argument('-c', '--camera')
        copts.add_argument('--detach', action='store_true')
        copts.add_argument('-F', '--frequency', '--freq',
                           type=float, default=1.0)
        copts.add_argument('-r', '--maxrate')
        copts.add_argument('-l', '--loglevel', default='INFO')

        daemon_options(parser, default_pidfile='celeryev.pid')

        user_options = self.app.user_options['events']
        if user_options:
            self.add_compat_options(
                parser.add_argument_group('User Options'),
                user_options)


def main():
    ev = events()
    ev.execute_from_commandline()

if __name__ == '__main__':              # pragma: no cover
    main()
