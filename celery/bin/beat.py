# -*- coding: utf-8 -*-
"""

The :program:`celery beat` command.

.. program:: celery beat

.. seealso::

    See :ref:`preload-options` and :ref:`daemon-options`.

.. cmdoption:: --detach

    Detach and run in the background as a daemon.

.. cmdoption:: -s, --schedule

    Path to the schedule database. Defaults to `celerybeat-schedule`.
    The extension '.db' may be appended to the filename.
    Default is {default}.

.. cmdoption:: -S, --scheduler

    Scheduler class to use.
    Default is :class:`celery.beat.PersistentScheduler`.

.. cmdoption:: --max-interval

    Max seconds to sleep between schedule iterations.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, `stderr` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.

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

from functools import partial

from celery.platforms import detached, maybe_drop_privileges

from celery.bin.base import Command, daemon_options

__all__ = ['beat']


class beat(Command):
    """Start the beat periodic task scheduler.

    Examples::

        celery beat -l info
        celery beat -s /var/run/celery/beat-schedule --detach
        celery beat -S djcelery.schedulers.DatabaseScheduler

    """
    doc = __doc__
    enable_config_from_cmdline = True
    supports_args = False

    def run(self, detach=False, logfile=None, pidfile=None, uid=None,
            gid=None, umask=None, working_directory=None, **kwargs):
        if not detach:
            maybe_drop_privileges(uid=uid, gid=gid)
        workdir = working_directory
        kwargs.pop('app', None)
        beat = partial(self.app.Beat,
                       logfile=logfile, pidfile=pidfile, **kwargs)

        if detach:
            with detached(logfile, pidfile, uid, gid, umask, workdir):
                return beat().run()
        else:
            return beat().run()

    def prepare_arguments(self, parser):
        c = self.app.conf
        parser.add_option('--detach', action='store_true')
        parser.add_option('-s', '--schedule', default=c.beat_schedule_filename)
        parser.add_option('--max-interval', type='float')
        parser.add_option('-S', '--scheduler', dest='scheduler_cls')
        parser.add_option('-l', '--loglevel', default='WARN')
        daemon_options(parser, default_pidfile='celerybeat.pid')
        parser.add_options(self.app.user_options['beat'])


def main(app=None):
    beat(app=app).execute_from_commandline()

if __name__ == '__main__':      # pragma: no cover
    main()
