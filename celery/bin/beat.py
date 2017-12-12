# -*- coding: utf-8 -*-
"""The :program:`celery beat` command.

.. program:: celery beat

.. seealso::

    See :ref:`preload-options` and :ref:`daemon-options`.

.. cmdoption:: --detach

    Detach and run in the background as a daemon.

.. cmdoption:: -s, --schedule

    Path to the schedule database.  Defaults to `celerybeat-schedule`.
    The extension '.db' may be appended to the filename.
    Default is {default}.

.. cmdoption:: -S, --scheduler

    Scheduler class to use.
    Default is :class:`{default}`.

.. cmdoption:: --max-interval

    Max seconds to sleep between schedule iterations.

.. cmdoption:: -f, --logfile

    Path to log file.  If no logfile is specified, `stderr` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.

.. cmdoption:: --pidfile

    File used to store the process pid. Defaults to `celerybeat.pid`.

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

from functools import partial

from celery.bin.base import Command, daemon_options
from celery.platforms import detached, maybe_drop_privileges

__all__ = ('beat',)

HELP = __doc__


class beat(Command):
    """Start the beat periodic task scheduler.

    Examples:
        .. code-block:: console

            $ celery beat -l info
            $ celery beat -s /var/run/celery/beat-schedule --detach
            $ celery beat -S django

    The last example requires the :pypi:`django-celery-beat` extension
    package found on PyPI.
    """

    doc = HELP
    enable_config_from_cmdline = True
    supports_args = False

    def run(self, detach=False, logfile=None, pidfile=None, uid=None,
            gid=None, umask=None, workdir=None, **kwargs):
        if not detach:
            maybe_drop_privileges(uid=uid, gid=gid)
        kwargs.pop('app', None)
        beat = partial(self.app.Beat,
                       logfile=logfile, pidfile=pidfile, **kwargs)

        if detach:
            with detached(logfile, pidfile, uid, gid, umask, workdir):
                return beat().run()
        else:
            return beat().run()

    def add_arguments(self, parser):
        c = self.app.conf
        bopts = parser.add_argument_group('Beat Options')
        bopts.add_argument('--detach', action='store_true', default=False)
        bopts.add_argument(
            '-s', '--schedule', default=c.beat_schedule_filename)
        bopts.add_argument('--max-interval', type=float)
        bopts.add_argument('-S', '--scheduler', default=c.beat_scheduler)
        bopts.add_argument('-l', '--loglevel', default='WARN')

        daemon_options(parser, default_pidfile='celerybeat.pid')

        user_options = self.app.user_options['beat']
        if user_options:
            uopts = parser.add_argument_group('User Options')
            self.add_compat_options(uopts, user_options)


def main(app=None):
    beat(app=app).execute_from_commandline()


if __name__ == '__main__':      # pragma: no cover
    main()
