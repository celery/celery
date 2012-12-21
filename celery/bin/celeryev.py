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

"""
from __future__ import absolute_import
from __future__ import with_statement

import sys

from functools import partial

from celery.platforms import detached, set_process_title, strargv
from celery.bin.base import Command, Option, daemon_options


class EvCommand(Command):
    doc = __doc__
    supports_args = False

    def run(self, dump=False, camera=None, frequency=1.0, maxrate=None,
            loglevel='INFO', logfile=None, prog_name='celeryev',
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
        prog = '%s:%s' % (self.prog_name, prog)
        info = '%s %s' % (info, strargv(sys.argv))
        return set_process_title(prog, info=info)

    def get_options(self):
        return (
            Option('-d', '--dump', action='store_true'),
            Option('-c', '--camera'),
            Option('--detach', action='store_true'),
            Option('-F', '--frequency', '--freq', type='float', default=1.0),
            Option('-r', '--maxrate'),
            Option('-l', '--loglevel', default='INFO'),
        ) + daemon_options(default_pidfile='celeryev.pid')


def main():
    ev = EvCommand()
    ev.execute_from_commandline()

if __name__ == '__main__':              # pragma: no cover
    main()
