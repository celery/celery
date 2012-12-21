# -*- coding: utf-8 -*-
"""

The :program:`celery worker` command (previously known as ``celeryd``)

.. program:: celery worker

.. seealso::

    See :ref:`preload-options`.

.. cmdoption:: -c, --concurrency

    Number of child processes processing the queue. The default
    is the number of CPUs available on your system.

.. cmdoption:: -P, --pool

    Pool implementation:

    processes (default), eventlet, gevent, solo or threads.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, `stderr` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.

.. cmdoption:: -n, --hostname

    Set custom hostname, e.g. 'foo.example.com'.

.. cmdoption:: -B, --beat

    Also run the `celerybeat` periodic task scheduler. Please note that
    there must only be one instance of this service.

.. cmdoption:: -Q, --queues

    List of queues to enable for this worker, separated by comma.
    By default all configured queues are enabled.
    Example: `-Q video,image`

.. cmdoption:: -I, --include

    Comma separated list of additional modules to import.
    Example: -I foo.tasks,bar.tasks

.. cmdoption:: -s, --schedule

    Path to the schedule database if running with the `-B` option.
    Defaults to `celerybeat-schedule`. The extension ".db" may be
    appended to the filename.

.. cmdoption:: --scheduler

    Scheduler class to use. Default is celery.beat.PersistentScheduler

.. cmdoption:: -S, --statedb

    Path to the state database. The extension '.db' may
    be appended to the filename. Default: %(default)s

.. cmdoption:: -E, --events

    Send events that can be captured by monitors like :program:`celeryev`,
    `celerymon`, and others.

.. cmdoption:: --purge

    Purges all waiting tasks before the daemon is started.
    **WARNING**: This is unrecoverable, and the tasks will be
    deleted from the messaging server.

.. cmdoption:: --time-limit

    Enables a hard time limit (in seconds int/float) for tasks.

.. cmdoption:: --soft-time-limit

    Enables a soft time limit (in seconds int/float) for tasks.

.. cmdoption:: --maxtasksperchild

    Maximum number of tasks a pool worker can execute before it's
    terminated and replaced by a new worker.

.. cmdoption:: --pidfile

    Optional file used to store the workers pid.

    The worker will not start if this file already exists
    and the pid is still alive.

.. cmdoption:: --autoscale

    Enable autoscaling by providing
    max_concurrency, min_concurrency. Example::

        --autoscale=10,3

    (always keep 3 processes, but grow to 10 if necessary)

.. cmdoption:: --autoreload

    Enable autoreloading.

.. cmdoption:: --no-execv

    Don't do execv after multiprocessing child fork.

"""
from __future__ import absolute_import

import sys

from celery import concurrency
from celery.bin.base import Command, Option
from celery.utils.log import LOG_LEVELS, mlevel


class WorkerCommand(Command):
    doc = __doc__  # parse help from this.
    namespace = 'celeryd'
    enable_config_from_cmdline = True
    supports_args = False

    def execute_from_commandline(self, argv=None):
        if argv is None:
            argv = list(sys.argv)
        return super(WorkerCommand, self).execute_from_commandline(argv)

    def run(self, *args, **kwargs):
        kwargs.pop('app', None)
        # Pools like eventlet/gevent needs to patch libs as early
        # as possible.
        kwargs['pool_cls'] = concurrency.get_implementation(
            kwargs.get('pool_cls') or self.app.conf.CELERYD_POOL)
        if self.app.IS_WINDOWS and kwargs.get('beat'):
            self.die('-B option does not work on Windows.  '
                     'Please run celerybeat as a separate service.')
        loglevel = kwargs.get('loglevel')
        if loglevel:
            try:
                kwargs['loglevel'] = mlevel(loglevel)
            except KeyError:  # pragma: no cover
                self.die('Unknown level %r. Please use one of %s.' % (
                    loglevel, '|'.join(l for l in LOG_LEVELS
                                       if isinstance(l, basestring))))
        return self.app.Worker(**kwargs).run()

    def with_pool_option(self, argv):
        # this command support custom pools
        # that may have to be loaded as early as possible.
        return (['-P'], ['--pool'])

    def get_options(self):
        conf = self.app.conf
        return (
            Option('-c', '--concurrency',
                   default=conf.CELERYD_CONCURRENCY, type='int'),
            Option('-P', '--pool', default=conf.CELERYD_POOL, dest='pool_cls'),
            Option('--purge', '--discard', default=False, action='store_true'),
            Option('-f', '--logfile', default=conf.CELERYD_LOG_FILE),
            Option('-l', '--loglevel', default=conf.CELERYD_LOG_LEVEL),
            Option('-n', '--hostname'),
            Option('-B', '--beat', action='store_true'),
            Option('-s', '--schedule', dest='schedule_filename',
                   default=conf.CELERYBEAT_SCHEDULE_FILENAME),
            Option('--scheduler', dest='scheduler_cls'),
            Option('-S', '--statedb',
                   default=conf.CELERYD_STATE_DB, dest='state_db'),
            Option('-E', '--events', default=conf.CELERY_SEND_EVENTS,
                   action='store_true', dest='send_events'),
            Option('--time-limit', type='float', dest='task_time_limit',
                   default=conf.CELERYD_TASK_TIME_LIMIT),
            Option('--soft-time-limit', dest='task_soft_time_limit',
                   default=conf.CELERYD_TASK_SOFT_TIME_LIMIT, type='float'),
            Option('--maxtasksperchild', dest='max_tasks_per_child',
                   default=conf.CELERYD_MAX_TASKS_PER_CHILD, type='int'),
            Option('--queues', '-Q', default=[]),
            Option('--include', '-I', default=[]),
            Option('--pidfile'),
            Option('--autoscale'),
            Option('--autoreload', action='store_true'),
            Option('--no-execv', action='store_true', default=False),
        )


def main():
    # Fix for setuptools generated scripts, so that it will
    # work with multiprocessing fork emulation.
    # (see multiprocessing.forking.get_preparation_data())
    if __name__ != '__main__':  # pragma: no cover
        sys.modules['__main__'] = sys.modules[__name__]
    from billiard import freeze_support
    freeze_support()
    worker = WorkerCommand()
    worker.execute_from_commandline()


if __name__ == '__main__':          # pragma: no cover
    main()
