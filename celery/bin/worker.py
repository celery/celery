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

    prefork (default), eventlet, gevent, solo or threads.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, `stderr` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.

.. cmdoption:: -n, --hostname

    Set custom hostname, e.g. 'w1.%h'. Expands: %h (hostname),
    %n (name) and %d, (domain).

.. cmdoption:: -B, --beat

    Also run the `celery beat` periodic task scheduler. Please note that
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

.. cmdoption:: -O

    Apply optimization profile.  Supported: default, fair

.. cmdoption:: --scheduler

    Scheduler class to use. Default is celery.beat.PersistentScheduler

.. cmdoption:: -S, --statedb

    Path to the state database. The extension '.db' may
    be appended to the filename. Default: {default}

.. cmdoption:: -E, --events

    Send task-related events that can be captured by monitors like
    :program:`celery events`, `celerymon`, and others.

.. cmdoption:: --without-gossip

    Do not subscribe to other workers events.

.. cmdoption:: --without-mingle

    Do not synchronize with other workers at startup.

.. cmdoption:: --without-heartbeat

    Do not send event heartbeats.

.. cmdoption:: --heartbeat-interval

    Interval in seconds at which to send worker heartbeat

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

.. cmdoption:: --maxmemperchild

    Maximum amount of resident memory, in KiB, that may be consumed by a
    child process before it will be replaced by a new one. If a single
    task causes a child process to exceed this limit, the task will be
    completed and the child process will be replaced afterwards.
    Default: no limit.

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
from __future__ import absolute_import, unicode_literals

import sys

from celery import concurrency
from celery.bin.base import Command, Option, daemon_options
from celery.bin.celeryd_detach import detached_celeryd
from celery.five import string_t
from celery.platforms import maybe_drop_privileges
from celery.utils import default_nodename
from celery.utils.log import LOG_LEVELS, mlevel

__all__ = ['worker', 'main']

__MODULE_DOC__ = __doc__


class worker(Command):
    """Start worker instance.

    Examples::

        celery worker --app=proj -l info
        celery worker -A proj -l info -Q hipri,lopri

        celery worker -A proj --concurrency=4
        celery worker -A proj --concurrency=1000 -P eventlet

        celery worker --autoscale=10,0
    """
    doc = __MODULE_DOC__  # parse help from this too
    namespace = 'worker'
    enable_config_from_cmdline = True
    supports_args = False

    def run_from_argv(self, prog_name, argv=None, command=None):
        command = sys.argv[0] if command is None else command
        argv = sys.argv[1:] if argv is None else argv
        # parse options before detaching so errors can be handled.
        options, args = self.prepare_args(
            *self.parse_options(prog_name, argv, command))
        self.maybe_detach([command] + argv)
        return self(*args, **options)

    def maybe_detach(self, argv, dopts=['-D', '--detach']):
        if any(arg in argv for arg in dopts):
            argv = [v for v in argv if v not in dopts]
            # will never return
            detached_celeryd(self.app).execute_from_commandline(argv)
            raise SystemExit(0)

    def run(self, hostname=None, pool_cls=None, app=None, uid=None, gid=None,
            loglevel=None, logfile=None, pidfile=None, state_db=None,
            **kwargs):
        maybe_drop_privileges(uid=uid, gid=gid)
        # Pools like eventlet/gevent needs to patch libs as early
        # as possible.
        pool_cls = (concurrency.get_implementation(pool_cls) or
                    self.app.conf.worker_pool)
        if self.app.IS_WINDOWS and kwargs.get('beat'):
            self.die('-B option does not work on Windows.  '
                     'Please run celery beat as a separate service.')
        hostname = self.host_format(default_nodename(hostname))
        if loglevel:
            try:
                loglevel = mlevel(loglevel)
            except KeyError:  # pragma: no cover
                self.die('Unknown level {0!r}. Please use one of {1}.'.format(
                    loglevel, '|'.join(
                        l for l in LOG_LEVELS if isinstance(l, string_t))))

        worker = self.app.Worker(
            hostname=hostname, pool_cls=pool_cls, loglevel=loglevel,
            logfile=logfile,  # node format handled by celery.app.log.setup
            pidfile=self.node_format(pidfile, hostname),
            state_db=self.node_format(state_db, hostname), **kwargs
        )
        worker.start()
        return worker.exitcode

    def with_pool_option(self, argv):
        # this command support custom pools
        # that may have to be loaded as early as possible.
        return (['-P'], ['--pool'])

    def get_options(self):
        conf = self.app.conf
        return (
            Option('-c', '--concurrency',
                   default=conf.worker_concurrency, type='int'),
            Option('-P', '--pool', default=conf.worker_pool, dest='pool_cls'),
            Option('--purge', '--discard', default=False, action='store_true'),
            Option('-l', '--loglevel', default='WARN'),
            Option('-n', '--hostname'),
            Option('-B', '--beat', action='store_true'),
            Option('-s', '--schedule', dest='schedule_filename',
                   default=conf.beat_schedule_filename),
            Option('--scheduler', dest='scheduler_cls'),
            Option('-S', '--statedb',
                   default=conf.worker_state_db, dest='state_db'),
            Option('-E', '--events', default=conf.worker_send_task_events,
                   action='store_true', dest='send_events'),
            Option('--time-limit', type='float', dest='task_time_limit',
                   default=conf.task_time_limit),
            Option('--soft-time-limit', dest='task_soft_time_limit',
                   default=conf.task_soft_time_limit, type='float'),
            Option('--maxtasksperchild', dest='max_tasks_per_child',
                   default=conf.worker_max_tasks_per_child, type='int'),
            Option('--prefetch-multiplier', dest='prefetch_multiplier',
                   default=conf.worker_prefetch_multiplier, type='int'),
            Option('--maxmemperchild', dest='max_memory_per_child',
                   default=conf.worker_max_memory_per_child, type='int'),
            Option('--queues', '-Q', default=[]),
            Option('--exclude-queues', '-X', default=[]),
            Option('--include', '-I', default=[]),
            Option('--autoscale'),
            Option('--autoreload', action='store_true'),
            Option('--no-execv', action='store_true', default=False),
            Option('--without-gossip', action='store_true', default=False),
            Option('--without-mingle', action='store_true', default=False),
            Option('--without-heartbeat', action='store_true', default=False),
            Option('--heartbeat-interval', type='int'),
            Option('-O', dest='optimization'),
            Option('-D', '--detach', action='store_true'),
        ) + daemon_options() + tuple(self.app.user_options['worker'])


def main(app=None):
    # Fix for setuptools generated scripts, so that it will
    # work with multiprocessing fork emulation.
    # (see multiprocessing.forking.get_preparation_data())
    if __name__ != '__main__':  # pragma: no cover
        sys.modules['__main__'] = sys.modules[__name__]
    from billiard import freeze_support
    freeze_support()
    worker(app=app).execute_from_commandline()


if __name__ == '__main__':          # pragma: no cover
    main()
