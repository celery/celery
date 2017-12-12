# -*- coding: utf-8 -*-
"""Program used to start a Celery worker instance.

The :program:`celery worker` command (previously known as ``celeryd``)

.. program:: celery worker

.. seealso::

    See :ref:`preload-options`.

.. cmdoption:: -c, --concurrency

    Number of child processes processing the queue.  The default
    is the number of CPUs available on your system.

.. cmdoption:: -P, --pool

    Pool implementation:

    prefork (default), eventlet, gevent or solo.

.. cmdoption:: -n, --hostname

    Set custom hostname (e.g., 'w1@%%h').  Expands: %%h (hostname),
    %%n (name) and %%d, (domain).

.. cmdoption:: -B, --beat

    Also run the `celery beat` periodic task scheduler.  Please note that
    there must only be one instance of this service.

    .. note::

        ``-B`` is meant to be used for development purposes. For production
        environment, you need to start :program:`celery beat` separately.

.. cmdoption:: -Q, --queues

    List of queues to enable for this worker, separated by comma.
    By default all configured queues are enabled.
    Example: `-Q video,image`

.. cmdoption:: -X, --exclude-queues

    List of queues to disable for this worker, separated by comma.
    By default all configured queues are enabled.
    Example: `-X video,image`.

.. cmdoption:: -I, --include

    Comma separated list of additional modules to import.
    Example: -I foo.tasks,bar.tasks

.. cmdoption:: -s, --schedule

    Path to the schedule database if running with the `-B` option.
    Defaults to `celerybeat-schedule`.  The extension ".db" may be
    appended to the filename.

.. cmdoption:: -O

    Apply optimization profile.  Supported: default, fair

.. cmdoption:: --prefetch-multiplier

    Set custom prefetch multiplier value for this worker instance.

.. cmdoption:: --scheduler

    Scheduler class to use.  Default is
    :class:`celery.beat.PersistentScheduler`

.. cmdoption:: -S, --statedb

    Path to the state database.  The extension '.db' may
    be appended to the filename.  Default: {default}

.. cmdoption:: -E, --task-events

    Send task-related events that can be captured by monitors like
    :program:`celery events`, `celerymon`, and others.

.. cmdoption:: --without-gossip

    Don't subscribe to other workers events.

.. cmdoption:: --without-mingle

    Don't synchronize with other workers at start-up.

.. cmdoption:: --without-heartbeat

    Don't send event heartbeats.

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

.. cmdoption:: --max-tasks-per-child

    Maximum number of tasks a pool worker can execute before it's
    terminated and replaced by a new worker.

.. cmdoption:: --max-memory-per-child

    Maximum amount of resident memory, in KiB, that may be consumed by a
    child process before it will be replaced by a new one.  If a single
    task causes a child process to exceed this limit, the task will be
    completed and the child process will be replaced afterwards.
    Default: no limit.

.. cmdoption:: --autoscale

    Enable autoscaling by providing
    max_concurrency, min_concurrency. Example::

        --autoscale=10,3

    (always keep 3 processes, but grow to 10 if necessary)

.. cmdoption:: --detach

    Start worker as a background process.

.. cmdoption:: -f, --logfile

    Path to log file.  If no logfile is specified, `stderr` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.

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

    Effective :manpage:`umask(1)` (in octal) of the process after detaching.
    Inherits the :manpage:`umask(1)` of the parent process by default.

.. cmdoption:: --workdir

    Optional directory to change to after detaching.

.. cmdoption:: --executable

    Executable to use for the detached process.
"""
from __future__ import absolute_import, unicode_literals

import sys

from celery import concurrency
from celery.bin.base import Command, daemon_options
from celery.bin.celeryd_detach import detached_celeryd
from celery.five import string_t
from celery.platforms import maybe_drop_privileges
from celery.utils.log import LOG_LEVELS, mlevel
from celery.utils.nodenames import default_nodename

__all__ = ('worker', 'main')

HELP = __doc__


class worker(Command):
    """Start worker instance.

    Examples:
        .. code-block:: console

            $ celery worker --app=proj -l info
            $ celery worker -A proj -l info -Q hipri,lopri

            $ celery worker -A proj --concurrency=4
            $ celery worker -A proj --concurrency=1000 -P eventlet
            $ celery worker --autoscale=10,0
    """

    doc = HELP  # parse help from this too
    namespace = 'worker'
    enable_config_from_cmdline = True
    supports_args = False
    removed_flags = {'--no-execv', '--force-execv'}

    def run_from_argv(self, prog_name, argv=None, command=None):
        argv = [x for x in argv if x not in self.removed_flags]
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
            loglevel=None, logfile=None, pidfile=None, statedb=None,
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
                self.die('Unknown level {0!r}.  Please use one of {1}.'.format(
                    loglevel, '|'.join(
                        l for l in LOG_LEVELS if isinstance(l, string_t))))

        worker = self.app.Worker(
            hostname=hostname, pool_cls=pool_cls, loglevel=loglevel,
            logfile=logfile,  # node format handled by celery.app.log.setup
            pidfile=self.node_format(pidfile, hostname),
            statedb=self.node_format(statedb, hostname),
            **kwargs)
        worker.start()
        return worker.exitcode

    def with_pool_option(self, argv):
        # this command support custom pools
        # that may have to be loaded as early as possible.
        return (['-P'], ['--pool'])

    def add_arguments(self, parser):
        conf = self.app.conf

        wopts = parser.add_argument_group('Worker Options')
        wopts.add_argument('-n', '--hostname')
        wopts.add_argument(
            '-D', '--detach',
            action='store_true', default=False,
        )
        wopts.add_argument(
            '-S', '--statedb',
            default=conf.worker_state_db,
        )
        wopts.add_argument('-l', '--loglevel', default='WARN')
        wopts.add_argument('-O', dest='optimization')
        wopts.add_argument(
            '--prefetch-multiplier',
            type=int, default=conf.worker_prefetch_multiplier,
        )

        topts = parser.add_argument_group('Pool Options')
        topts.add_argument(
            '-c', '--concurrency',
            default=conf.worker_concurrency, type=int,
        )
        topts.add_argument(
            '-P', '--pool',
            default=conf.worker_pool,
        )
        topts.add_argument(
            '-E', '--task-events', '--events',
            action='store_true', default=conf.worker_send_task_events,
        )
        topts.add_argument(
            '--time-limit',
            type=float, default=conf.task_time_limit,
        )
        topts.add_argument(
            '--soft-time-limit',
            type=float, default=conf.task_soft_time_limit,
        )
        topts.add_argument(
            '--max-tasks-per-child', '--maxtasksperchild',
            type=int, default=conf.worker_max_tasks_per_child,
        )
        topts.add_argument(
            '--max-memory-per-child', '--maxmemperchild',
            type=int, default=conf.worker_max_memory_per_child,
        )

        qopts = parser.add_argument_group('Queue Options')
        qopts.add_argument(
            '--purge', '--discard',
            action='store_true', default=False,
        )
        qopts.add_argument('--queues', '-Q', default=[])
        qopts.add_argument('--exclude-queues', '-X', default=[])
        qopts.add_argument('--include', '-I', default=[])

        fopts = parser.add_argument_group('Features')
        fopts.add_argument(
            '--without-gossip', action='store_true', default=False,
        )
        fopts.add_argument(
            '--without-mingle', action='store_true', default=False,
        )
        fopts.add_argument(
            '--without-heartbeat', action='store_true', default=False,
        )
        fopts.add_argument('--heartbeat-interval', type=int)
        fopts.add_argument('--autoscale')

        daemon_options(parser)

        bopts = parser.add_argument_group('Embedded Beat Options')
        bopts.add_argument('-B', '--beat', action='store_true', default=False)
        bopts.add_argument(
            '-s', '--schedule-filename', '--schedule',
            default=conf.beat_schedule_filename,
        )
        bopts.add_argument('--scheduler')

        user_options = self.app.user_options['worker']
        if user_options:
            uopts = parser.add_argument_group('User Options')
            self.add_compat_options(uopts, user_options)


def main(app=None):
    """Start worker."""
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
