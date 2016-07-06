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

    prefork (default), eventlet, gevent or solo.

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

.. cmdoption:: -X, --exclude-queues

    List of queues to disable for this worker, separated by comma.
    By default all configured queues are enabled.
    Example: `-X video,image`.

.. cmdoption:: -I, --include

    Comma separated list of additional modules to import.
    Example: -I foo.tasks,bar.tasks

.. cmdoption:: -s, --schedule

    Path to the schedule database if running with the `-B` option.
    Defaults to `celerybeat-schedule`. The extension ".db" may be
    appended to the filename.

.. cmdoption:: -O

    Apply optimization profile.  Supported: default, fair

.. cmdoption:: --prefetch-multiplier

    Set custom prefetch multiplier value for this worker instance.

.. cmdoption:: --scheduler

    Scheduler class to use. Default is
    :class:`celery.beat.PersistentScheduler`

.. cmdoption:: -S, --statedb

    Path to the state database. The extension '.db' may
    be appended to the filename. Default: {default}

.. cmdoption:: -E, --events

    Send task-related events that can be captured by monitors like
    :program:`celery events`, `celerymon`, and others.

.. cmdoption:: --without-gossip

    Do not subscribe to other workers events.

.. cmdoption:: --without-mingle

    Do not synchronize with other workers at start-up.

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

.. cmdoption:: --detach

    Start worker as a background process.

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

    Effective :manpage:`umask(1)` (in octal) of the process after detaching.
    Inherits the :manpage:`umask(1)` of the parent process by default.

.. cmdoption:: --workdir

    Optional directory to change to after detaching.

.. cmdoption:: --executable

    Executable to use for the detached process.

"""
from __future__ import absolute_import, unicode_literals

import sys

from optparse import OptionGroup

from celery import concurrency
from celery.bin.base import Command, daemon_options
from celery.bin.celeryd_detach import detached_celeryd
from celery.five import string_t
from celery.platforms import maybe_drop_privileges
from celery.utils.log import LOG_LEVELS, mlevel
from celery.utils.nodenames import default_nodename

__all__ = ['worker', 'main']

__MODULE_DOC__ = __doc__


class worker(Command):
    """Start worker instance.

    Examples::

        celery worker --app=proj -l info
        celery worker -A proj -l info -Q hipri,lopri

        celery worker -A proj --concurrency=4
        celery worker -A proj --concurrency=1000 -P eventlet

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

    def prepare_arguments(self, parser):
        conf = self.app.conf

        wopts = OptionGroup(parser, 'Worker Options')
        wopts.add_option('-n', '--hostname')
        wopts.add_option('-D', '--detach', action='store_true')
        wopts.add_option(
            '-S', '--statedb',
            default=conf.worker_state_db, dest='state_db',
        )
        wopts.add_option('-l', '--loglevel', default='WARN')
        wopts.add_option('-O', dest='optimization')
        wopts.add_option(
            '--prefetch-multiplier',
            dest='prefetch_multiplier', type='int',
            default=conf.worker_prefetch_multiplier,
        )
        parser.add_option_group(wopts)

        topts = OptionGroup(parser, 'Pool Options')
        topts.add_option(
            '-c', '--concurrency',
            default=conf.worker_concurrency, type='int',
        )
        topts.add_option(
            '-P', '--pool',
            default=conf.worker_pool, dest='pool_cls',
        )
        topts.add_option(
            '-E', '--events',
            default=conf.worker_send_task_events,
            action='store_true', dest='send_events',
        )
        topts.add_option(
            '--time-limit',
            type='float', dest='task_time_limit',
            default=conf.task_time_limit,
        )
        topts.add_option(
            '--soft-time-limit',
            dest='task_soft_time_limit', type='float',
            default=conf.task_soft_time_limit,
        )
        topts.add_option(
            '--maxtasksperchild',
            dest='max_tasks_per_child', type='int',
            default=conf.worker_max_tasks_per_child,
        )
        topts.add_option(
            '--maxmemperchild',
            dest='max_memory_per_child', type='int',
            default=conf.worker_max_memory_per_child,
        )
        parser.add_option_group(topts)

        qopts = OptionGroup(parser, 'Queue Options')
        qopts.add_option(
            '--purge', '--discard',
            default=False, action='store_true',
        )
        qopts.add_option('--queues', '-Q', default=[])
        qopts.add_option('--exclude-queues', '-X', default=[])
        qopts.add_option('--include', '-I', default=[])
        parser.add_option_group(qopts)

        fopts = OptionGroup(parser, 'Features')
        fopts.add_option(
            '--without-gossip', action='store_true', default=False,
        )
        fopts.add_option(
            '--without-mingle', action='store_true', default=False,
        )
        fopts.add_option(
            '--without-heartbeat', action='store_true', default=False,
        )
        fopts.add_option('--heartbeat-interval', type='int')
        parser.add_option_group(fopts)

        daemon_options(parser)

        bopts = OptionGroup(parser, 'Embedded Beat Options')
        bopts.add_option('-B', '--beat', action='store_true')
        bopts.add_option(
            '-s', '--schedule', dest='schedule_filename',
            default=conf.beat_schedule_filename,
        )
        bopts.add_option('--scheduler', dest='scheduler_cls')
        parser.add_option_group(bopts)

        user_options = self.app.user_options['worker']
        if user_options:
            uopts = OptionGroup(parser, 'User Options')
            uopts.option_list.extend(user_options)
            parser.add_option_group(uopts)


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
