# -*- coding: utf-8 -*-
"""celeryd

.. program:: celeryd

.. cmdoption:: -c, --concurrency

    Number of child processes processing the queue. The default
    is the number of CPUs available on your system.

.. cmdoption:: -f, --logfile

    Path to log file. If no logfile is specified, `stderr` is used.

.. cmdoption:: -l, --loglevel

    Logging level, choose between `DEBUG`, `INFO`, `WARNING`,
    `ERROR`, `CRITICAL`, or `FATAL`.

.. cmdoption:: -n, --hostname

    Set custom hostname.

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
    Defaults to `celerybeat-schedule`. The extension ".db" will be
    appended to the filename.

.. cmdoption:: --scheduler

    Scheduler class to use. Default is celery.beat.PersistentScheduler

.. cmdoption:: -E, --events

    Send events that can be captured by monitors like `celerymon`.

.. cmdoption:: --purge, --discard

    Discard all waiting tasks before the daemon is started.
    **WARNING**: This is unrecoverable, and the tasks will be
    deleted from the messaging server.

.. cmdoption:: --time-limit

    Enables a hard time limit (in seconds) for tasks.

.. cmdoption:: --soft-time-limit

    Enables a soft time limit (in seconds) for tasks.

.. cmdoption:: --maxtasksperchild

    Maximum number of tasks a pool worker can execute before it's
    terminated and replaced by a new worker.

"""
from __future__ import absolute_import

import sys

try:
    from multiprocessing import freeze_support
except ImportError:  # pragma: no cover
    freeze_support = lambda: True  # noqa

from celery.bin.base import Command, Option


class WorkerCommand(Command):
    namespace = "celeryd"
    enable_config_from_cmdline = True
    supports_args = False

    def run(self, *args, **kwargs):
        kwargs.pop("app", None)
        # Pools like eventlet/gevent needs to patch libs as early
        # as possible.
        from celery import concurrency
        kwargs["pool"] = concurrency.get_implementation(
                    kwargs.get("pool") or self.app.conf.CELERYD_POOL)
        return self.app.Worker(**kwargs).run()

    def get_options(self):
        conf = self.app.conf
        return (
            Option('-c', '--concurrency',
                default=conf.CELERYD_CONCURRENCY,
                action="store", dest="concurrency", type="int",
                help="Number of worker threads/processes"),
            Option('-P', '--pool',
                default=conf.CELERYD_POOL,
                action="store", dest="pool", type="str",
                help="Pool implementation: "
                     "processes (default), eventlet, gevent, "
                     "solo or threads."),
            Option('--purge', '--discard', default=False,
                action="store_true", dest="discard",
                help="Discard all waiting tasks before the server is"
                     "started. WARNING: There is no undo operation "
                     "and the tasks will be deleted."),
            Option('-f', '--logfile', default=conf.CELERYD_LOG_FILE,
                action="store", dest="logfile",
                help="Path to log file."),
            Option('-l', '--loglevel', default=conf.CELERYD_LOG_LEVEL,
                action="store", dest="loglevel",
                help="Choose between DEBUG/INFO/WARNING/ERROR/CRITICAL"),
            Option('-n', '--hostname', default=None,
                action="store", dest="hostname",
                help="Set custom host name. E.g. 'foo.example.com'."),
            Option('-B', '--beat', default=False,
                action="store_true", dest="run_clockservice",
                help="Also run the celerybeat periodic task scheduler. "
                     "NOTE: Only one instance of celerybeat must be"
                     "running at any one time."),
            Option('-s', '--schedule',
                default=conf.CELERYBEAT_SCHEDULE_FILENAME,
                action="store", dest="schedule",
                help="Path to the schedule database if running with the -B "
                     "option. The extension '.db' will be appended to the "
                    "filename. Default: %s" % (
                        conf.CELERYBEAT_SCHEDULE_FILENAME, )),
            Option('--scheduler',
                default=None,
                action="store", dest="scheduler_cls",
                help="Scheduler class. Default is "
                     "celery.beat.PersistentScheduler"),
            Option('-S', '--statedb', default=conf.CELERYD_STATE_DB,
                action="store", dest="db",
                help="Path to the state database. The extension '.db' will "
                     "be appended to the filename. Default: %s" % (
                        conf.CELERYD_STATE_DB, )),
            Option('-E', '--events', default=conf.CELERY_SEND_EVENTS,
                action="store_true", dest="events",
                help="Send events so the worker can be monitored by "
                     "celeryev, celerymon and other monitors.."),
            Option('--time-limit',
                default=conf.CELERYD_TASK_TIME_LIMIT,
                action="store", type="int", dest="task_time_limit",
                help="Enables a hard time limit (in seconds) for tasks."),
            Option('--soft-time-limit',
                default=conf.CELERYD_TASK_SOFT_TIME_LIMIT,
                action="store", type="int", dest="task_soft_time_limit",
                help="Enables a soft time limit (in seconds) for tasks."),
            Option('--maxtasksperchild',
                default=conf.CELERYD_MAX_TASKS_PER_CHILD,
                action="store", type="int", dest="max_tasks_per_child",
                help="Maximum number of tasks a pool worker can execute"
                     "before it's terminated and replaced by a new worker."),
            Option('--queues', '-Q', default=[],
                action="store", dest="queues",
                help="Comma separated list of queues to consume from. "
                     "By default all configured queues are used. "
                     "Example: -Q video,image"),
            Option('--include', '-I', default=[],
                action="store", dest="include",
                help="Comma separated list of additional modules to import. "
                 "Example: -I foo.tasks,bar.tasks"),
            Option('--pidfile', default=None,
                help="Optional file used to store the workers pid. "
                     "The worker will not start if this file already exists "
                     "and the pid is still alive."),
            Option('--autoscale', default=None,
                help="Enable autoscaling by providing "
                     "max_concurrency,min_concurrency. Example: "
                     "--autoscale=10,3 (always keep 3 processes, "
                     "but grow to 10 if necessary)."),
        )


def main():
    freeze_support()
    worker = WorkerCommand()
    worker.execute_from_commandline()


def windows_main():
    sys.stderr.write("""

The celeryd command does not work on Windows.

Instead, please use:

    ..> python -m celery.bin.celeryd

You can also supply arguments:

    ..> python -m celery.bin.celeryd --concurrency=10 --loglevel=DEBUG


    """.strip())


if __name__ == "__main__":          # pragma: no cover
    main()
