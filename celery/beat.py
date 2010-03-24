"""

Periodic Task Scheduler

"""
import time
import shelve
import threading
import multiprocessing
from datetime import datetime
from UserDict import UserDict

from celery import log
from celery import conf
from celery import registry as _registry
from celery import platform
from celery.utils.info import humanize_seconds


class SchedulingError(Exception):
    """An error occured while scheduling a task."""


class ScheduleEntry(object):
    """An entry in the scheduler.

    :param task: see :attr:`task`.
    :keyword last_run_at: see :attr:`last_run_at`.
    :keyword total_run_count: see :attr:`total_run_count`.

    .. attribute:: task

        The task class.

    .. attribute:: last_run_at

        The time and date of when this task was last run.

    .. attribute:: total_run_count

        Total number of times this periodic task has been executed.

    """

    def __init__(self, name, last_run_at=None, total_run_count=None):
        self.name = name
        self.last_run_at = last_run_at or datetime.now()
        self.total_run_count = total_run_count or 0

    def next(self):
        """Returns a new instance of the same class, but with
        its date and count fields updated."""
        return self.__class__(self.name,
                              datetime.now(),
                              self.total_run_count + 1)

    def is_due(self, task):
        """See :meth:`celery.task.base.PeriodicTask.is_due`."""
        return task.is_due(self.last_run_at)


class Scheduler(UserDict):
    """Scheduler for periodic tasks.

    :keyword registry: see :attr:`registry`.
    :keyword schedule: see :attr:`schedule`.
    :keyword logger:  see :attr:`logger`.
    :keyword max_interval: see :attr:`max_interval`.

    .. attribute:: registry

        The task registry to use.

    .. attribute:: schedule

        The schedule dict/shelve.

    .. attribute:: logger

        The logger to use.

    .. attribute:: max_interval

        Maximum time to sleep between re-checking the schedule.

    """

    def __init__(self, registry=None, schedule=None, logger=None,
            max_interval=None):
        self.registry = registry or _registry.TaskRegistry()
        self.data = schedule
        if self.data is None:
            self.data = {}
        self.logger = logger or log.get_default_logger()
        self.max_interval = max_interval or conf.CELERYBEAT_MAX_LOOP_INTERVAL

        self.cleanup()
        self.schedule_registry()

    def tick(self):
        """Run a tick, that is one iteration of the scheduler.
        Executes all due tasks."""
        debug = self.logger.debug
        error = self.logger.error

        remaining_times = []
        for entry in self.schedule.values():
            is_due, next_time_to_run = self.is_due(entry)
            if is_due:
                debug("Scheduler: Sending due task %s" % entry.name)
                try:
                    result = self.apply_async(entry)
                except SchedulingError, exc:
                    error("Scheduler: %s" % exc)
                else:
                    debug("%s sent. id->%s" % (entry.name, result.task_id))
            if next_time_to_run:
                remaining_times.append(next_time_to_run)

        return min(remaining_times + [self.max_interval])

    def get_task(self, name):
        return self.registry[name]

    def is_due(self, entry):
        return entry.is_due(self.get_task(entry.name))

    def apply_async(self, entry):

        # Update timestamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.schedule[entry.name] = entry.next()
        task = self.get_task(entry.name)

        try:
            result = task.apply_async()
        except Exception, exc:
            raise SchedulingError("Couldn't apply scheduled task %s: %s" % (
                    task.name, exc))
        return result

    def schedule_registry(self):
        """Add the current contents of the registry to the schedule."""
        for name, task in self.registry.periodic().items():
            if name not in self.schedule:
                self.logger.debug("Scheduler: "
                    "Added periodic task %s to schedule" % name)
            self.schedule.setdefault(name, ScheduleEntry(task.name))

    def cleanup(self):
        for task_name, entry in self.schedule.items():
            if task_name not in self.registry:
                self.schedule.pop(task_name, None)

    @property
    def schedule(self):
        return self.data


class ClockService(object):
    scheduler_cls = Scheduler
    registry = _registry.tasks
    open_schedule = lambda self, filename: shelve.open(filename)

    def __init__(self, logger=None,
            max_interval=conf.CELERYBEAT_MAX_LOOP_INTERVAL,
            schedule_filename=conf.CELERYBEAT_SCHEDULE_FILENAME):
        self.logger = logger or log.get_default_logger()
        self.max_interval = max_interval
        self.schedule_filename = schedule_filename
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self._schedule = None
        self._scheduler = None
        self._in_sync = False
        silence = self.max_interval < 60 and 10 or 1
        self.debug = log.SilenceRepeated(self.logger.debug,
                                         max_iterations=silence)

    def start(self, embedded_process=False):
        self.logger.info("ClockService: Starting...")
        self.logger.debug("ClockService: "
            "Ticking with max interval->%s, schedule->%s" % (
                    humanize_seconds(self.max_interval),
                    self.schedule_filename))

        if embedded_process:
            platform.set_process_title("celerybeat")

        try:
            try:
                while True:
                    if self._shutdown.isSet():
                        break
                    interval = self.scheduler.tick()
                    self.debug("ClockService: Waking up %s." % (
                            humanize_seconds(interval, prefix="in ")))
                    time.sleep(interval)
            except (KeyboardInterrupt, SystemExit):
                self.sync()
        finally:
            self.sync()

    def sync(self):
        if self._schedule is not None and not self._in_sync:
            self.logger.debug("ClockService: Syncing schedule to disk...")
            self._schedule.sync()
            self._schedule.close()
            self._in_sync = True
            self._stopped.set()

    def stop(self, wait=False):
        self.logger.info("ClockService: Shutting down...")
        self._shutdown.set()
        wait and self._stopped.wait() # block until shutdown done.

    @property
    def schedule(self):
        if self._schedule is None:
            filename = self.schedule_filename
            self._schedule = self.open_schedule(filename=filename)
        return self._schedule

    @property
    def scheduler(self):
        if self._scheduler is None:
            self._scheduler = self.scheduler_cls(schedule=self.schedule,
                                            registry=self.registry,
                                            logger=self.logger,
                                            max_interval=self.max_interval)
        return self._scheduler


class _Threaded(threading.Thread):
    """Embedded clock service using threading."""

    def __init__(self, *args, **kwargs):
        super(_Threaded, self).__init__()
        self.clockservice = ClockService(*args, **kwargs)
        self.setDaemon(True)

    def run(self):
        self.clockservice.start()

    def stop(self):
        self.clockservice.stop(wait=True)


class _Process(multiprocessing.Process):
    """Embedded clock service using multiprocessing."""

    def __init__(self, *args, **kwargs):
        super(_Process, self).__init__()
        self.clockservice = ClockService(*args, **kwargs)

    def run(self):
        platform.reset_signal("SIGTERM")
        self.clockservice.start(embedded_process=True)

    def stop(self):
        self.clockservice.stop()
        self.terminate()


def EmbeddedClockService(*args, **kwargs):
    """Return embedded clock service.

    :keyword thread: Run threaded instead of as a separate process.
        Default is ``False``.

    """
    if kwargs.pop("thread", False):
        # Need short max interval to be able to stop thread
        # in reasonable time.
        kwargs.setdefault("max_interval", 1)
        return _Threaded(*args, **kwargs)

    return _Process(*args, **kwargs)
