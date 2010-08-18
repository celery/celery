"""

Periodic Task Scheduler

"""
import time
import shelve
import threading
import multiprocessing
from datetime import datetime, timedelta
from UserDict import UserDict

from celery import log
from celery import conf
from celery import platform
from celery.execute import send_task
from celery.schedules import schedule
from celery.messaging import establish_connection
from celery.utils import instantiate
from celery.utils.info import humanize_seconds


class SchedulingError(Exception):
    """An error occured while scheduling a task."""


def maybe_schedule(s, relative=False):
    if isinstance(s, int):
        s = timedelta(seconds=s)
    if isinstance(s, timedelta):
        return schedule(s, relative)
    return s


class ScheduleEntry(object):
    """An entry in the scheduler.

    :param name: see :attr:`name`.
    :param schedule: see :attr:`schedule`.
    :param args: see :attr:`args`.
    :param kwargs: see :attr:`kwargs`.
    :keyword last_run_at: see :attr:`last_run_at`.
    :keyword total_run_count: see :attr:`total_run_count`.

    .. attribute:: name

        The task name.

    .. attribute:: schedule

        The schedule (run_every/crontab)

    .. attribute:: args

        Args to apply.

    .. attribute:: kwargs

        Keyword arguments to apply.

    .. attribute:: last_run_at

        The time and date of when this task was last run.

    .. attribute:: total_run_count

        Total number of times this periodic task has been executed.

    """

    def __init__(self, name, last_run_at=None, total_run_count=None,
            schedule=None, args=(), kwargs={}, options={}, relative=False):
        self.name = name
        self.schedule = maybe_schedule(schedule, relative)
        self.args = args
        self.kwargs = kwargs
        self.options = options
        self.last_run_at = last_run_at or datetime.now()
        self.total_run_count = total_run_count or 0

    def next(self):
        """Returns a new instance of the same class, but with
        its date and count fields updated."""
        return self.__class__(name=self.name,
                              schedule=self.schedule,
                              args=self.args,
                              kwargs=self.kwargs,
                              options=self.options,
                              last_run_at=datetime.now(),
                              total_run_count=self.total_run_count + 1)

    def update(self, other):
        """Update values from another entry.

        Does only update "editable" fields (schedule, args,
        kwargs, options).

        """
        self.schedule = other.schedule
        self.args = other.args
        self.kwargs = other.kwargs
        self.options = other.options

    def is_due(self):
        """See :meth:`celery.task.base.PeriodicTask.is_due`."""
        return self.schedule.is_due(self.last_run_at)

    def __repr__(self):
        return "<Entry: %s(*%s, **%s) {%s}>" % (self.name,
                                                self.args,
                                                self.kwargs,
                                                self.schedule)


class Scheduler(UserDict):
    """Scheduler for periodic tasks.

    :keyword schedule: see :attr:`schedule`.
    :keyword logger:  see :attr:`logger`.
    :keyword max_interval: see :attr:`max_interval`.

    .. attribute:: schedule

        The schedule dict/shelve.

    .. attribute:: logger

        The logger to use.

    .. attribute:: max_interval

        Maximum time to sleep between re-checking the schedule.

    """
    Entry = ScheduleEntry

    def __init__(self, schedule=None, logger=None, max_interval=None,
            **kwargs):
        UserDict.__init__(self)
        if schedule is None:
            schedule = self.dict_to_entries(conf.CELERYBEAT_SCHEDULE)
        self.data = schedule
        self.logger = logger or log.get_default_logger("celery.beat")
        self.max_interval = max_interval or conf.CELERYBEAT_MAX_LOOP_INTERVAL
        self.setup_schedule()

    def maybe_due(self, entry, connection=None):
        is_due, next_time_to_run = entry.is_due()

        if is_due:
            self.logger.debug("Scheduler: Sending due task %s" % entry.name)
            try:
                result = self.apply_async(entry, connection=connection)
            except SchedulingError, exc:
                self.logger.error("Scheduler: %s" % exc)
            else:
                self.logger.debug("%s sent. id->%s" % (entry.name,
                                                       result.task_id))
        return next_time_to_run

    def tick(self):
        """Run a tick, that is one iteration of the scheduler.

        Executes all due tasks.

        """
        remaining_times = []
        connection = establish_connection()
        try:
            try:
                for entry in self.schedule.itervalues():
                    next_time_to_run = self.maybe_due(entry, connection)
                    if next_time_to_run:
                        remaining_times.append(next_time_to_run)
            except RuntimeError:
                pass
        finally:
            connection.close()

        return min(remaining_times + [self.max_interval])

    def reserve(self, entry):
        new_entry = self[entry.name] = entry.next()
        return new_entry

    def apply_async(self, entry, connection=None, **kwargs):
        # Update timestamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.reserve(entry)

        try:
            result = send_task(entry.name, entry.args, entry.kwargs,
                               connection=connection, **entry.options)
        except Exception, exc:
            raise SchedulingError("Couldn't apply scheduled task %s: %s" % (
                    entry.name, exc))
        return result

    def setup_schedule(self):
        pass

    def sync(self):
        pass

    def close(self):
        self.sync()

    def dict_to_entries(self, dict_):
        return dict((name, self.Entry(**entry))
                        for name, entry in dict_.items())

    def get_schedule(self):
        return self.data

    def _set_schedule(self, schedule):
        self.data = schedule

    def _get_schedule(self):
        return self.get_schedule()

    schedule = property(_get_schedule, _set_schedule)


class PersistentScheduler(Scheduler):
    persistence = shelve

    _store = None

    def __init__(self, *args, **kwargs):
        self.schedule_filename = kwargs.get("schedule_filename")
        Scheduler.__init__(self, *args, **kwargs)

    def setup_schedule(self):
        self._store = self.persistence.open(self.schedule_filename)
        self._diskmerge(self._store, conf.CELERYBEAT_SCHEDULE)
        self.sync()
        self.schedule = self._store

    def _diskmerge(self, a, b):
        A, B = set(a), set(b)

        # Remove items from disk not in the schedule anymore.
        for key in A ^ B:
            a.pop(key, None)

        # Update and add new items in the schedule
        for key in B:
            entry = self.Entry(**b[key])
            if a.get(key):
                a[key].update(entry)
            else:
                a[key] = entry

    def sync(self):
        if self._store is not None:
            self.logger.debug("CeleryBeat: Syncing schedule to disk...")
            self._store.sync()

    def close(self):
        self.sync()
        self._store.close()


class Service(object):
    scheduler_cls = PersistentScheduler

    def __init__(self, logger=None,
            max_interval=conf.CELERYBEAT_MAX_LOOP_INTERVAL,
            schedule=conf.CELERYBEAT_SCHEDULE,
            schedule_filename=conf.CELERYBEAT_SCHEDULE_FILENAME,
            scheduler_cls=None):
        self.max_interval = max_interval
        self.scheduler_cls = scheduler_cls or self.scheduler_cls
        self.logger = logger or log.get_default_logger(name="celery.beat")
        self.schedule = schedule
        self.schedule_filename = schedule_filename

        self._scheduler = None
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        silence = self.max_interval < 60 and 10 or 1
        self.debug = log.SilenceRepeated(self.logger.debug,
                                         max_iterations=silence)

    def start(self, embedded_process=False):
        self.logger.info("Celerybeat: Starting...")
        self.logger.debug("Celerybeat: "
            "Ticking with max interval->%s" % (
                    humanize_seconds(self.scheduler.max_interval)))

        if embedded_process:
            platform.set_process_title("celerybeat")

        try:
            try:
                while not self._shutdown.isSet():
                    interval = self.scheduler.tick()
                    self.debug("Celerybeat: Waking up %s." % (
                            humanize_seconds(interval, prefix="in ")))
                    time.sleep(interval)
            except (KeyboardInterrupt, SystemExit):
                self._shutdown.set()
        finally:
            self.sync()

    def sync(self):
        self.scheduler.close()
        self._stopped.set()

    def stop(self, wait=False):
        self.logger.info("Celerybeat: Shutting down...")
        self._shutdown.set()
        wait and self._stopped.wait() # block until shutdown done.

    @property
    def scheduler(self):
        if self._scheduler is None:
            filename = self.schedule_filename
            self._scheduler = instantiate(self.scheduler_cls,
                                          schedule=self.schedule,
                                          schedule_filename=filename,
                                          logger=self.logger,
                                          max_interval=self.max_interval)
        return self._scheduler


class _Threaded(threading.Thread):
    """Embedded task scheduler using threading."""

    def __init__(self, *args, **kwargs):
        super(_Threaded, self).__init__()
        self.service = Service(*args, **kwargs)
        self.setDaemon(True)

    def run(self):
        self.service.start()

    def stop(self):
        self.service.stop(wait=True)


class _Process(multiprocessing.Process):
    """Embedded task scheduler using multiprocessing."""

    def __init__(self, *args, **kwargs):
        super(_Process, self).__init__()
        self.service = Service(*args, **kwargs)

    def run(self):
        platform.reset_signal("SIGTERM")
        self.service.start(embedded_process=True)

    def stop(self):
        self.service.stop()
        self.terminate()


def EmbeddedService(*args, **kwargs):
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
