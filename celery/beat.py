"""

Periodic Task Scheduler

"""
import time
import shelve
import sys
import threading
import traceback
try:
    import multiprocessing
except ImportError:
    multiprocessing = None

from datetime import datetime

from kombu.utils import cached_property

from celery import __version__
from celery import platforms
from celery import registry
from celery import signals
from celery.app import app_or_default
from celery.log import SilenceRepeated
from celery.schedules import maybe_schedule, crontab
from celery.utils import instantiate, maybe_promise
from celery.utils.timeutils import humanize_seconds


class SchedulingError(Exception):
    """An error occured while scheduling a task."""


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

    def __init__(self, name=None, task=None, last_run_at=None,
            total_run_count=None, schedule=None, args=(), kwargs={},
            options={}, relative=False):
        self.name = name
        self.task = task
        self.schedule = maybe_schedule(schedule, relative)
        self.args = args
        self.kwargs = kwargs
        self.options = options
        self.last_run_at = last_run_at or datetime.now()
        self.total_run_count = total_run_count or 0

    def next(self, last_run_at=None):
        """Returns a new instance of the same class, but with
        its date and count fields updated."""
        last_run_at = last_run_at or datetime.now()
        total_run_count = self.total_run_count + 1
        return self.__class__(**dict(self,
                                     last_run_at=last_run_at,
                                     total_run_count=total_run_count))

    def update(self, other):
        """Update values from another entry.

        Does only update "editable" fields (schedule, args,
        kwargs, options).

        """
        self.task = other.task
        self.schedule = other.schedule
        self.args = other.args
        self.kwargs = other.kwargs
        self.options = other.options

    def is_due(self):
        """See :meth:`celery.task.base.PeriodicTask.is_due`."""
        return self.schedule.is_due(self.last_run_at)

    def __iter__(self):
        return vars(self).iteritems()

    def __repr__(self):
        return "<Entry: %s %s(*%s, **%s) {%s}>" % (self.name,
                                                   self.task,
                                                   self.args,
                                                   self.kwargs,
                                                   self.schedule)


class Scheduler(object):
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
            app=None, Publisher=None, lazy=False, **kwargs):
        if schedule is None:
            schedule = {}
        self.app = app_or_default(app)
        conf = self.app.conf
        self.data = maybe_promise(schedule)
        self.logger = logger or self.app.log.get_default_logger(
                                                name="celery.beat")
        self.max_interval = max_interval or conf.CELERYBEAT_MAX_LOOP_INTERVAL
        self.Publisher = Publisher or self.app.amqp.TaskPublisher
        if not lazy:
            self.setup_schedule()

    def install_default_entries(self, data):
        entries = {}
        if self.app.conf.CELERY_TASK_RESULT_EXPIRES:
            if "celery.backend_cleanup" not in data:
                entries["celery.backend_cleanup"] = {
                        "task": "celery.backend_cleanup",
                        "schedule": crontab("0", "4", "*"),
                        "options": {"expires": 12 * 3600}}
        self.update_from_dict(entries)

    def maybe_due(self, entry, publisher=None):
        is_due, next_time_to_run = entry.is_due()

        if is_due:
            self.logger.debug("Scheduler: Sending due task %s" % entry.task)
            try:
                result = self.apply_async(entry, publisher=publisher)
            except Exception, exc:
                self.logger.error("Message Error: %s\n%s" % (exc,
                    traceback.format_stack()), exc_info=sys.exc_info())
            else:
                self.logger.debug("%s sent. id->%s" % (entry.task,
                                                       result.task_id))
        return next_time_to_run

    def tick(self):
        """Run a tick, that is one iteration of the scheduler.

        Executes all due tasks.

        """
        remaining_times = []
        try:
            for entry in self.schedule.itervalues():
                next_time_to_run = self.maybe_due(entry, self.publisher)
                if next_time_to_run:
                    remaining_times.append(next_time_to_run)
        except RuntimeError:
            pass

        return min(remaining_times + [self.max_interval])

    def reserve(self, entry):
        new_entry = self.schedule[entry.name] = entry.next()
        return new_entry

    def apply_async(self, entry, publisher=None, **kwargs):
        # Update timestamps and run counts before we actually execute,
        # so we have that done if an exception is raised (doesn't schedule
        # forever.)
        entry = self.reserve(entry)

        try:
            task = registry.tasks[entry.task]
        except KeyError:
            task = None

        try:
            if task:
                result = task.apply_async(entry.args, entry.kwargs,
                                          publisher=publisher,
                                          **entry.options)
            else:
                result = self.send_task(entry.task, entry.args, entry.kwargs,
                                        publisher=publisher,
                                        **entry.options)
        except Exception, exc:
            raise SchedulingError("Couldn't apply scheduled task %s: %s" % (
                    entry.name, exc))
        return result

    def send_task(self, *args, **kwargs):               # pragma: no cover
        return self.app.send_task(*args, **kwargs)

    def setup_schedule(self):
        self.install_default_entries(self.data)

    def sync(self):
        pass

    def close(self):
        self.sync()

    def add(self, **kwargs):
        entry = self.Entry(**kwargs)
        self.schedule[entry.name] = entry
        return entry

    def _maybe_entry(self, name, entry):
        if isinstance(entry, self.Entry):
            return entry
        return self.Entry(**dict(entry, name=name))

    def update_from_dict(self, dict_):
        self.schedule.update(dict((name, self._maybe_entry(name, entry))
                                for name, entry in dict_.items()))

    def merge_inplace(self, b):
        schedule = self.schedule
        A, B = set(schedule.keys()), set(b.keys())

        # Remove items from disk not in the schedule anymore.
        for key in A ^ B:
            schedule.pop(key, None)

        # Update and add new items in the schedule
        for key in B:
            entry = self.Entry(**dict(b[key], name=key))
            if schedule.get(key):
                schedule[key].update(entry)
            else:
                schedule[key] = entry

    def get_schedule(self):
        return self.data

    def set_schedule(self, schedule):
        self.data = schedule

    @cached_property
    def connection(self):
        return self.app.broker_connection()

    @cached_property
    def publisher(self):
        return self.Publisher(connection=self.connection)

    @property
    def schedule(self):
        return self.get_schedule()

    @property
    def info(self):
        return ""


class PersistentScheduler(Scheduler):
    persistence = shelve

    _store = None

    def __init__(self, *args, **kwargs):
        self.schedule_filename = kwargs.get("schedule_filename")
        Scheduler.__init__(self, *args, **kwargs)

    def setup_schedule(self):
        self._store = self.persistence.open(self.schedule_filename,
                                            writeback=True)
        if "__version__" not in self._store:
            self._store.clear()   # remove schedule at 2.2.2 upgrade.
        entries = self._store.setdefault("entries", {})
        self.merge_inplace(self.app.conf.CELERYBEAT_SCHEDULE)
        self.install_default_entries(self.schedule)
        self._store["__version__"] = __version__
        self.sync()
        self.logger.debug("Current schedule:\n" +
                          "\n".join(repr(entry)
                                    for entry in entries.itervalues()))

    def get_schedule(self):
        return self._store["entries"]

    def sync(self):
        if self._store is not None:
            self.logger.debug("CeleryBeat: Syncing schedule to disk...")
            self._store.sync()

    def close(self):
        self.sync()
        self._store.close()

    @property
    def info(self):
        return "    . db -> %s" % (self.schedule_filename, )


class Service(object):
    scheduler_cls = PersistentScheduler

    def __init__(self, logger=None,
            max_interval=None, schedule_filename=None,
            scheduler_cls=None, app=None):
        self.app = app_or_default(app)
        self.max_interval = max_interval or \
                            self.app.conf.CELERYBEAT_MAX_LOOP_INTERVAL
        self.scheduler_cls = scheduler_cls or self.scheduler_cls
        self.logger = logger or self.app.log.get_default_logger(
                                                name="celery.beat")
        self.schedule_filename = schedule_filename or \
                                    self.app.conf.CELERYBEAT_SCHEDULE_FILENAME

        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        silence = self.max_interval < 60 and 10 or 1
        self.debug = SilenceRepeated(self.logger.debug,
                                     max_iterations=silence)

    def start(self, embedded_process=False):
        self.logger.info("Celerybeat: Starting...")
        self.logger.debug("Celerybeat: "
            "Ticking with max interval->%s" % (
                    humanize_seconds(self.scheduler.max_interval)))

        signals.beat_init.send(sender=self)
        if embedded_process:
            signals.beat_embedded_init.send(sender=self)
            platforms.set_process_title("celerybeat")

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
        wait and self._stopped.wait()           # block until shutdown done.

    def get_scheduler(self, lazy=False):
        filename = self.schedule_filename
        scheduler = instantiate(self.scheduler_cls,
                                app=self.app,
                                schedule_filename=filename,
                                logger=self.logger,
                                max_interval=self.max_interval,
                                lazy=lazy)
        return scheduler

    @cached_property
    def scheduler(self):
        return self.get_scheduler()


class _Threaded(threading.Thread):
    """Embedded task scheduler using threading."""

    def __init__(self, *args, **kwargs):
        super(_Threaded, self).__init__()
        self.service = Service(*args, **kwargs)
        self.setDaemon(True)
        self.setName("Beat")

    def run(self):
        self.service.start()

    def stop(self):
        self.service.stop(wait=True)


if multiprocessing is not None:
    class _Process(multiprocessing.Process):
        """Embedded task scheduler using multiprocessing."""

        def __init__(self, *args, **kwargs):
            super(_Process, self).__init__()
            self.service = Service(*args, **kwargs)
            self.name = "Beat"

        def run(self):
            platforms.reset_signal("SIGTERM")
            self.service.start(embedded_process=True)

        def stop(self):
            self.service.stop()
            self.terminate()
else:
    _Process = None


def EmbeddedService(*args, **kwargs):
    """Return embedded clock service.

    :keyword thread: Run threaded instead of as a separate process.
        Default is :const:`False`.

    """
    if kwargs.pop("thread", False) or _Process is None:
        # Need short max interval to be able to stop thread
        # in reasonable time.
        kwargs.setdefault("max_interval", 1)
        return _Threaded(*args, **kwargs)

    return _Process(*args, **kwargs)
